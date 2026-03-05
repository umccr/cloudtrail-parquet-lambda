import * as arrow from 'apache-arrow';
import { SCHEMA, flattenRecord, type FlatRecord } from './schema.js';

// ── Types ─────────────────────────────────────────────────────────────────────

export interface FieldDiagnosis {
    field:       string;
    arrowType:   string;
    nullable:    boolean;
    issue:       'null_in_required' | 'unexpected_type' | 'invalid_timestamp' | 'ok';
    /** How many records in the sample triggered this issue */
    count:       number;
    /** A representative bad raw value (before flattening) */
    example?:    unknown;
}

export interface DiagnosisReport {
    totalSampled:   number;
    totalFlattened: number;
    fields:         FieldDiagnosis[];
    /** Fields with issues only */
    issues:         FieldDiagnosis[];
    /** True if any required field received a null value */
    hasNullViolations: boolean;
    /** True if any field had a type that couldn't be coerced */
    hasTypeErrors: boolean;
}

// ── Expected JS types per Arrow type ID ──────────────────────────────────────

const EXPECTED_TYPEOF: Partial<Record<arrow.Type, string[]>> = {
    [arrow.Type.Utf8]:               ['string'],
    [arrow.Type.Bool]:               ['boolean'],
    [arrow.Type.TimestampMillisecond]: ['bigint'],
};

// ── Core diagnostic logic ─────────────────────────────────────────────────────

/**
 * Diagnose a sample of raw CloudTrail records against the Arrow schema.
 * Returns a report detailing any type mismatches, unexpected nulls in
 * required fields, or timestamp parse failures.
 *
 * @param records  Raw (pre-flatten) CloudTrail records to sample
 */
export function diagnoseRecords(records: any[]): DiagnosisReport {
    const counts: Record<string, { issue: FieldDiagnosis['issue']; count: number; example?: unknown }> =
        Object.fromEntries(SCHEMA.fields.map(f => [f.name, { issue: 'ok' as const, count: 0 }]));

    let totalFlattened = 0;

    for (const raw of records) {
        let flat: FlatRecord;

        try {
            flat = flattenRecord(raw);
            totalFlattened++;
        } catch (err) {
            // flattenRecord itself threw — almost certainly a bad eventTime
            // Try to identify which field caused it
            const msg = err instanceof Error ? err.message : String(err);
            if (msg.toLowerCase().includes('bigint') || msg.toLowerCase().includes('date')) {
                const entry = counts['eventTime']!;
                if (entry.issue === 'ok' || entry.issue === 'invalid_timestamp') {
                    entry.issue = 'invalid_timestamp';
                    entry.count++;
                    entry.example ??= raw.eventTime;
                }
            }
            continue;
        }

        for (const field of SCHEMA.fields) {
            const name = field.name as keyof FlatRecord;
            const value = flat[name];
            const entry = counts[field.name]!;

            // Check 1: null in a required (non-nullable) field
            if (!field.nullable && value == null) {
                if (entry.issue === 'ok') entry.issue = 'null_in_required';
                entry.count++;
                entry.example ??= (raw as Record<string, unknown>)[field.name];
                continue;
            }

            // Check 2: wrong JS type for non-null values
            if (value != null) {
                const expected = EXPECTED_TYPEOF[field.type.typeId as arrow.Type];
                if (expected && !expected.includes(typeof value)) {
                    if (entry.issue === 'ok') entry.issue = 'unexpected_type';
                    entry.count++;
                    entry.example ??= value;
                    continue;
                }

                // Check 3: timestamp validity — a NaN bigint means the source string
                // parsed to an invalid date
                if (field.type.typeId === arrow.Type.TimestampMillisecond) {
                    const ms = Number(value as bigint);
                    if (isNaN(ms) || !isFinite(ms)) {
                        if (entry.issue === 'ok') entry.issue = 'invalid_timestamp';
                        entry.count++;
                        entry.example ??= (raw as Record<string, unknown>)[field.name];
                    }
                }
            }
        }
    }

    const fields: FieldDiagnosis[] = SCHEMA.fields.map(field => ({
        field:     field.name,
        arrowType: field.type.toString(),
        nullable:  field.nullable,
        issue:     counts[field.name]!.issue,
        count:     counts[field.name]!.count,
        example:   counts[field.name]!.example,
    }));

    const issues = fields.filter(f => f.issue !== 'ok');

    return {
        totalSampled:      records.length,
        totalFlattened,
        fields,
        issues,
        hasNullViolations: issues.some(f => f.issue === 'null_in_required'),
        hasTypeErrors:     issues.some(f => f.issue === 'unexpected_type' || f.issue === 'invalid_timestamp'),
    };
}

/**
 * Print a human-readable diagnosis report to stdout.
 */
export function printReport(report: DiagnosisReport): void {
    const { totalSampled, totalFlattened, issues } = report;

    console.log(`\nDiagnosis: sampled ${totalSampled} records, ${totalFlattened} flattened successfully`);

    if (issues.length === 0) {
        console.log('✓ No issues found — all fields match the schema\n');
        return;
    }

    console.log(`\n⚠ ${issues.length} field(s) with issues:\n`);

    const colW = Math.max(...issues.map(f => f.field.length), 10);

    console.log(
        `  ${'field'.padEnd(colW)}  ${'arrow type'.padEnd(22)}  ${'issue'.padEnd(20)}  count  example`
    );
    console.log(`  ${'-'.repeat(colW)}  ${'─'.repeat(22)}  ${'─'.repeat(20)}  ─────  ───────`);

    for (const f of issues) {
        const example = f.example !== undefined
            ? JSON.stringify(f.example).slice(0, 40)
            : '';
        console.log(
            `  ${f.field.padEnd(colW)}  ${f.arrowType.padEnd(22)}  ${f.issue.padEnd(20)}  ${String(f.count).padStart(5)}  ${example}`
        );
    }

    console.log();

    if (report.hasNullViolations) {
        console.log('  ✗ null_in_required: a field declared non-nullable received a null value.');
        console.log('    Fix: either make the field nullable in the schema, or ensure the source always provides a value.\n');
    }
    if (issues.some(f => f.issue === 'unexpected_type')) {
        console.log('  ✗ unexpected_type: a flattened value has the wrong JS type for its Arrow column.');
        console.log('    Fix: check the coercion logic in flattenRecord() for the affected field.\n');
    }
    if (issues.some(f => f.issue === 'invalid_timestamp')) {
        console.log('  ✗ invalid_timestamp: eventTime could not be parsed as a valid date.');
        console.log('    Fix: check the source eventTime format — expected ISO 8601 (e.g. 2024-01-15T10:30:00Z).\n');
    }
}