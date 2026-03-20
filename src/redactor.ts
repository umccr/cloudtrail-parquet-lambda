import { type FlatRecord } from "./cloudtrail_to_arrow";

const REDACTED_VALUE = "-";

// Keys whose values are replaced with REDACTED_VALUE wherever they appear in the
// JSON-serialised fields of a FlatRecord.  Targets high-cardinality /
// sensitive data that adds no analytical value (e.g. STS session tokens,
// S3 extended request IDs).
const REDACTED_KEYS = new Set(["sessionToken", "x-amz-id-2"]);

function redactObject(v: unknown): unknown {
  if (v == null || typeof v !== "object") return v;
  if (Array.isArray(v)) return v.map(redactObject);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    out[k] = REDACTED_KEYS.has(k) ? REDACTED_VALUE : redactObject(val);
  }
  return out;
}

function redactJsonField(s: string | null): string | null {
  if (s === null) return null;
  return JSON.stringify(redactObject(JSON.parse(s)));
}

// Returns a new FlatRecord with sensitive values replaced in the JSON-string
// fields.  All other fields are passed through unchanged.
export function redact(record: FlatRecord): FlatRecord {
  return {
    ...record,
    responseElements: redactJsonField(record.responseElements),
    additionalEventData: redactJsonField(record.additionalEventData),
  };
}
