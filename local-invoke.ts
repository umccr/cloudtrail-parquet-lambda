#!/usr/bin/env bun
/**
 * local-invoke.ts
 *
 * Runs the Lambda handler locally with a mock EventBridge Scheduled Event.
 * Uses the same env-var contract as the real Lambda so you can test end-to-end
 * against real S3 paths (or local test_data/ with a file:// base path).
 *
 * Usage:
 *   bun run local-invoke.ts
 *   PROCESS_DATE=2024-03-01 bun run local-invoke.ts
 *   CLOUDTRAIL_BASE_PATH=test_data/ OUTPUT_PATH=/tmp/parquet-out/ bun run local-invoke.ts
 */

import { entrypoint_handler } from './src/entrypoint_handler';

// ── Default env for local runs ─────────────────────────────────────────────
// Override any of these via environment variables before running.

process.env.CLOUDTRAIL_BASE_PATH ??= 'test_data/';
process.env.OUTPUT_PATH          ??= '/tmp/cloudtrail-parquet/';
process.env.ORGANISATION_ID      ??= 'o-abcd';
process.env.PROCESS_DATE      = '2020-09-21'; // uncomment to pin a date

// ── Mock EventBridge Scheduled Event ──────────────────────────────────────
// `time` is set to "tomorrow" so that resolveDate() lands on today when
// PROCESS_DATE is not set. For pinned dates, set PROCESS_DATE instead.
const tomorrow = new Date();
tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);

const mockEvent = {
    version: '0',
    id: 'local-test-' + Math.random().toString(36).slice(2),
    'detail-type': 'Scheduled Event' as const,
    source: 'aws.events' as const,
    account: '123456789012',
    time: tomorrow.toISOString(),
    region: 'ap-southeast-2',
    resources: ['arn:aws:events:ap-southeast-2:123456789012:rule/cloudtrail-to-parquet-daily'],
    detail: {} as Record<string, never>,
};

const mockContext = {
    functionName: 'cloudtrail-to-parquet',
    awsRequestId: 'local-' + Date.now(),
    logGroupName: '/aws/lambda/cloudtrail-to-parquet',
};

console.log('─── Local Lambda invoke ───────────────────────────────────');
console.log('Env:');
console.log('  CLOUDTRAIL_BASE_PATH =', process.env.CLOUDTRAIL_BASE_PATH);
console.log('  OUTPUT_PATH          =', process.env.OUTPUT_PATH);
console.log('  ORGANISATION_ID      =', process.env.ORGANISATION_ID);
console.log('  PROCESS_DATE         =', process.env.PROCESS_DATE ?? '(not set — will use yesterday)');
console.log('───────────────────────────────────────────────────────────\n');

try {
    await entrypoint_handler(mockEvent, mockContext);
    console.log('\n✅  Handler completed successfully.');
} catch (err) {
    console.error('\n❌  Handler threw an error:');
    console.error(err);
    process.exit(1);
}
