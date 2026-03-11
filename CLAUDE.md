# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
bun install

# Type check (no emit)
bun run typecheck

# Run all tests
bun test

# Run a single test file
bun test tests/1_11.test.ts

# Run the handler locally against test_input/ fixtures
bun run test-local-invoke.ts

# Build Docker image for local Lambda emulation
bash docker_local.sh

# Invoke the locally running Lambda container
bash docker_simulate.sh
```

## Architecture

This is a **Bun-based AWS Lambda** (container image) that converts AWS CloudTrail
JSON logs from S3 into Parquet files and writes them back to S3.

The original reference documentation for the CloudTrail JSON fields is
[here](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference.html).


### Data flow

```
S3 CloudTrail JSON (gzipped) → reader.ts → converter.ts → writer.ts → S3 Parquet
```

1. **`src/entrypoint_handler.ts`** — Lambda `handler` export. Receives a `ScheduledEvent` with `baseInputPath`, `baseOutputPath`, `organisationId`, `accountId`. Resolves the date to process (defaults to yesterday UTC; overridable via `PROCESS_DATE` env var `YYYY-MM-DD`).

2. **`src/converter.ts`** — Core logic. `convertSingleDayCloudTrailToParquets` enumerates regions, then calls `convertSingle` which streams records, batches them into Arrow `RecordBatch` objects, and yields Parquet `Uint8Array` chunks. It reads both the target day *and* the next calendar day (to capture delayed CloudTrail events), filtering by `eventTime` prefix.

3. **`src/reader.ts`** — Streaming JSON reader using `stream-json`. Handles gzip decompression, S3 reads, and local file reads. Filters records by `eventTimePrefix`.

4. **`src/schema.ts`** — Defines the `FlatRecord` TypeScript type, the Apache Arrow `SCHEMA`, and `flattenRecord()`. All CloudTrail versions (1.0–1.11) are **upgraded to the latest schema**; missing mandatory fields from older versions are filled with a sentinel string like `Pre1.07SchemaNull`. Nested objects (`requestParameters`, `responseElements`, `userIdentity.sessionContext`, etc.) are serialised as JSON strings. `eventTime` is stored as `TimestampMillisecond` (bigint).

5. **`src/writer.ts`** — Converts Arrow `RecordBatch[]` to Parquet via `parquet-wasm` (IPC bridge). Writes to S3 or local filesystem. Compression defaults to Snappy.

6. **`src/lister_folders.ts` / `src/lister_files.ts`** — S3 list helpers for navigating the CloudTrail prefix structure.

7. **`src/entrypoint_lambda_runtime.ts`** — Custom Lambda Runtime API loop (polls `/runtime/invocation/next`). Used because Bun has no official Lambda runtime layer; bundled separately.

### Output path structure

Parquet files are written using Hive partitioning:
```
{baseOutputPath}AWSLogsParquet/CloudTrail/[{orgId}/]account={accountId}/region={region}year={year}/month={month}/day={day}/{00000}.parquet
```

### Docker / deployment

- **`Dockerfile`** has two targets: `local` (includes Lambda RIE emulator for local testing) and `runtime` (production). Default target is `runtime`.
- The bundle step (`bun build`) produces `dist/handler.js` and `dist/runtime.js`. `parquet-wasm` native module is copied separately (not bundleable).
- Lambda is triggered by EventBridge on a schedule (see `terraform_example/`).

### Testing

Tests live in `tests/` and use `bun:test`. Test fixtures are in `test_input/examples/` (JSON files
per CloudTrail version). `hyparquet` (dev dependency) is used to read back
Parquet output in tests to verify correctness.