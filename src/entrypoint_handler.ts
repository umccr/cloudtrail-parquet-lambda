import { convertSingleDayCloudTrailToParquets } from "./converter";

/**
 * EventBridge Scheduled Event (cron) shape.
 * The Lambda runtime passes this when triggered by an EventBridge rule.
 */
interface ScheduledEvent {
  version: string;
  id: string;
  "detail-type": "Scheduled Event";
  source: "aws.events";
  account: string;
  time: string; // ISO-8601, e.g. "2024-03-05T00:00:00Z"
  region: string;
  resources: string[];
  detail: Record<string, never>;
}

interface LambdaContext {
  functionName: string;
  awsRequestId: string;
  logGroupName: string;
}

/**
 * Resolve the date to process.
 *
 * Priority:
 *   1. PROCESS_DATE env var ("YYYY-MM-DD") - useful for backfills
 *   2. The EventBridge event `time` minus one day - i.e. "yesterday"
 *
 * For a daily cron that fires at 01:00 UTC, this means we always process
 * the fully completed previous day.
 */
function resolveDate(eventTime: string): {
  year: number;
  month: number;
  day: number;
} {
  const override = process.env.PROCESS_DATE;
  if (override) {
    const [y, m, d] = override.split("-").map(Number);
    if (!y || !m || !d)
      throw new Error(
        `Invalid PROCESS_DATE: ${override} — expected YYYY-MM-DD`,
      );
    return { year: y, month: m, day: d };
  }

  // Default: yesterday relative to the event fire time
  const t = new Date(eventTime);
  t.setUTCDate(t.getUTCDate() - 1);
  return {
    year: t.getUTCFullYear(),
    month: t.getUTCMonth() + 1,
    day: t.getUTCDate(),
  };
}

export async function handler(
  event: ScheduledEvent,
  context: LambdaContext,
): Promise<void> {
  console.log("Event:", JSON.stringify(event));
  console.log("Request ID:", context.awsRequestId);

  // ── Required env vars ──────────────────────────────────────────────────────
  const basePath = process.env.CLOUDTRAIL_BASE_PATH;
  const outputPath = process.env.OUTPUT_PATH;

  if (!basePath)
    throw new Error(
      "Missing env var: CLOUDTRAIL_BASE_PATH (e.g. s3://my-bucket/prefix/)",
    );
  if (!outputPath)
    throw new Error(
      "Missing env var: OUTPUT_PATH (e.g. s3://my-output-bucket/parquet/)",
    );

  // ── Optional env vars ──────────────────────────────────────────────────────
  const organisationId = process.env.ORGANISATION_ID ?? null; // null = no org prefix

  const { year, month, day } = resolveDate(event.time);
  console.log(
    `Processing CloudTrail logs for ${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
  );

  await convertSingleDayCloudTrailToParquets(
    basePath,
    outputPath,
    organisationId,
    year,
    month,
    day,
    50000,
    5
  );

  console.log("Done.");
}
