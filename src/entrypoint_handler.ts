import { convertSingleDayCloudTrailToParquets } from "./converter";

/**
 * An AWS Lambda entry point for a lambda that converts
 * CloudTrail logs for a single account into Parquet files.
 */
interface ScheduledEvent {
  baseInputPath: string;
  baseOutputPath: string | null;
  organisationId: string | null;
  accountId: string;
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
 *   2. The current UTC time of invocation minus 1 day (i.e. yesterday)
 *
 * For a daily cron that fires at 01:00 UTC, this means we always process
 * the fully completed previous day.
 */
function resolveDate(): {
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
  const t = new Date();
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

  const invokedAt = new Date().toISOString();
  console.log("Invoked At:", invokedAt);

  if (!event.baseInputPath || !event.baseInputPath.endsWith("/"))
    throw new Error(
      "Missing slash terminated base input path 'baseInputPath' e.g. s3://my-bucket/prefix/cloudtrail-logs/",
    );

  if (event.baseOutputPath)
    if (!event.baseOutputPath.endsWith("/"))
      throw new Error(
        "Missing slash termination on optional base output path 'baseOutputPath' e.g. s3://my-bucket/prefix/cloudtrail-parquet-logs/",
      );

  if (!event.accountId)
    throw new Error(
      "Missing account id to process 'accountId' (e.g. 3453464564)",
    );

  const { year, month, day } = resolveDate();
  console.log(
    `Processing CloudTrail logs for ${event.accountId} for ${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`,
  );

  await convertSingleDayCloudTrailToParquets(
    event.baseInputPath,
    event.baseOutputPath,
    event.organisationId,
    event.accountId,
    year,
    month,
    day,
    50000,
    5
  );

  console.log("Done.");
}
