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
  processDate: "today" | "yesterday" | string;
}

interface LambdaContext {
  functionName: string;
  awsRequestId: string;
  logGroupName: string;
}

/**
 * Resolve the date to process, be in a fixed date (mainly for
 * debugging) or a date relative to the event fire time.
 */
function resolveDate(processDate: string): {
  year: number;
  month: number;
  day: number;
} {
  if (processDate === "today") {
    const t = new Date();
    return {
      year: t.getUTCFullYear(),
      month: t.getUTCMonth() + 1,
      day: t.getUTCDate(),
    };
  } else if (processDate === "yesterday") {
    const t = new Date();
    t.setUTCDate(t.getUTCDate() - 1);
    return {
      year: t.getUTCFullYear(),
      month: t.getUTCMonth() + 1,
      day: t.getUTCDate(),
    };
  } else {
    const [y, m, d] = processDate.split("-").map(Number);
    if (!y || !m || !d)
      throw new Error(
        `Invalid processDate of ${processDate} - expected YYYY-MM-DD`,
      );
    return { year: y, month: m, day: d };
  }
}

export async function handler(
  event: ScheduledEvent,
  context: LambdaContext,
): Promise<void> {
  console.log("Event:", JSON.stringify(event));
  console.log("Request ID:", context.awsRequestId);
  console.log("Invoked At:", new Date().toISOString());

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

  if (!event.processDate)
    throw new Error(
      "Missing date to process 'processDate' (e.g. 2020-09-30 or today or yesterday)",
    );

  const { year, month, day } = resolveDate(event.processDate);

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
    5,
  );

  console.log(`Done processing CloudTrail logs for ${event.accountId}`);
}
