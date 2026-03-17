import { RecordBatch } from "apache-arrow";
import { streamRecords } from "./reader";
import { flattenRecord } from "./schema";
import { buildBatch, writeBatchesToParquet, writeBytes } from "./writer";
import { Compression } from "parquet-wasm/node";
import { listChildFolders } from "./lister_folders";
import { listChildFiles } from "./lister_files";

// Events matching these (eventSource, eventName) pairs are discarded during
// conversion — high-volume automated operations with minimal analytical value.
// For KMS only the automated crypto operations are dropped; management events
// (CreateKey, PutKeyPolicy, ScheduleKeyDeletion, etc.) are kept.
const DISCARDED_EVENTS: Map<string, Set<string>> = new Map([
  [
    "kms.amazonaws.com",
    new Set([
      // Crypto operations called constantly by AWS services (S3, EBS,
      // Secrets Manager, etc.) — not user-triggered.
      "Decrypt",
      "Encrypt",
      "GenerateDataKey",
      "GenerateDataKeyWithoutPlaintext",
      "GenerateDataKeyPair",
      "GenerateDataKeyPairWithoutPlaintext",
      "ReEncryptFrom",
      "ReEncryptTo",
      "Sign",
      "Verify",
      // Read-only describe/list noise.
      "DescribeKey",
      "GetKeyPolicy",
      "GetKeyRotationStatus",
      "GetPublicKey",
      "ListAliases",
      "ListGrants",
      "ListKeyPolicies",
      "ListKeys",
      "ListResourceTags",
      "ListRetirableGrants",
    ]),
  ],
  [
    "rds.amazonaws.com",
    new Set([
      // High-frequency automated describe/monitoring calls.
      "DescribeDBInstances",
      "DescribeDBClusters",
      "DescribeDBLogFiles",
      "DescribeEvents",
      "DescribeDBEngineVersions",
      "DescribeDBParameterGroups",
      "DescribeDBParameters",
      "DescribeDBSubnetGroups",
      "DescribeOptionGroups",
      "DescribePendingMaintenanceActions",
      "DownloadDBLogFilePortion",
    ]),
  ],
]);

/**
 * Enumerates through a set of CloudTrail folders and writes out
 * corresponding Parquet versions.
 *
 * @param baseInputPath the base path to read the CloudTrail files from including trailing slash
 * @param baseOutputPath the base path to write the parquet files to including trailing slash (or null to disable writing)
 * @param organisationId an organisation id to use in finding CloudTrails or null for single account style
 * @param accountId the account id to process
 * @param year
 * @param month
 * @param day
 * @param parquetEntriesPerRowGroup the number of entries for each RowGroup in the parquet file (affects stats etc)
 * @param parquetRowGroupsPerFile the desired size of the individual parquet files in bytes
 */
export async function convertSingleDayCloudTrailToParquets(
  baseInputPath: string,
  baseOutputPath: string | null,
  organisationId: string | null,
  accountId: string,
  year: number,
  month: number,
  day: number,
  parquetEntriesPerRowGroup: number,
  parquetRowGroupsPerFile: number,
): Promise<void> {
  if (!baseInputPath.endsWith("/"))
    throw new Error("baseInputPath must end with a slash");

  if (baseOutputPath && !baseOutputPath.endsWith("/"))
    throw new Error("baseOutputPath must end with a slash if specified");

  const yearString = year.toString().padStart(4, "0");
  const monthString = month.toString().padStart(2, "0");
  const dayString = day.toString().padStart(2, "0");

  const awsLogsBaseInputPathWithOrg = organisationId
    ? `${baseInputPath}AWSLogs/${organisationId}/`
    : `${baseInputPath}AWSLogs/`;

  const awsCloudTrailLogsBasePath = `${awsLogsBaseInputPathWithOrg}${accountId}/CloudTrail/`;

  const regions = await listChildFolders(awsCloudTrailLogsBasePath);

  for (const region of regions) {
    const logsOfInterest = await findAllPossibleDayLogs(
      `${awsCloudTrailLogsBasePath}${region}`,
      yearString,
      monthString,
      dayString,
    );

    if (logsOfInterest.length === 0) {
      continue;
    }
    console.log(
      `Inspecting region ${region} in account ${accountId} gives the following log sample path ${logsOfInterest[0]} and length ${logsOfInterest.length}\n`
    );

    let parquetCounter = 0;

    for await (const pq of convertSingle(
      logsOfInterest,
      `${yearString}-${monthString}-${dayString}T`,
      parquetEntriesPerRowGroup,
      parquetRowGroupsPerFile,
    )) {
      // in the case where we have no output and are just simulating, make a fake path
      const base = baseOutputPath ? baseOutputPath : "<outputPath>/";

      const output = organisationId
        ? `${base}AWSLogsParquet/CloudTrail/${organisationId}/account=${accountId}/region=${region}year=${yearString}/month=${monthString}/day=${dayString}/${parquetCounter.toString().padStart(5, "0")}.parquet`
        : `${base}AWSLogsParquet/CloudTrail/account=${accountId}/region=${region}year=${yearString}/month=${monthString}/day=${dayString}/${parquetCounter.toString().padStart(5, "0")}.parquet`;

      if (baseOutputPath) {
        console.log(
          `Actually writing Parquet of size ${pq.length} to ${output}`,
        );
        await writeBytes(output, pq);
      } else {
        console.log(
          `Proposing writing Parquet of size ${pq.length} to ${output}`,
        );
      }
      parquetCounter++;
    }

    if (parquetCounter === 0)
      console.log(
        `No CloudTrail entries found for ${accountId}${region}${yearString}/${monthString}/${dayString}`,
      );
  }
}

/**
 * For a given folder base, use the known CloudTrail partitioning scheme
 * to find all the logs for the given day (and the next day).
 *
 * @param basePathAtDatePartition
 * @param yearString
 * @param monthString
 * @param dayString
 */
async function findAllPossibleDayLogs(
  basePathAtDatePartition: string,
  yearString: string,
  monthString: string,
  dayString: string,
) {
  const logsOfInterest: string[] = [];

  {
    const dayBasePath = `${basePathAtDatePartition}${yearString}/${monthString}/${dayString}/`;

    const dayFullLogs = (await listChildFiles(dayBasePath)).map(
      (logFile) => `${dayBasePath}${logFile}`,
    );

    // add the logs we suspect from our target day (for this account and region)
    logsOfInterest.push(...dayFullLogs);
  }

  {
    const day = new Date(`${yearString}-${monthString}-${dayString}`);
    const dayPlus = new Date(
      day.getFullYear(),
      day.getMonth(),
      day.getDate() + 1,
    );

    const dayPlusYearString = dayPlus.getFullYear().toString().padStart(4, "0");
    // noting that getMonth is 0 based
    const dayPlusMonthString = (dayPlus.getMonth() + 1)
      .toString()
      .padStart(2, "0");
    const dayPlusDayString = dayPlus.getDate().toString().padStart(2, "0");
    const dayPlusBasePath = `${basePathAtDatePartition}${dayPlusYearString}/${dayPlusMonthString}/${dayPlusDayString}/`;

    const dayPlusFullLogs = (await listChildFiles(dayPlusBasePath)).map(
      (logFile) => `${dayPlusBasePath}${logFile}`,
    );

    // we only want day plus logs around midnight as cloudtrail pretty much guarantees writes within the
    // hour
    // ACTUALLY GIVEN EVENTS CAN BE DELAYED BY HOURS IN VERY EDGE CASES - AND THE PRICE
    // OF SCANNING THE ENTIRE NEXT DAY IS NOT MUCH WE CURRENTLY DO NOT OPTIMISE HERE
    // const pattern = new RegExp(
    //   `^\\d+_CloudTrail_[\\w-]+-${dayPlusYearString}${dayPlusMonthString}${dayPlusDayString}T00\\d\\d\\.json$`,
    // );
    //const filtered = dayPlusFullLogs.filter((logFile) => pattern.test(logFile))

    // add the logs we suspect from our next day (for this account and region)
    logsOfInterest.push(...dayPlusFullLogs);
  }

  return logsOfInterest;
}

/**
 * Convert a set of cloud trail log files (intended to represent all
 * those logs from a single day) to a sequence
 * of parquet files (as byte arrays).
 *
 * Rejects all CloudTrail entries that do not start with the event time given
 * (pass in "" to include *all* entries). This is used to filter entries
 * to a single day (CloudTrail events from the previous day can be saved
 * into the next day).
 *
 * Optionally, choose the compression used just to support
 * our testing parquet reader which only supports snappy.
 *
 * @param logFilePaths
 * @param eventTimePrefix
 * @param parquetEntriesPerRowGroup
 * @param parquetRowGroupsPerFile
 * @param compression
 */
export async function* convertSingle(
  logFilePaths: string[],
  eventTimePrefix: string,
  parquetEntriesPerRowGroup: number,
  parquetRowGroupsPerFile: number,
  compression = Compression.SNAPPY,
): AsyncGenerator<Uint8Array, void, unknown> {
  let batches: RecordBatch[] = [];
  let buffer = [];
  let recordsFound = 0;

  // process all the given log files
  for (const logFilePath of logFilePaths) {
    // stream records continuously from each one
    // and build batches
    for await (const record of streamRecords(logFilePath, eventTimePrefix)) {
      if (DISCARDED_EVENTS.get(record.eventSource)?.has(record.eventName)) continue;

      recordsFound++;

      buffer.push(flattenRecord(record));

      // construct row groups (batches) to help facilitate slicing within
      // a single parquet file
      if (buffer.length >= parquetEntriesPerRowGroup) {
        const batch = buildBatch(buffer);
        batches.push(batch);
        buffer = [];

        // break into separate files to help facilitate not having
        // our parquet be too large
        // (helps also with memory usage of our lambda)
        if (batches.length >= parquetRowGroupsPerFile) {
          console.debug(
            `Yielding parquet file at record count ${recordsFound}, batch count ${batches.length}, buffer count ${buffer.length}`,
          );
          yield await writeBatchesToParquet(batches, compression);
          batches = [];
        }
      }
    }
  }

  // if we have anything left over - it has to go into a final batch
  if (buffer.length > 0) {
    batches.push(buildBatch(buffer));
  }

  if (recordsFound === 0) {
    // if we actually found nothing at all then exit without having
    // yielded
  } else {
    console.debug(
      `Last yielding parquet file at record count ${recordsFound}, batch count ${batches.length}, buffer count ${buffer.length}`,
    );
    yield await writeBatchesToParquet(batches, compression);
  }
}
