import { listChildFolders } from "./lister_folders";
import { listChildFiles } from "./lister_files";
import { convertSingle } from "./converter";

export async function cloudtrailToParquet(
  basePath: string,
  organisationId: string | null,
  year: number,
  month: number,
  day: number,
  outputPath: string,
): Promise<void> {
  const startMs = Date.now();

  if (!basePath.endsWith("/"))
    throw new Error("basePath must end with a slash");

  const yearString = year.toString().padStart(4, "0");
  const monthString = month.toString().padStart(2, "0");
  const dayString = day.toString().padStart(2, "0");

  const awsLogsBasePathWithOrg = organisationId
    ? `${basePath}AWSLogs/${organisationId}/`
    : `${basePath}AWSLogs/`;

  const accounts = await listChildFolders(awsLogsBasePathWithOrg);

  for (const account of accounts) {
    const awsCloudTrailLogsBasePath = `${awsLogsBasePathWithOrg}${account}CloudTrail/`;

    const regions = await listChildFolders(awsCloudTrailLogsBasePath);

    for (const region of regions) {
      console.log(
        `Inspecting region ${region} in account ${account} at ${awsCloudTrailLogsBasePath}`,
      );

      const awsActualCloudTrailLogsBasePath = `${awsCloudTrailLogsBasePath}${region}${yearString}/${monthString}/${dayString}/`;

      const logs = await listChildFiles(awsActualCloudTrailLogsBasePath);

      for (const log of logs) {
        for await (const pq of convertSingle(
          `${awsActualCloudTrailLogsBasePath}${log}`,
          `${yearString}${monthString}${dayString}`,
        )) {
          // do something with pq
        }
      }
    }
  }
}

// ── CLI entry point ───────────────────────────────────────────────────────────
if (import.meta.main) {
  const [inputPath, outputPath] = Bun.argv.slice(2);

  if (!inputPath || !outputPath) {
    console.error(
      "Usage: bun index.ts <input.json|input.json.gz> <output.parquet>",
    );
    process.exit(1);
  }

  // await cloudtrailToParquet("s3://aws-cloudtrail-logs-843407916570-dcd05a9d/", null, 2021, 9, 1, outputPath);
  await cloudtrailToParquet(
    "test_data/",
    "o-version1-0",
    2014,
    3,
    6,
    outputPath,
  );
  await cloudtrailToParquet(
    "test_data/",
    "o-version1-05",
    2018,
    10,
    21,
    outputPath,
  );
  await cloudtrailToParquet("test_data/", "o-abcd", 2020, 9, 21, outputPath);
}
