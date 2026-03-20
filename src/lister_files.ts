import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { readdirSync } from "fs";
import { isS3Uri, parseS3Uri } from "./s3_uri";

/**
 * For a given input path (either S3 or local) return a sorted list of files that are
 * its immediate children. For instance if given
 * "s3://amzn-s3-demo-bucket/prefix_name/AWSLogs/CloudTrail/123123123/2020/09/01"
 * this would return (for example)
 * [
 *     "123123123_CloudTrail_ap-southeast-2_20210901T0025Z_2OA5GobnmtdzNsh1.json.gz",
 *     "123123123_CloudTrail_ap-southeast-2_20210901T0040Z_MqmyvtCclncS21os.json.gz"
 * ]
 *
 * This will work even if the paths do not exist (will return an empty array)
 *
 * @param inputPath an S3 or local directory path
 */
export async function listChildFiles(inputPath: string): Promise<string[]> {
  return isS3Uri(inputPath)
    ? listS3ChildFiles(inputPath)
    : listLocalChildFiles(inputPath);
}

async function listS3ChildFiles(uri: string): Promise<string[]> {
  const { bucket, key: prefix } = parseS3Uri(uri);

  // ensure prefix ends with '/' so we list inside the "directory", not the key itself
  const normalizedPrefix =
    prefix && !prefix.endsWith("/") ? `${prefix}/` : prefix;

  const client = new S3Client({});
  const results: string[] = [];
  let continuationToken: string | undefined;

  do {
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: normalizedPrefix,
        Delimiter: "/", // stop at next slash — immediate children only
        ContinuationToken: continuationToken,
      }),
    );

    for (const obj of response.Contents ?? []) {
      if (obj.Key) {
        // remove the prefix
        results.push(obj.Key.replace(normalizedPrefix, ""));
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return results.sort();
}

function listLocalChildFiles(dirPath: string): Promise<string[]> {
  const results: string[] = [];

  try {
    for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
      if (entry.isFile()) {
        results.push(`${entry.name}`);
      }
    }
  } catch (err: any) {
    if (err.code === "ENOENT") {
    } else {
      throw err; // re-throw unexpected errors
    }
  }

  return Promise.resolve(results.sort());
}
