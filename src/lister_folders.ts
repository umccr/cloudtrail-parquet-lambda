import { ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import { readdirSync } from "fs";
import { join } from "path";
import { isS3Uri, parseS3Uri } from "./s3_uri";

/**
 * For a given input path (either S3 or local) return a sorted list of subfolders that are
 * its immediate children. For instance if given
 * "s3://amzn-s3-demo-bucket/prefix_name/AWSLogs/"
 * this would return (for example)
 * [
 *     "1234567890/",
 *     "0123456789/"
 * ]
 * as the account folders that CloudTrail has been writing logs to.
 *
 * @param inputPath an S3 or local directory path
 */
export async function listChildFolders(inputPath: string): Promise<string[]> {
  return isS3Uri(inputPath)
    ? listS3ChildFolders(inputPath)
    : listLocalChildFolders(inputPath);
}

async function listS3ChildFolders(uri: string): Promise<string[]> {
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

    console.log(normalizedPrefix);

    for (const cp of response.CommonPrefixes ?? []) {
      if (cp.Prefix) {
        // remove the prefix
        // note the result from AWS will be / terminated
        results.push(cp.Prefix.replace(normalizedPrefix, ""));
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return results.sort();
}

function listLocalChildFolders(dirPath: string): Promise<string[]> {
  const results: string[] = [];

  try {
    for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
      const fullPath = join(dirPath, entry.name);
      if (entry.isDirectory()) {
        // return / terminated to match equivalent from S3 lister
        results.push(`${entry.name}/`);
      }
    }
  } catch (err: any) {
    if (err.code === "ENOENT") {
      console.log("Directory does not exist");
    } else {
      throw err; // re-throw unexpected errors
    }
  }

  return Promise.resolve(results.sort());
}
