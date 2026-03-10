import { chain } from "stream-chain";
import { parser } from "stream-json";
import { pick } from "stream-json/filters/Pick.js";
import { streamArray } from "stream-json/streamers/StreamArray.js";
import { Readable } from "stream";
import { createGunzip } from "zlib";
import {
  S3Client,
  GetObjectCommand
} from "@aws-sdk/client-s3";
import { isS3Uri, parseS3Uri } from "./s3_uri";

/**
 * Async generator for the JSON records in a single CloudTrail JSON. We
 * require the event to have an 'eventTime' field, but otherwise we do
 * not enforce any schema at this point. We filter records
 * to those whose time starts with eventTimePrefix.
 *
 * @param inputPath
 * @param eventTimePrefix
 */
export async function* streamRecords(
  inputPath: string,
  eventTimePrefix: string,
): AsyncGenerator<any> {
  const readable = await openReadable(inputPath);

  // cloudtrail files annoyingly have a single top-level field 'Records'
  // that is the array of entries. This streaming JSON
  // reader is an efficient way to get the entries without
  // loading the entire file into memory.
  const pipeline = chain([
    readable,
    ...(isGzipped(inputPath) ? [createGunzip()] : []),
    parser(),
    pick({ filter: "Records" }),
    streamArray(),
  ]);

  let skipped = 0;
  let yielded = 0;

  for await (const { value } of pipeline) {
    const et = value["eventTime"];

    // eventTime is absolutely mandatory in every cloudtrail entry, so
    // if we don't have those chances are we are not processing
    // CloudTrail entries at all
    if (!et)
      throw new Error(
        "encountered a CloudTrail entry that has no eventTime field",
      );

    if (String(et).startsWith(eventTimePrefix)) {
      yield value;
      yielded++;
    }
    else
      skipped++;
  }

  console.log(`Finished '${inputPath.slice(-45)}' where we yielded ${yielded} and skipped ${skipped} (that did not start with ${eventTimePrefix})`);
}

const isGzipped = (path: string): boolean =>
  path.endsWith(".gz") || path.endsWith(".json.gz");

/**
 * Get a streaming Readable from a single file input path.
 *
 * @param inputPath
 */
async function openReadable(inputPath: string): Promise<Readable> {
  // support S3
  if (isS3Uri(inputPath)) {
    const { bucket, key } = parseS3Uri(inputPath);

    const client = new S3Client({});
    const response = await client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key }),
    );

    if (!response.Body) throw new Error(`S3 object has no body: ${inputPath}`);

    // In the Node.js runtime, Body is already a Node.js Readable
    return response.Body as Readable;
  }

  // otherwise support local file
  // Local file: Bun.file().stream() returns a Web ReadableStream — bridge to Node Readable
  return Readable.fromWeb(
    Bun.file(inputPath).stream() as Parameters<typeof Readable.fromWeb>[0],
  );
}
