import { chain } from 'stream-chain';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick.js';
import { streamArray } from 'stream-json/streamers/StreamArray.js';
import { Readable } from 'stream';
import { createGunzip } from 'zlib';
import {S3Client, GetObjectCommand, ListObjectsV2Command} from '@aws-sdk/client-s3';
import {isS3Uri, parseS3Uri} from "./s3_uri";

// ── Source detection ──────────────────────────────────────────────────────────
const isGzipped = (path: string): boolean =>
    path.endsWith('.gz') || path.endsWith('.json.gz');

// ── Get a Node.js Readable from a local file or S3 object ────────────────────
async function openReadable(inputPath: string): Promise<Readable> {
  if (isS3Uri(inputPath)) {
    const { bucket, key } = parseS3Uri(inputPath);

    // Region can be overridden via AWS_REGION env var (standard AWS SDK behaviour)
    const client = new S3Client({});
    const response = await client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));

    if (!response.Body) throw new Error(`S3 object has no body: ${inputPath}`);

    // In the Node.js runtime, Body is already a Node.js Readable
    return response.Body as Readable;
  }

  // Local file: Bun.file().stream() returns a Web ReadableStream — bridge to Node Readable
  return Readable.fromWeb(
      Bun.file(inputPath).stream() as Parameters<typeof Readable.fromWeb>[0],
  );
}

/**
 * Async generator for the JSON records in the cloudtrail JSON. At this
 * point we are not assuming anything about the object types - we
 * are happy with them as 'any'. We will do enforcement when we
 * put the values into the Parquet.
 *
 * @param inputPath
 */
export async function* streamRecords(inputPath: string): AsyncGenerator<any> {
  const readable = await openReadable(inputPath);

  // cloudtrail files annoyingly have a single top-level field 'Records'
  // that is the array of entries. This streaming JSON
  // reader is an efficient way to get the entries without
  // loading the entire file into memory.
  const pipeline = chain([
    readable,
    ...(isGzipped(inputPath) ? [createGunzip()] : []),
    parser(),
    pick({ filter: 'Records' }),
    streamArray(),
  ]);

  for await (const { value } of pipeline) {
    yield value;
  }
}