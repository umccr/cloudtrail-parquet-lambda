import { RecordBatch } from "apache-arrow";
import { streamRecords } from "./reader";
import { flattenRecord } from "./schema";
import {
  buildBatch,
  getRecordBatchSize,
  writeBatchesToParquet,
} from "./writer";
import { Compression } from "parquet-wasm/node";

const BATCH_SIZE = 5_000; // tune: larger = faster, more memory

/**
 * Convert a single cloud trail log file to a sequence
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
 * @param inputPath
 * @param eventTimePrefix
 * @param compression
 */
export async function* convertSingle(
  inputPath: string,
  eventTimePrefix: string,
  compression = Compression.SNAPPY,
): AsyncGenerator<Uint8Array, void, unknown> {
  const batches: RecordBatch[] = [];
  let buffer = [];
  let estimatedBytes = 0;

  for await (const record of streamRecords(inputPath, eventTimePrefix)) {
    buffer.push(flattenRecord(record));

    if (buffer.length >= BATCH_SIZE) {
      const batch = buildBatch(buffer);
      estimatedBytes += getRecordBatchSize(batch);
      batches.push(batch);
      buffer = [];
    }
  }

  console.log(estimatedBytes);

  if (buffer.length > 0) {
    batches.push(buildBatch(buffer));
  }

  yield await writeBatchesToParquet(batches, compression);
}
