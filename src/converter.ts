import {RecordBatch} from "apache-arrow";
import {streamRecords} from "./reader";
import {flattenRecord} from "./schema";
import {buildBatch, writeBatchesToParquet} from "./writer";
import {Compression} from "parquet-wasm/node";

const BATCH_SIZE = 5_000; // tune: larger = faster, more memory

/**
 * Convert the cloud trail log
 * @param inputPath
 * @param compression
 */
export async function convert(inputPath: string, compression = Compression.SNAPPY): Promise<Uint8Array> {
    const batches: RecordBatch[] = [];
    let buffer = [];
    let totalRows = 0;

    for await (const record of streamRecords(inputPath)) {
        buffer.push(flattenRecord(record));

        if (buffer.length >= BATCH_SIZE) {
            batches.push(buildBatch(buffer));
            totalRows += buffer.length;
            buffer = [];
        }
    }

    if (buffer.length > 0) {
        batches.push(buildBatch(buffer));
        totalRows += buffer.length;
    }

    if (totalRows === 0) {
        throw new Error('No records found.');

    }

    console.log(`\r  Processed ${totalRows.toLocaleString()} records — writing parquet...`);

    return await writeBatchesToParquet(batches, compression);

    //const elapsed = ((Date.now() - startMs) / 1000).toFixed(1);
    //console.log(`✓ Done: ${written.toLocaleString()} rows → ${outputPath} (${elapsed}s)`);
}