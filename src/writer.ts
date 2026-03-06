import * as arrow from "apache-arrow";
import {
  Compression,
  EnabledStatistics,
  Table as WasmTable,
  writeParquet,
  WriterPropertiesBuilder,
} from "parquet-wasm/node";
import { type FlatRecord, SCHEMA } from "./schema.js";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { mkdir, writeFile } from "node:fs/promises";
import { isS3Uri, parseS3Uri } from "./s3_uri";
import { dirname } from "node:path";

// ── Build a typed column Vector via Builder (correctly handles nulls) ─────────
function buildCol<T extends arrow.DataType>(
  type: T,
  values: unknown[],
): arrow.Data<T> {
  const builder = arrow.makeBuilder({ type, nullValues: [null] });
  for (const v of values) builder.append(v as T["TValue"]);

  // Bool-specific fix: when every value in a column is null, arrow-js IPC
  // writer shortcuts to Uint8Array(0) for the data bitmap, which parquet-wasm
  // (arrow-rs) rejects with "Need at least 1 bytes for bitmap in buffers[0]".
  // Cloning with nullCount = length - 1 bypasses the shortcut. The validity
  // bitmap is still all-zeros (all null), so readers correctly see every value
  // as null; the FieldNode nullCount discrepancy is tolerated by downstream tools.
  //if (type instanceof arrow.Bool && data.nullCount >= data.length && data.length > 0) {
  //  return (data as any).clone(data.type, data.offset, data.length, data.length - 1) as arrow.Data<T>;
  // }
  return builder.finish().toVector().data[0]! as arrow.Data<T>;
}

// ── Build an Arrow RecordBatch using the declared SCHEMA ──────────────────────
// We use the RecordBatch(schema, Data<Struct>) constructor overload so the
// declared schema (including nullability) is used directly rather than inferred
// from the column data.
export function buildBatch(rows: FlatRecord[]): arrow.RecordBatch {
  const childData = SCHEMA.fields.map((field: arrow.Field) => {
    const values = rows.map((r) => r[field.name as keyof FlatRecord]);
    return buildCol(field.type, values);
  });

  const structData = arrow.makeData({
    type: new arrow.Struct(SCHEMA.fields),
    children: childData,
    length: rows.length,
  });

  return new arrow.RecordBatch(SCHEMA, structData);
}

// ── Write Arrow RecordBatches to a Parquet file via IPC bridge ────────────────
export async function writeBatchesToParquet(
  batches: arrow.RecordBatch[],
  compression: Compression,
): Promise<Uint8Array> {
  const arrowTable = new arrow.Table(SCHEMA, batches);

  // parquet-wasm has its own Table type — bridge via Arrow IPC stream bytes
  const ipcBytes = arrow.tableToIPC(arrowTable, "stream");
  const wasmTable = WasmTable.fromIPCStream(ipcBytes);

  const writerProps = new WriterPropertiesBuilder()
    .setStatisticsEnabled(EnabledStatistics.Page)
    .setCompression(compression)
    .build();

  return writeParquet(wasmTable, writerProps);
}

export async function writeBytes(uri: string, data: Uint8Array): Promise<void> {
  if (isS3Uri(uri)) {
    const { bucket, key } = parseS3Uri(uri);
    const client = new S3Client({});
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: data,
      }),
    );
  } else {
    // Treat as a local file path (file:// or bare path)
    const localPath = uri.startsWith("file://") ? uri.slice(7) : uri;
    await mkdir(dirname(localPath), { recursive: true });
    await writeFile(localPath, data);
  }
}
