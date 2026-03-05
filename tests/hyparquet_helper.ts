import { parquetRead, parquetMetadata, parquetSchema } from 'hyparquet';

// hyparquet works with ArrayBuffer, not Uint8Array — convert once up front.
function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
    return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

function makeAsyncBuffer(arrayBuffer: ArrayBuffer) {
    return {
        byteLength: arrayBuffer.byteLength,
        slice: (start: number, end?: number) => Promise.resolve(arrayBuffer.slice(start, end)),
    };
}

/**
 * List all column names in a Parquet buffer.
 */
export function parquetColumns(parquetBytes: Uint8Array): string[] {
    const ab = toArrayBuffer(parquetBytes);
    const meta = parquetMetadata(ab);
    const schema = parquetSchema(meta) as any;
    return schema.children.map((c: any) => c.element.name as string);
}

/**
 * Read rows from an in-memory Parquet buffer.
 * Returns plain objects keyed by column name.
 *
 * @param parquetBytes  The parquet file contents
 * @param columns       Which columns to read (omit for all columns)
 *
 * @example
 * const rows = await readParquetBuffer(bytes, ['eventName', 'awsRegion']);
 * const all  = await readParquetBuffer(bytes);
 */
export async function readParquetBuffer<T = Record<string, unknown>>(
    parquetBytes: Uint8Array,
    columns?: string[],
): Promise<T[]> {
    const ab = toArrayBuffer(parquetBytes);
    const rows: T[] = [];

    await parquetRead({
        file:      makeAsyncBuffer(ab) as any,
        columns,
        rowFormat: 'object',
        onComplete: (data: any) => rows.push(...data),
    });

    return rows;
}
