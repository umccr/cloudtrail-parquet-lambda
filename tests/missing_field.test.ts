import { expect, test } from "bun:test";
import { convert } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";

test("missing field but schema version says it is mandatory", async () => {

  const asyncThrower = async () => {
    const pq = await convert("test_data/examples/bad1_01.json", Compression.SNAPPY);
  };

  await expect(asyncThrower()).rejects.toThrow("this field was null");
});
