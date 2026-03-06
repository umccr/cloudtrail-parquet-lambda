import { expect, test } from "bun:test";
import { convertSingle } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";

test("missing field but schema version says it is mandatory", async () => {

  const asyncThrower = async () => {
    for await (const pq of convertSingle("test_data/examples/bad1_01.json", "", Compression.SNAPPY)) {
      break;
    }
  };

  await expect(asyncThrower()).rejects.toThrow("this field was null");
});
