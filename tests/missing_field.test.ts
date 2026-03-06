import { expect, test } from "bun:test";
import { convertSingle } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";

test("missing field but schema version says it is mandatory", async () => {

  const asyncThrower = async () => {
    await convertSingle(
      ["test_input/examples/bad1_01.json"],
      "",
      100,
      1,
      Compression.SNAPPY,
    ).next();
  };

  expect(asyncThrower()).rejects.toThrow("this field was null");
});
