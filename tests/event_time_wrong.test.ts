import { expect, test } from "bun:test";
import { convertSingle } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";
import { getFirstConversion } from "./convert_helper";

test("event time wrong", async () => {

  // just like the real 1.01 test but with the event time prefix wrong
  // which will lead to us having no entries - and hence generating
  // an empty (null) parquet file
  const pq = await getFirstConversion(
    "test_input/examples/1_01.json",
    "2014-07-19",
  );

  expect(pq).toBeNull();
});
