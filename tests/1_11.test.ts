import { expect, test } from "bun:test";
import { convertSingle } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";
import { getFirstConversion } from "./convert_helper";

test("1.11", async () => {
  const pq = await getFirstConversion("test_data/examples/1_11.json", "");
  const rows = await readParquetBuffer(pq);

  expect(rows).toBeArrayOfSize(1);
  expect(rows[0]).toHaveProperty("eventVersion", "1.11");
  expect(rows[0]).toHaveProperty("userIdentity_type", "IAMUser");
  expect(rows[0]).toHaveProperty("userIdentity_principalId", "AIDA6ON6E4XEGIEXAMPLE");
  expect(rows[0]).toHaveProperty(
    "userIdentity_arn",
    "arn:aws:iam::111122223333:user/Terry",
  );
  expect(rows[0]).toHaveProperty("userIdentity_accountId", "111122223333");
  expect(rows[0]).toHaveProperty("userIdentity_accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(rows[0]).toHaveProperty("userIdentity_userName", "Terry");
  expect(rows[0]).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(rows[0]).toHaveProperty("eventName", "PutObject");
  expect(rows[0]).toHaveProperty("awsRegion", "us-east-1");
  expect(rows[0]).toHaveProperty("sourceIPAddress", "192.0.2.0");
  expect(rows[0]).toHaveProperty("eventCategory", "Data");
});
