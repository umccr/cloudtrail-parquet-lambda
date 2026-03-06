import { expect, test } from "bun:test";
import { convertSingle } from "../src/converter";
import { readParquetBuffer } from "./hyparquet_helper";
import { Compression } from "parquet-wasm";
import { getFirstConversion } from "./convert_helper";

test("1.01", async () => {

  const pq = await getFirstConversion(
    "test_input/examples/1_01.json",
    "2014-07-18",
  );
  const rows = await readParquetBuffer(pq);

  expect(rows).toBeArrayOfSize(1);
  expect(rows[0]).toHaveProperty("eventVersion", "1.01");
  expect(rows[0]).toHaveProperty("userIdentity_type", "IAMUser");
  expect(rows[0]).toHaveProperty("userIdentity_principalId", "EX_PRINCIPAL_ID");
  expect(rows[0]).toHaveProperty(
    "userIdentity_arn",
    "arn:aws:iam::123456789012:user/Alice",
  );
  expect(rows[0]).toHaveProperty("userIdentity_accountId", "123456789012");
  expect(rows[0]).toHaveProperty("userIdentity_accessKeyId", "EXAMPLE_KEY_ID");
  expect(rows[0]).toHaveProperty("userIdentity_userName", "Alice");
  expect(rows[0]).toHaveProperty("eventSource", "iam.amazonaws.com");
  expect(rows[0]).toHaveProperty("eventName", "CreateRole");
  expect(rows[0]).toHaveProperty("awsRegion", "us-east-1");
  expect(rows[0]).toHaveProperty("sourceIPAddress", "192.0.2.1");
  expect(rows[0]).toHaveProperty("eventVersion", "1.01");
  expect(rows[0]).toHaveProperty("eventCategory", "Pre1.07SchemaNull");
});
