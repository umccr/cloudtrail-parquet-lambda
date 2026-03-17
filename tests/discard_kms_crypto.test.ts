import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

// Fixture has 3 events: S3 PutObject, KMS GenerateDataKey, KMS CreateKey.
// GenerateDataKey is an automated crypto operation and must be discarded,
// leaving exactly 2 rows in the output.
test("discard automated KMS crypto events", async () => {
  const pq = await getFirstConversion(
    "test_input/examples/discard_kms_crypto.json",
    "2023-11-01",
  );
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(2);

  const eventNames = rows.map((r: any) => r.eventName);
  expect(eventNames).toContain("PutObject");
  expect(eventNames).toContain("CreateKey");
  expect(eventNames).not.toContain("GenerateDataKey");

  // S3 PutObject row
  const s3Row = rows.find((r: any) => r.eventName === "PutObject")!;
  expect(s3Row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(s3Row).toHaveProperty("eventID", "KEEP-0001-5678-abcd-EXAMPLE00001");

  // KMS CreateKey row (user-triggered management event — kept)
  const kmsRow = rows.find((r: any) => r.eventName === "CreateKey")!;
  expect(kmsRow).toHaveProperty("eventSource", "kms.amazonaws.com");
  expect(kmsRow).toHaveProperty("eventID", "KEEP-0002-5678-abcd-EXAMPLE00003");
});