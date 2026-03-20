import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

// Fixture has 4 events:
//   1. S3 PutObject (human IP)            → kept
//   2. KMS GenerateDataKey (s3.amazonaws.com sourceIPAddress) → discarded
//   3. KMS Decrypt (human IP 203.0.113.77) → kept (human-initiated crypto)
//   4. KMS CreateKey (human IP)            → kept
// Only the service-sourced GenerateDataKey is discarded; the human Decrypt is kept.
test("discard automated KMS crypto events but keep human-initiated ones", async () => {
  const pq = await getFirstConversion(
    "test_input/examples/discard_kms_crypto.json",
    "2023-11-01",
  );
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(3);

  const eventNames = rows.map((r: any) => r.eventName);
  expect(eventNames).toContain("PutObject");
  expect(eventNames).toContain("Decrypt");
  expect(eventNames).toContain("CreateKey");
  expect(eventNames).not.toContain("GenerateDataKey");

  // S3 PutObject row
  const s3Row = rows.find((r: any) => r.eventName === "PutObject")!;
  expect(s3Row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(s3Row).toHaveProperty("eventID", "KEEP-0001-5678-abcd-EXAMPLE00001");

  // KMS Decrypt row — human-initiated (real IP), must be kept
  const decryptRow = rows.find((r: any) => r.eventName === "Decrypt")!;
  expect(decryptRow).toHaveProperty("eventSource", "kms.amazonaws.com");
  expect(decryptRow).toHaveProperty("eventID", "KEEP-0003-5678-abcd-EXAMPLE00004");
  expect(decryptRow).toHaveProperty("sourceIPAddress", "203.0.113.77");

  // KMS CreateKey row (user-triggered management event — kept)
  const kmsRow = rows.find((r: any) => r.eventName === "CreateKey")!;
  expect(kmsRow).toHaveProperty("eventSource", "kms.amazonaws.com");
  expect(kmsRow).toHaveProperty("eventID", "KEEP-0002-5678-abcd-EXAMPLE00003");
});