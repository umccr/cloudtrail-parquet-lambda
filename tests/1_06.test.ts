import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.06", async () => {
  const pq = await getFirstConversion("test_input/examples/1_06.json", "2019-10-01");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.06");
  expect(row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(row).toHaveProperty("eventName", "GetObject");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.100");
  expect(row).toHaveProperty("userAgent", "aws-cli/1.16.248");
  expect(row).toHaveProperty("requestID", "D81E97E4B3EEF04C");
  expect(row).toHaveProperty("eventID", "7EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", true);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("recipientAccountId", "123456789012");
  expect(row).toHaveProperty("eventCategory", "Data");
  expect(row.eventTime).not.toBeNull();

  // Optional fields absent in this event
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("serviceEventDetails", null);
  expect(row).toHaveProperty("sharedEventID", null);
  expect(row).toHaveProperty("vpcEndpointId", null);
  expect(row).toHaveProperty("vpcEndpointAccountId", null);
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("responseElements", null);

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "AIDAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::123456789012:user/Alice");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", "Alice");
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect((row as any).userIdentity?.sessionContext).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ bucketName: "my-data-bucket", key: "reports/2019/report.csv" }),
  );
  expect(row).toHaveProperty(
    "additionalEventData",
    JSON.stringify({ SignatureVersion: "SigV4", CipherSuite: "ECDHE-RSA-AES128-SHA", bytesTransferredOut: 12345, "x-amz-id-2": "-" }),
  );

  // resources list (2 entries; first has no accountId)
  expect(row).toHaveProperty("resources.0.type", "AWS::S3::Object");
  expect(row).toHaveProperty("resources.0.ARN", "arn:aws:s3:::my-data-bucket/reports/2019/report.csv");
  expect(row).toHaveProperty("resources.0.accountId", null);
  expect(row).toHaveProperty("resources.1.accountId", "123456789012");
  expect(row).toHaveProperty("resources.1.type", "AWS::S3::Bucket");
  expect(row).toHaveProperty("resources.1.ARN", "arn:aws:s3:::my-data-bucket");
});