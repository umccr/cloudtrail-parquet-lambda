import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.11", async () => {
  const pq = await getFirstConversion("test_input/examples/1_11.json", "");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.11");
  expect(row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(row).toHaveProperty("eventName", "PutObject");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.0");
  expect(row).toHaveProperty("userAgent", "aws-cli/2.15.0 Python/3.12.0");
  expect(row).toHaveProperty("requestID", "EXAMPLE0REQID00001");
  expect(row).toHaveProperty("eventID", "CEXAMPLE-1234-5678-abcd-EXAMPLE00003");
  expect(row).toHaveProperty("readOnly", false);
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("recipientAccountId", "111122223333");
  expect(row).toHaveProperty("eventCategory", "Data");
  expect(row.eventTime).not.toBeNull();

  // Fields absent in this event
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

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "AIDA6ON6E4XEGIEXAMPLE");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::111122223333:user/Terry");
  expect(row).toHaveProperty("userIdentity.accountId", "111122223333");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", "Terry");
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty(
    "userIdentity.sessionContext",
    JSON.stringify({ attributes: { creationDate: "2024-06-01T08:00:00Z", mfaAuthenticated: "true", sessionCredentialFromConsole: "false" } }),
  );

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ bucketName: "my-secure-bucket", key: "uploads/data.parquet" }),
  );
  expect(row).toHaveProperty(
    "responseElements",
    JSON.stringify({ "x-amz-request-id": "EXAMPLE0REQID00001", "x-amz-id-2": "EXAMPLEID2000001" }),
  );
  expect(row).toHaveProperty(
    "additionalEventData",
    JSON.stringify({ SignatureVersion: "SigV4", CipherSuite: "TLS_AES_256_GCM_SHA384", bytesTransferredIn: 204800, bytesTransferredOut: 0, SSEApplied: "SSE-KMS", "x-amz-id-2": "EXAMPLEID2000001" }),
  );
  expect(row).toHaveProperty(
    "tlsDetails",
    JSON.stringify({ tlsVersion: "TLSv1.3", cipherSuite: "TLS_AES_256_GCM_SHA384", clientProvidedHostHeader: "s3.us-east-1.amazonaws.com" }),
  );
  expect(row).toHaveProperty(
    "eventContext.requestContext",
    JSON.stringify({ conditionKeys: { "aws:ViaAWSService": "false", "aws:SecureTransport": "true", "aws:MultiFactorAuthPresent": "false" } }),
  );
  expect(row).toHaveProperty(
    "eventContext.tagContext",
    JSON.stringify({ resourceTags: [{ arn: "arn:aws:s3:::my-secure-bucket", tags: { Environment: "prod", DataClassification: "sensitive" } }] }),
  );

  // resources list (2 entries; first has no accountId)
  expect(row).toHaveProperty("resources.0.type", "AWS::S3::Object");
  expect(row).toHaveProperty("resources.0.ARN", "arn:aws:s3:::my-secure-bucket/uploads/data.parquet");
  expect(row).toHaveProperty("resources.0.accountId", null);
  expect(row).toHaveProperty("resources.1.accountId", "111122223333");
  expect(row).toHaveProperty("resources.1.type", "AWS::S3::Bucket");
  expect(row).toHaveProperty("resources.1.ARN", "arn:aws:s3:::my-secure-bucket");
});