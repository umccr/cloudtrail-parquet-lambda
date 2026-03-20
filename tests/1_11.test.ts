import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.11", async () => {
  const pq = await getFirstConversion("test_input/examples/1_11.json", "");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(2);
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

  // userIdentity nested struct (AssumedRole with invokedByDelegate)
  expect(row).toHaveProperty("userIdentity.type", "AssumedRole");
  expect(row).toHaveProperty("userIdentity.principalId", "AIDACKCEVSQ6C2EXAMPLE:Role-Session-Name");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:sts::111122223333:assumed-role/Role-Name/Role-Session-Name");
  expect(row).toHaveProperty("userIdentity.accountId", "111122223333");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "[REDACTED:AWS_ACCESS_KEY]");
  expect(row).toHaveProperty("userIdentity.userName", null);
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty("userIdentity.invokedByDelegate.accountId", "444455556666");
  expect((row as any).userIdentity?.onBehalfOf).toBeUndefined();
  expect((row as any).userIdentity?.inScopeOf).toBeUndefined();
  expect(row).toHaveProperty("userIdentity.credentialId", null);
  expect(row).toHaveProperty("userIdentity.identityProvider", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", "Role");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.principalId", "AIDACKCEVSQ6C2EXAMPLE");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.arn", "arn:aws:iam::111122223333:role/Admin");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.accountId", "111122223333");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", "Admin");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2024-09-09T17:50:16Z");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ bucketName: "my-secure-bucket", key: "uploads/data.parquet" }),
  );
  expect(row).toHaveProperty(
    "responseElements",
    JSON.stringify({ "x-amz-request-id": "EXAMPLE0REQID00001", "x-amz-id-2": "-" }),
  );
  expect(row).toHaveProperty(
    "additionalEventData",
    JSON.stringify({ SignatureVersion: "SigV4", CipherSuite: "TLS_AES_256_GCM_SHA384", bytesTransferredIn: 204800, bytesTransferredOut: 0, SSEApplied: "SSE-KMS", "x-amz-id-2": "-" }),
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

  // AssumeRole record — verifies credentials + assumedRoleUser in responseElements
  const assumeRoleRow = rows[1]!;
  expect(assumeRoleRow).toHaveProperty("eventSource", "sts.amazonaws.com");
  expect(assumeRoleRow).toHaveProperty("eventName", "AssumeRole");
  expect(assumeRoleRow).toHaveProperty("managementEvent", true);
  expect(assumeRoleRow).toHaveProperty("eventCategory", "Management");
  expect(assumeRoleRow).toHaveProperty(
    "requestParameters",
    JSON.stringify({ roleArn: "arn:aws:iam::111122223333:role/Admin", roleSessionName: "Role-Session-Name", durationSeconds: 3600 }),
  );
  expect(assumeRoleRow).toHaveProperty(
    "responseElements",
    JSON.stringify({
      credentials: {
        accessKeyId: "ASIAIOSFODNN7EXAMPLE3",
        sessionToken: "-",
        expiration: "Jun 1, 2024 9:15:00 AM",
      },
      assumedRoleUser: {
        assumedRoleId: "AIDACKCEVSQ6C2EXAMPLE:Role-Session-Name",
        arn: "arn:aws:sts::111122223333:assumed-role/Admin/Role-Session-Name",
      },
    }),
  );
  expect(assumeRoleRow).toHaveProperty("resources.0.type", "AWS::IAM::Role");
  expect(assumeRoleRow).toHaveProperty("resources.0.ARN", "arn:aws:iam::111122223333:role/Admin");
  expect(assumeRoleRow).toHaveProperty("resources.0.accountId", "111122223333");
});