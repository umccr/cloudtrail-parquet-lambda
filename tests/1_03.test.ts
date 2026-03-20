import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.03", async () => {
  const pq = await getFirstConversion("test_input/examples/1_03.json", "2016-03-14");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.03");
  expect(row).toHaveProperty("eventSource", "kms.amazonaws.com");
  expect(row).toHaveProperty("eventName", "PutKeyPolicy");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.1");
  expect(row).toHaveProperty("userAgent", "aws-sdk-java/1.11.0");
  expect(row).toHaveProperty("requestID", "3EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventID", "4EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", false);
  expect(row).toHaveProperty("recipientAccountId", "123456789012");
  expect(row.eventTime).not.toBeNull();

  // Sentinel strings for mandatory fields not present until a later schema version
  expect(row).toHaveProperty("eventCategory", "Pre1.07SchemaNull");

  // Optional fields absent in this event
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("serviceEventDetails", null);
  expect(row).toHaveProperty("sharedEventID", null);
  expect(row).toHaveProperty("vpcEndpointId", null);
  expect(row).toHaveProperty("vpcEndpointAccountId", null);
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("additionalEventData", null);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("responseElements", null);

  // userIdentity nested struct (AssumedRole — no userName)
  expect(row).toHaveProperty("userIdentity.type", "AssumedRole");
  expect(row).toHaveProperty("userIdentity.principalId", "AROAI3FHNTUD3XMDE2RFF:role-session");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:sts::123456789012:assumed-role/MyRole/role-session");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "ASIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", null);
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", "Role");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.principalId", "AROAI3FHNTUD3XMDE2RFF");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.arn", "arn:aws:iam::123456789012:role/MyRole");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", "MyRole");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2016-03-14T13:51:37Z");
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ keyId: "arn:aws:kms:us-east-1:999999999999:key/mrk-EXAMPLE", policyName: "default", policy: "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},\"Action\":\"kms:*\",\"Resource\":\"*\"}]}" }),
  );

  // resources list
  expect(row).toHaveProperty("resources.0.ARN", "arn:aws:kms:us-east-1:999999999999:key/mrk-EXAMPLE");
  expect(row).toHaveProperty("resources.0.accountId", "999999999999");
  expect(row).toHaveProperty("resources.0.type", "AWS::KMS::Key");
});