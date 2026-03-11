import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.07", async () => {
  const pq = await getFirstConversion("test_input/examples/1_07.json", "2021-04-10");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.07");
  expect(row).toHaveProperty("eventSource", "kms.amazonaws.com");
  expect(row).toHaveProperty("eventName", "Decrypt");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.10");
  expect(row).toHaveProperty("userAgent", "aws-sdk-java/2.17.0");
  expect(row).toHaveProperty("requestID", "AEXAMPLE-1234-5678-abcd-EXAMPLE00010");
  expect(row).toHaveProperty("eventID", "BEXAMPLE-1234-5678-abcd-EXAMPLE00011");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", true);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("recipientAccountId", "111122223333");
  expect(row).toHaveProperty("eventCategory", "Management");
  expect(row).toHaveProperty("sharedEventID", "CEXAMPLE-1234-5678-abcd-EXAMPLE00099");
  expect(row.eventTime).not.toBeNull();

  // Optional fields absent in this event
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("serviceEventDetails", null);
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
  expect(row).toHaveProperty("userIdentity.principalId", "AROAI3FHNTUD3XMDE2RFF:app-session");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:sts::111122223333:assumed-role/AppRole/app-session");
  expect(row).toHaveProperty("userIdentity.accountId", "111122223333");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "ASIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", null);
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty(
    "userIdentity.sessionContext",
    JSON.stringify({ sessionIssuer: { type: "Role", principalId: "AROAI3FHNTUD3XMDE2RFF", arn: "arn:aws:iam::111122223333:role/AppRole", accountId: "111122223333", userName: "AppRole" }, attributes: { creationDate: "2021-04-10T08:00:00Z", mfaAuthenticated: "false" } }),
  );

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ keyId: "arn:aws:kms:us-east-1:999988887777:key/mrk-EXAMPLE1234", encryptionAlgorithm: "SYMMETRIC_DEFAULT" }),
  );

  // resources list
  expect(row).toHaveProperty("resources.0.accountId", "999988887777");
  expect(row).toHaveProperty("resources.0.type", "AWS::KMS::Key");
  expect(row).toHaveProperty("resources.0.ARN", "arn:aws:kms:us-east-1:999988887777:key/mrk-EXAMPLE1234");
});