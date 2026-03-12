import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.10", async () => {
  const pq = await getFirstConversion("test_input/examples/1_10.json", "2024-01-15");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.10");
  expect(row).toHaveProperty("eventSource", "ec2.amazonaws.com");
  expect(row).toHaveProperty("eventName", "DescribeInstances");
  expect(row).toHaveProperty("awsRegion", "ap-southeast-2");
  expect(row).toHaveProperty("sourceIPAddress", "203.0.113.1");
  expect(row).toHaveProperty("userAgent", "console.ec2.amazonaws.com");
  expect(row).toHaveProperty("requestID", "AEXAMPLE-1234-5678-abcd-EXAMPLE00001");
  expect(row).toHaveProperty("eventID", "BEXAMPLE-1234-5678-abcd-EXAMPLE00002");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", true);
  expect(row).toHaveProperty("managementEvent", true);
  expect(row).toHaveProperty("recipientAccountId", "123456789012");
  expect(row).toHaveProperty("eventCategory", "Management");
  expect(row.eventTime).not.toBeNull();

  // Optional fields absent in this event
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("serviceEventDetails", null);
  expect(row).toHaveProperty("sharedEventID", null);
  expect(row).toHaveProperty("vpcEndpointId", null);
  expect(row).toHaveProperty("vpcEndpointAccountId", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("additionalEventData", null);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("responseElements", null);
  expect(row.resources).toBeUndefined();

  // userIdentity nested struct (AssumedRole — no userName)
  expect(row).toHaveProperty("userIdentity.type", "AssumedRole");
  expect(row).toHaveProperty("userIdentity.principalId", "AROAI3FHNTUD3XMDE2RFF:console-session");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:sts::123456789012:assumed-role/Admin/console-session");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "ASIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", null);
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", "Role");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.principalId", "AROAI3FHNTUD3XMDE2RFF");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.arn", "arn:aws:iam::123456789012:role/Admin");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", "Admin");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2024-01-15T10:00:00Z");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "true");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.sessionCredentialFromConsole", "true");
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ filterSet: {}, instancesSet: {} }),
  );
  expect(row).toHaveProperty(
    "tlsDetails",
    JSON.stringify({ tlsVersion: "TLSv1.3", cipherSuite: "TLS_AES_256_GCM_SHA384", clientProvidedHostHeader: "ec2.ap-southeast-2.amazonaws.com" }),
  );
  expect(row).toHaveProperty(
    "addendum",
    JSON.stringify({ reason: "UPDATED_DATA", updatedFields: "responseElements", originalUID: "BEXAMPLE-1234-5678-abcd-EXAMPLE00002", originalEventID: "BEXAMPLE-1234-5678-abcd-EXAMPLE00002" }),
  );
});