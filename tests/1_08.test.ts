import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.08", async () => {
  const pq = await getFirstConversion("test_input/examples/1_08.json", "2023-07-19");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(2);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.08");
  expect(row).toHaveProperty("eventSource", "iam.amazonaws.com");
  expect(row).toHaveProperty("eventName", "CreateUser");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.0");
  expect(row).toHaveProperty("userAgent", "aws-cli/2.13.5 Python/3.11.4");
  expect(row).toHaveProperty("requestID", "2d528c76-329e-4444-9a8d-EXAMPLEa9bb9");
  expect(row).toHaveProperty("eventID", "AEXAMPLE-f1d4-4e5a-a32c-EXAMPLE5c0de");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", false);
  expect(row).toHaveProperty("managementEvent", true);
  expect(row).toHaveProperty("recipientAccountId", "888888888888");
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
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("additionalEventData", null);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row.resources).toBeUndefined();

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "AIDA6ON6E4XEGITEXAMPLE");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::888888888888:user/Mary");
  expect(row).toHaveProperty("userIdentity.accountId", "888888888888");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", "Mary");
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2023-07-19T21:11:57Z");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");

  // sessionIssuer: {} — struct present but all sub-fields null
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.principalId", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.arn", null);
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.accountId", null);

  // webIdFederationData: {} — struct present, federatedProvider null, nested attributes absent
  expect(row).toHaveProperty("userIdentity.sessionContext.webIdFederationData.federatedProvider", null);
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData?.attributes).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ userName: "Richard" }),
  );
  expect(row).toHaveProperty(
    "responseElements",
    JSON.stringify({ user: { path: "/", arn: "arn:aws:iam::888888888888:user/Richard", userId: "AIDA6ON6E4XEP7EXAMPLE", createDate: "Jul 19, 2023 9:25:09 PM", userName: "Richard" } }),
  );

  // AssumeRole record — verifies credentials + assumedRoleUser in responseElements
  const assumeRoleRow = rows[1]!;
  expect(assumeRoleRow).toHaveProperty("eventSource", "sts.amazonaws.com");
  expect(assumeRoleRow).toHaveProperty("eventName", "AssumeRole");
  expect(assumeRoleRow).toHaveProperty(
    "requestParameters",
    JSON.stringify({ roleArn: "arn:aws:iam::999999999999:role/CrossAccountRole", roleSessionName: "MarySession", durationSeconds: 3600 }),
  );
  expect(assumeRoleRow).toHaveProperty(
    "responseElements",
    JSON.stringify({
      credentials: {
        accessKeyId: "ASIAIOSFODNN7EXAMPLE",
        sessionToken: "-",
        expiration: "Jul 19, 2023 10:30:00 PM",
      },
      assumedRoleUser: {
        assumedRoleId: "AROA6ON6E4XEEXAMPLE123:MarySession",
        arn: "arn:aws:sts::999999999999:assumed-role/CrossAccountRole/MarySession",
      },
    }),
  );
});