import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.01", async () => {

  const pq = await getFirstConversion(
    "test_input/examples/1_01.json",
    "2014-07-18",
  );

  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.01");
  expect(row).toHaveProperty("eventSource", "iam.amazonaws.com");
  expect(row).toHaveProperty("eventName", "CreateRole");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.1");
  expect(row).toHaveProperty("userAgent", "aws-cli/1.4.4");
  expect(row).toHaveProperty("requestID", "4EXAMPLE-7e2b-11e4-abb8-3f06f8f3935f");
  expect(row).toHaveProperty("eventID", "aEXAMPLE-4a4f-4f2a-a7ed-c97a67b82974");
  expect(row).toHaveProperty("readOnly", false);
  expect(row.eventTime).not.toBeNull();

  // Sentinel strings for mandatory fields not present until a later schema version
  expect(row).toHaveProperty("eventType", "Pre1.02SchemaNull");
  expect(row).toHaveProperty("eventCategory", "Pre1.07SchemaNull");

  // Fields absent in this event
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("recipientAccountId", null);
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

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "EX_PRINCIPAL_ID");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::123456789012:user/Alice");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "EXAMPLE_KEY_ID");
  expect(row).toHaveProperty("userIdentity.userName", "Alice");
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect((row as any).userIdentity?.sessionContext).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ roleName: "TestRole", assumeRolePolicyDocument: '{"Version":"2012-10-17"}' }),
  );
  expect(row).toHaveProperty(
    "responseElements",
    JSON.stringify({ role: { roleName: "TestRole", roleId: "AROAI3FHNTUD3XMDE2RFF", arn: "arn:aws:iam::123456789012:role/TestRole", createDate: "Jul 18, 2014 3:07:39 PM", assumeRolePolicyDocument: "%7B%22Version%22%3A%222012-10-17%22%7D" } }),
  );

  // resources list
  expect(row).toHaveProperty("resources.0.ARN", "arn:aws:iam::123456789012:role/TestRole");
  expect(row).toHaveProperty("resources.0.accountId", "123456789012");
  expect(row).toHaveProperty("resources.0.type", "AWS::IAM::Role");
});