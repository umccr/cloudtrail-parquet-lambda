import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.02", async () => {
  const pq = await getFirstConversion("test_input/examples/1_02.json", "2015-01-16");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.02");
  expect(row).toHaveProperty("eventSource", "signin.amazonaws.com");
  expect(row).toHaveProperty("eventName", "ConsoleLogin");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.1");
  expect(row).toHaveProperty("userAgent", "Mozilla/5.0");
  expect(row).toHaveProperty("requestID", "1EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventID", "2EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventType", "AwsConsoleSignIn");
  expect(row).toHaveProperty("readOnly", false);
  expect(row.eventTime).not.toBeNull();

  // Sentinel strings for mandatory fields not present until a later schema version
  expect(row).toHaveProperty("eventCategory", "Pre1.07SchemaNull");

  // Optional fields absent in this event
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
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("requestParameters", null);
  expect(row.resources).toBeUndefined();

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
    "responseElements",
    JSON.stringify({ ConsoleLogin: "Success" }),
  );
  expect(row).toHaveProperty(
    "additionalEventData",
    JSON.stringify({ LoginTo: "https://console.aws.amazon.com/console/home", MobileVersion: "No", MFAUsed: "No" }),
  );
});