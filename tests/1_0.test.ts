import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("1.0", async () => {
  const pq = await getFirstConversion("test_input/examples/1_0.json", "2014-03-06");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.0");
  expect(row).toHaveProperty("eventSource", "ec2.amazonaws.com");
  expect(row).toHaveProperty("eventName", "StartInstances");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "205.251.233.176");
  expect(row).toHaveProperty("userAgent", "ec2-api-tools 1.6.12.2");
  expect(row).toHaveProperty("readOnly", false);
  expect(row.eventTime).not.toBeNull();

  // Sentinel strings for mandatory fields not present until a later schema version
  expect(row).toHaveProperty("eventID", "Pre1.01SchemaNull");
  expect(row).toHaveProperty("eventType", "Pre1.02SchemaNull");
  expect(row).toHaveProperty("eventCategory", "Pre1.07SchemaNull");

  // Optional fields absent in this event
  expect(row).toHaveProperty("requestID", null);
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
    "requestParameters",
    JSON.stringify({ instancesSet: { items: [{ instanceId: "i-ebeaf9e2" }] } }),
  );
  expect(row).toHaveProperty(
    "responseElements",
    JSON.stringify({ instancesSet: { items: [{ instanceId: "i-ebeaf9e2", currentState: { code: 0, name: "pending" }, previousState: { code: 80, name: "stopped" } }] } }),
  );
});