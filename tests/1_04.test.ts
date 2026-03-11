import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.04", async () => {
  const pq = await getFirstConversion("test_input/examples/1_04.json", "2016-11-12");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.04");
  expect(row).toHaveProperty("eventSource", "rds.amazonaws.com");
  expect(row).toHaveProperty("eventName", "ApplyPendingMaintenanceAction");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "rds.amazonaws.com");
  expect(row).toHaveProperty("requestID", "5EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventID", "6EXAMPLE-0a1b-2c3d-4e5f-0a1b2c3d4e5f");
  expect(row).toHaveProperty("eventType", "AwsServiceEvent");
  expect(row).toHaveProperty("recipientAccountId", "123456789012");
  expect(row).toHaveProperty("readOnly", false);
  expect(row.eventTime).not.toBeNull();

  // Sentinel strings for mandatory fields not present until a later schema version
  expect(row).toHaveProperty("eventCategory", "Pre1.07SchemaNull");

  // Optional fields absent in this event
  expect(row).toHaveProperty("userAgent", null);
  expect(row).toHaveProperty("errorCode", null);
  expect(row).toHaveProperty("errorMessage", null);
  expect(row).toHaveProperty("apiVersion", null);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("sharedEventID", null);
  expect(row).toHaveProperty("vpcEndpointId", null);
  expect(row).toHaveProperty("vpcEndpointAccountId", null);
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("additionalEventData", null);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("requestParameters", null);
  expect(row).toHaveProperty("responseElements", null);
  expect(row.resources).toBeUndefined();

  // userIdentity nested struct (AWSService — most fields absent)
  expect(row).toHaveProperty("userIdentity.type", "AWSService");
  expect(row).toHaveProperty("userIdentity.principalId", null);
  expect(row).toHaveProperty("userIdentity.arn", null);
  expect(row).toHaveProperty("userIdentity.accountId", null);
  expect(row).toHaveProperty("userIdentity.accessKeyId", null);
  expect(row).toHaveProperty("userIdentity.userName", null);
  expect(row).toHaveProperty("userIdentity.invokedBy", "AWS Internal");
  expect(row).toHaveProperty("userIdentity.sessionContext", null);

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "serviceEventDetails",
    JSON.stringify({ resourceId: "db-ABCDEFGHIJKL01234", resourceType: "RDS:DB", maintenanceAction: "system-update", maintenanceActionStatus: "pending-maintenance" }),
  );
});