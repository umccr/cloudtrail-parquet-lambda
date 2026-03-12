import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.05", async () => {
  const pq = await getFirstConversion("test_input/examples/1_05.json", "2018-10-21");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.05");
  expect(row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(row).toHaveProperty("eventName", "GetBucketLocation");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "12.34.56.78");
  expect(row).toHaveProperty("userAgent", "signin.amazonaws.com");
  expect(row).toHaveProperty("requestID", "544A415A398A169C");
  expect(row).toHaveProperty("eventID", "5989cc55-f752-468f-b669-f8abbeb008ba");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", false);
  expect(row).toHaveProperty("recipientAccountId", "123456789012");
  expect(row).toHaveProperty("vpcEndpointId", "vpce-aabbccdd00112233");
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
  expect(row).toHaveProperty("vpcEndpointAccountId", null);
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row).toHaveProperty("tlsDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("responseElements", null);
  expect(row.resources).toBeUndefined();

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "AIDAIPFJYALOFOP2DK2SY");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::123456789012:user/Administrator");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", "Administrator");
  expect(row).toHaveProperty("userIdentity.invokedBy", "signin.amazonaws.com");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2018-10-21T09:17:01Z");
  expect((row as any).userIdentity?.sessionContext?.sessionIssuer).toBeUndefined();
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData).toBeUndefined();

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ bucketName: "my-bucket", location: [""] }),
  );
  expect(row).toHaveProperty(
    "additionalEventData",
    JSON.stringify({ vpcEndpointId: "vpce-aabbccdd00112233" }),
  );
});