import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("1.09", async () => {
  const pq = await getFirstConversion("test_input/examples/1_09.json", "2023-07-19");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  // Core event fields present in the JSON
  expect(row).toHaveProperty("eventVersion", "1.09");
  expect(row).toHaveProperty("eventSource", "cloudtrail.amazonaws.com");
  expect(row).toHaveProperty("eventName", "StartLogging");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("sourceIPAddress", "192.0.2.0");
  expect(row).toHaveProperty("userAgent", "aws-cli/2.13.5 Python/3.11.4");
  expect(row).toHaveProperty("requestID", "9d478fc1-4f10-490f-a26b-EXAMPLE0e932");
  expect(row).toHaveProperty("eventID", "eae87c48-d421-4626-94f5-EXAMPLEac994");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", false);
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
  expect(row).toHaveProperty("addendum", null);
  expect(row).toHaveProperty("sessionCredentialFromConsole", false);
  expect(row).toHaveProperty("additionalEventData", null);
  expect(row).toHaveProperty("edgeDeviceDetails", null);
  expect(row.eventContext).toBeUndefined();
  expect(row).toHaveProperty("responseElements", null);
  expect(row.resources).toBeUndefined();

  // userIdentity nested struct
  expect(row).toHaveProperty("userIdentity.type", "IAMUser");
  expect(row).toHaveProperty("userIdentity.principalId", "EXAMPLE6E4XEGITWATV6R");
  expect(row).toHaveProperty("userIdentity.arn", "arn:aws:iam::123456789012:user/Mary_Major");
  expect(row).toHaveProperty("userIdentity.accountId", "123456789012");
  expect(row).toHaveProperty("userIdentity.accessKeyId", "AKIAIOSFODNN7EXAMPLE");
  expect(row).toHaveProperty("userIdentity.userName", "Mary_Major");
  expect(row).toHaveProperty("userIdentity.invokedBy", null);
  expect(row).toHaveProperty(
    "userIdentity.sessionContext",
    JSON.stringify({ attributes: { creationDate: "2023-07-19T21:11:57Z", mfaAuthenticated: "false" } }),
  );

  // JSON-serialised complex fields
  expect(row).toHaveProperty(
    "requestParameters",
    JSON.stringify({ name: "myTrail" }),
  );
  expect(row).toHaveProperty(
    "tlsDetails",
    JSON.stringify({ tlsVersion: "TLSv1.3", cipherSuite: "TLS_AES_128_GCM_SHA256", clientProvidedHostHeader: "cloudtrail.us-east-1.amazonaws.com" }),
  );
});