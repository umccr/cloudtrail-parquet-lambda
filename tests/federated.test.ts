import { expect, test } from "bun:test";
import { readParquetBuffer } from "./helpers/hyparquet_helper";
import { getFirstConversion } from "./helpers/convert_helper";

test("federated", async () => {
  const pq = await getFirstConversion("test_input/examples/federated.json", "2024-08-10");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  expect(row).toHaveProperty("eventVersion", "1.11");
  expect(row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(row).toHaveProperty("eventName", "PutObject");
  expect(row).toHaveProperty("awsRegion", "us-west-2");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", false);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("eventCategory", "Data");

  // userIdentity — identityProvider
  expect(row).toHaveProperty("userIdentity.type", "AssumedRole");
  expect(row).toHaveProperty("userIdentity.identityProvider", "https://accounts.google.com");

  // sessionContext — webIdFederationData (federatedProvider + nested attributes)
  expect(row).toHaveProperty("userIdentity.sessionContext.webIdFederationData.federatedProvider", "https://accounts.google.com");
  expect(row).toHaveProperty("userIdentity.sessionContext.webIdFederationData.attributes.appid", "123456789.apps.googleusercontent.com");
  expect(row).toHaveProperty("userIdentity.sessionContext.webIdFederationData.attributes.aud", "123456789.apps.googleusercontent.com");

  // sessionContext — sourceIdentity and ec2RoleDelivery
  expect(row).toHaveProperty("userIdentity.sessionContext.sourceIdentity", "john.doe@example.com");
  expect(row).toHaveProperty("userIdentity.sessionContext.ec2RoleDelivery", "2.0");

  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", "Role");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", "WebIdentityRole");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2024-08-10T15:00:00Z");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");
});
