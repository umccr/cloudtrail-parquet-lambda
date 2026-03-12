import { expect, test } from "bun:test";
import { readParquetBuffer } from "./hyparquet_helper";
import { getFirstConversion } from "./convert_helper";

test("idc", async () => {
  const pq = await getFirstConversion("test_input/examples/idc.json", "2024-08-10");
  expect(pq).toBeObject();

  const rows = await readParquetBuffer(pq!);
  expect(rows).toBeArrayOfSize(1);
  const row = rows[0]!;

  expect(row).toHaveProperty("eventVersion", "1.11");
  expect(row).toHaveProperty("eventSource", "s3.amazonaws.com");
  expect(row).toHaveProperty("eventName", "GetObject");
  expect(row).toHaveProperty("awsRegion", "us-east-1");
  expect(row).toHaveProperty("eventType", "AwsApiCall");
  expect(row).toHaveProperty("readOnly", true);
  expect(row).toHaveProperty("managementEvent", false);
  expect(row).toHaveProperty("eventCategory", "Data");

  // userIdentity — onBehalfOf, credentialId, inScopeOf
  expect(row).toHaveProperty("userIdentity.type", "AssumedRole");
  expect(row).toHaveProperty("userIdentity.credentialId", "ACCAQOALEXAMPLECREDENTIAL");

  expect(row).toHaveProperty("userIdentity.onBehalfOf.userId", "e0b4f9c0-1234-5678-abcd-ef0123456789");
  expect(row).toHaveProperty("userIdentity.onBehalfOf.identityStoreArn", "arn:aws:identitystore::111122223333:identitystore/d-1234567890");

  expect(row).toHaveProperty("userIdentity.inScopeOf.sourceArn", "arn:aws:lambda:us-east-1:111122223333:function:MyFunction");
  expect(row).toHaveProperty("userIdentity.inScopeOf.sourceAccount", "111122223333");
  expect(row).toHaveProperty("userIdentity.inScopeOf.issuerType", "AWS::Lambda::Function");
  expect(row).toHaveProperty("userIdentity.inScopeOf.credentialsIssuedTo", "arn:aws:iam::111122223333:role/MyLambdaRole");

  // sessionContext.assumedRoot
  expect(row).toHaveProperty("userIdentity.sessionContext.assumedRoot", "true");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.type", "Role");
  expect(row).toHaveProperty("userIdentity.sessionContext.sessionIssuer.userName", "AWSReservedSSO_ReadOnly_abcdef012345");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.creationDate", "2024-08-10T14:00:00Z");
  expect(row).toHaveProperty("userIdentity.sessionContext.attributes.mfaAuthenticated", "false");
  expect((row as any).userIdentity?.sessionContext?.webIdFederationData).toBeUndefined();
});
