/**
 * The flattened Typescript record that is inserted into the
 * Parquet output. Each field here maps one to one with
 * a field in the schema. Note that the Typescript definition
 * here does not define the optionality of any field, for
 * that need to check the Schema itself.
 */
export type FlatRecord = {
  eventVersion: string | null;
  eventTime: bigint | null;
  eventSource: string | null;
  eventName: string | null;
  eventID: string | null;
  eventType: string | null;
  awsRegion: string | null;
  recipientAccountId: string | null;
  requestID: string | null;
  apiVersion: string | null;
  managementEvent: boolean | null;
  readOnly: boolean | null;
  sourceIPAddress: string | null;
  userAgent: string | null;
  vpcEndpointId: string | null;
  errorCode: string | null;
  errorMessage: string | null;
  userIdentity: {
    type: string | null;
    principalId: string | null;
    arn: string | null;
    accountId: string | null;
    accessKeyId: string | null;
    userName: string | null;
    invokedBy: string | null;
    invokedByDelegate: { accountId: string | null } | null;
    sessionContext: {
      sessionIssuer: {
        type: string | null;
        userName: string | null;
        principalId: string | null;
        arn: string | null;
        accountId: string | null;
      } | null;
      webIdFederationData: {
        federatedProvider: string | null;
        attributes: {
          appid: string | null;
          aud: string | null;
        } | null;
      } | null;
      attributes: {
        creationDate: string | null;
        mfaAuthenticated: string | null;
        sessionCredentialFromConsole: string | null;
      } | null;
      assumedRoot: string | null;
      sourceIdentity: string | null;
      ec2RoleDelivery: string | null;
    } | null;
    onBehalfOf: {
      userId: string | null;
      identityStoreArn: string | null;
    } | null;
    inScopeOf: {
      sourceArn: string | null;
      sourceAccount: string | null;
      issuerType: string | null;
      credentialsIssuedTo: string | null;
    } | null;
    credentialId: string | null;
    identityProvider: string | null;
  } | null;
  requestParameters: string | null;
  responseElements: string | null;
  additionalEventData: string | null;
  serviceEventDetails: string | null;
  resources: Array<{
    ARN: string | null;
    accountId: string | null;
    type: string | null;
  }> | null;
  tlsDetails: string | null;
  sharedEventID: string | null;

  vpcEndpointAccountId: string | null;
  eventCategory: string | null;
  addendum: string | null;
  sessionCredentialFromConsole: boolean | null;
  eventContext: {
    requestContext: string | null;
    tagContext: string | null;
  } | null;
  edgeDeviceDetails: string | null;
};

export type FlatRecordKeys = keyof FlatRecord;

// ── Type helpers ──────────────────────────────────────────────────────────────
const str = (v: unknown): string | null => (v == null ? null : String(v));
const bool = (v: unknown): boolean | null =>
  v == null ? null : v === true || v === "true";
const ts = (v: unknown): bigint | null =>
  v == null ? null : BigInt(new Date(v as string).getTime());
const json = (v: unknown): string | null =>
  v == null ? null : JSON.stringify(v);

/**
 * Rather than writing out mixed parquet - we are upgrading all events to the "latest"
 * schema. This means we need to deal with previously empty fields (i.e. empty in "1.0")
 * that have been made mandatory in a later schema. For strings we handle this by
 * inserting a string indicating it was an allowed null from a previous schema.
 *
 * @param ver the event version from the event being serialised
 * @param firstVersionWithField the definitional version at which point this field was introduced
 * @param value the value being serialised
 */
function throwIfNotAllowedMissingMandatoryString(
  ver: string,
  firstVersionWithField: string,
  value: string | null,
): string {
  // if we have a value then we have no problem!
  if (value != null) return value;

  // if it is null we need to check if that is allowable - given it is a non-null field

  const parts = ver.split(".");
  const firstVersionParts = firstVersionWithField.split(".");

  if (parts.length != 2 || firstVersionParts.length != 2)
    throw new Error(`malformed eventVersion string of ${ver}`);

  if (parseInt(parts[0]!) > parseInt(firstVersionParts[0]!))
    throw new Error(
      `this field was null but the event version of ${ver} means that it must be present`,
    );

  if (parseInt(parts[0]!) == parseInt(firstVersionParts[0]!)) {
    if (parseInt(parts[1]!) >= parseInt(firstVersionParts[1]!))
      throw new Error(
        `this field was null but the event version of ${ver} means that it must be present`,
      );
  }

  return `Pre${firstVersionWithField}SchemaNull`;
}

/**
 * Flattens the JSON structure expected from a CloudTrail event
 * and returns a converted record that matches exactly the
 * data we will be pushing into the Arrow table.
 *
 * @param r A CloudTrail event record direct from its json.gz
 */
export function flattenRecord(r: any): FlatRecord {
  const ui = r.userIdentity ?? {};
  const ec = r.eventContext ?? {};

  const ver = str(r.eventVersion);

  if (!ver) throw new Error("eventVersion is a mandatory field");

  return {
    eventTime: ts(r.eventTime),
    eventVersion: str(r.eventVersion),
    userIdentity:
      r.userIdentity == null
        ? null
        : {
            type: str(ui.type),
            principalId: str(ui.principalId),
            arn: str(ui.arn),
            accountId: str(ui.accountId),
            accessKeyId: str(ui.accessKeyId),
            userName: str(ui.userName),
            invokedBy: str(ui.invokedBy),
            invokedByDelegate:
              ui.invokedByDelegate == null
                ? null
                : { accountId: str(ui.invokedByDelegate.accountId) },
            sessionContext:
              ui.sessionContext == null
                ? null
                : {
                    sessionIssuer:
                      ui.sessionContext.sessionIssuer == null
                        ? null
                        : {
                            type: str(ui.sessionContext.sessionIssuer.type),
                            userName: str(
                              ui.sessionContext.sessionIssuer.userName,
                            ),
                            principalId: str(
                              ui.sessionContext.sessionIssuer.principalId,
                            ),
                            arn: str(ui.sessionContext.sessionIssuer.arn),
                            accountId: str(
                              ui.sessionContext.sessionIssuer.accountId,
                            ),
                          },
                    webIdFederationData:
                      ui.sessionContext.webIdFederationData == null
                        ? null
                        : {
                            federatedProvider: str(
                              ui.sessionContext.webIdFederationData
                                .federatedProvider,
                            ),
                            attributes:
                              ui.sessionContext.webIdFederationData
                                .attributes == null
                                ? null
                                : {
                                    appid: str(
                                      ui.sessionContext.webIdFederationData
                                        .attributes.appid,
                                    ),
                                    aud: str(
                                      ui.sessionContext.webIdFederationData
                                        .attributes.aud,
                                    ),
                                  },
                          },
                    attributes:
                      ui.sessionContext.attributes == null
                        ? null
                        : {
                            creationDate: str(
                              ui.sessionContext.attributes.creationDate,
                            ),
                            mfaAuthenticated: str(
                              ui.sessionContext.attributes.mfaAuthenticated,
                            ),
                            sessionCredentialFromConsole: str(
                              ui.sessionContext.attributes
                                .sessionCredentialFromConsole,
                            ),
                          },
                    assumedRoot: str(ui.sessionContext.assumedRoot),
                    sourceIdentity: str(ui.sessionContext.sourceIdentity),
                    ec2RoleDelivery: str(ui.sessionContext.ec2RoleDelivery),
                  },
            onBehalfOf:
              ui.onBehalfOf == null
                ? null
                : {
                    userId: str(ui.onBehalfOf.userId),
                    identityStoreArn: str(ui.onBehalfOf.identityStoreArn),
                  },
            inScopeOf:
              ui.inScopeOf == null
                ? null
                : {
                    sourceArn: str(ui.inScopeOf.sourceArn),
                    sourceAccount: str(ui.inScopeOf.sourceAccount),
                    issuerType: str(ui.inScopeOf.issuerType),
                    credentialsIssuedTo: str(ui.inScopeOf.credentialsIssuedTo),
                  },
            credentialId: str(ui.credentialId),
            identityProvider: str(ui.identityProvider),
          },
    eventSource: str(r.eventSource),
    eventName: str(r.eventName),
    awsRegion: str(r.awsRegion),
    sourceIPAddress: str(r.sourceIPAddress),
    userAgent: str(r.userAgent),
    errorCode: str(r.errorCode),
    errorMessage: str(r.errorMessage),
    requestParameters: json(r.requestParameters),
    responseElements: json(r.responseElements),
    additionalEventData: json(r.additionalEventData),
    requestID: str(r.requestID),
    eventID: throwIfNotAllowedMissingMandatoryString(
      ver,
      "1.01",
      str(r.eventID),
    ),
    eventType: throwIfNotAllowedMissingMandatoryString(
      ver,
      "1.02",
      str(r.eventType),
    ),
    apiVersion: str(r.apiVersion),
    managementEvent: bool(r.managementEvent) ?? false,
    readOnly: bool(r.readOnly) ?? false,
    resources: Array.isArray(r.resources)
      ? (r.resources as any[]).map((res: any) => ({
          ARN: str(res.ARN),
          accountId: str(res.accountId),
          type: str(res.type),
        }))
      : null,
    recipientAccountId: str(r.recipientAccountId),
    serviceEventDetails: json(r.serviceEventDetails),
    sharedEventID: str(r.sharedEventID),
    vpcEndpointId: str(r.vpcEndpointId),
    vpcEndpointAccountId: str(r.vpcEndpointAccountId),
    eventCategory: throwIfNotAllowedMissingMandatoryString(
      ver,
      "1.07",
      str(r.eventCategory),
    ),
    addendum: json(r.addendum),
    sessionCredentialFromConsole: bool(r.sessionCredentialFromConsole) ?? false,
    eventContext:
      r.eventContext == null
        ? null
        : {
            requestContext: json(ec.requestContext),
            tagContext: json(ec.tagContext),
          },
    edgeDeviceDetails: json(r.edgeDeviceDetails),
    tlsDetails: json(r.tlsDetails),
  };
}
