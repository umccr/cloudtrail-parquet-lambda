import * as arrow from "apache-arrow";

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

type FlatRecordKeys = keyof FlatRecord;

// ── Type helpers ──────────────────────────────────────────────────────────────
const str = (v: unknown): string | null => (v == null ? null : String(v));
const bool = (v: unknown): boolean | null =>
  v == null ? null : v === true || v === "true";
const ts = (v: unknown): bigint | null =>
  v == null ? null : BigInt(new Date(v as string).getTime());
const json = (v: unknown): string | null =>
  v == null ? null : JSON.stringify(v);

// Recursively redact known high-cardinality / sensitive keys that add no
// analytical value when stored in Parquet (e.g. STS session tokens).
const REDACTED_KEYS = new Set(["sessionToken", "x-amz-id-2"]);
function redactSensitive(v: unknown): unknown {
  if (v == null || typeof v !== "object") return v;
  if (Array.isArray(v)) return v.map(redactSensitive);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    out[k] = REDACTED_KEYS.has(k) ? "-" : redactSensitive(val);
  }
  return out;
}
const jsonRedacted = (v: unknown): string | null =>
  v == null ? null : JSON.stringify(redactSensitive(v));

export enum CloudTrailVersion {
  VER_1_0,
  VER_1_01,
  VER_1_02,
  VER_1_03,
  VER_1_04,
  VER_1_05,
  VER_1_06,
  VER_1_07,
  VER_1_08,
  VER_1_09,
  VER_1_10,
  VER_1_11,
}

/**
 * A little syntactic sugar to make sure our name values match up with our
 * fields definitions from FlatRecord.
 *
 * Note that 'since' is not actually used - but might be useful in the future
 * so I have put it in the definitions.
 */
function mf(
  name: FlatRecordKeys,
  type: any,
  since: CloudTrailVersion,
  optional: boolean,
): arrow.Field {
  return new arrow.Field(name, type, optional);
}

// Hand matched to
// We chose to match the order as per the docs
// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-record-contents.html
export const SCHEMA = new arrow.Schema([
  // The date and time the request was completed, in coordinated universal time (UTC). An event's time
  // stamp comes from the local host that provides the service API endpoint on which the API call was
  // made. For example, a CreateBucket API event that is run in the US West (Oregon) Region would
  // get its time stamp from the time on an AWS host running the Amazon S3 endpoint,
  // s3.us-west-2.amazonaws.com. In general, AWS services use Network Time Protocol (NTP) to synchronize their system clocks.
  mf(
    "eventTime",
    new arrow.TimestampMillisecond(),
    CloudTrailVersion.VER_1_0,
    false,
  ),

  // The version of the log event format. The current version is 1.11.
  // The eventVersion value is a major and minor version in the form major_version.minor_version.
  // For example, you can have an eventVersion value of 1.10, where 1 is the major version,
  // and 10 is the minor version.
  mf("eventVersion", new arrow.Utf8(), CloudTrailVersion.VER_1_0, false),

  // userIdentity — nested struct; sessionContext serialised as a JSON string
  mf(
    "userIdentity",
    new arrow.Struct([
      // The type of the identity.
      new arrow.Field("type", new arrow.Utf8(), true),
      // The friendly name of the identity that made the call.
      new arrow.Field("userName", new arrow.Utf8(), true),
      // A unique identifier for the entity that made the call.
      new arrow.Field("principalId", new arrow.Utf8(), true),
      // The Amazon Resource Name (ARN) of the principal that made the call.
      new arrow.Field("arn", new arrow.Utf8(), true),
      // The account that owns the entity that granted permissions for the request.
      new arrow.Field("accountId", new arrow.Utf8(), true),
      // The access key ID that was used to sign the request.
      new arrow.Field("accessKeyId", new arrow.Utf8(), true),
      // If the request was made with temporary security credentials, sessionContext
      // provides information about the session created for those credentials.
      new arrow.Field(
        "sessionContext",
        new arrow.Struct([
          new arrow.Field(
            "sessionIssuer",
            new arrow.Struct([
              new arrow.Field("type", new arrow.Utf8(), true),
              new arrow.Field("userName", new arrow.Utf8(), true),
              new arrow.Field("principalId", new arrow.Utf8(), true),
              new arrow.Field("arn", new arrow.Utf8(), true),
              new arrow.Field("accountId", new arrow.Utf8(), true),
            ]),
            true,
          ),
          new arrow.Field(
            "webIdFederationData",
            new arrow.Struct([
              new arrow.Field("federatedProvider", new arrow.Utf8(), true),
              new arrow.Field(
                "attributes",
                new arrow.Struct([
                  new arrow.Field("appid", new arrow.Utf8(), true),
                  new arrow.Field("aud", new arrow.Utf8(), true),
                ]),
                true,
              ),
            ]),
            true,
          ),
          new arrow.Field(
            "attributes",
            new arrow.Struct([
              new arrow.Field("creationDate", new arrow.Utf8(), true),
              new arrow.Field("mfaAuthenticated", new arrow.Utf8(), true),
              new arrow.Field(
                "sessionCredentialFromConsole",
                new arrow.Utf8(),
                true,
              ),
            ]),
            true,
          ),
          new arrow.Field("assumedRoot", new arrow.Utf8(), true),
          new arrow.Field("sourceIdentity", new arrow.Utf8(), true),
          new arrow.Field("ec2RoleDelivery", new arrow.Utf8(), true),
        ]),
        true,
      ),
      // The name of the AWS service that made the request, when a request is made by
      // an AWS service such as Amazon EC2 Auto Scaling or AWS Elastic Beanstalk.
      new arrow.Field("invokedBy", new arrow.Utf8(), true),
      // The AWS account ID of a product provider using temporary delegated access.
      new arrow.Field(
        "invokedByDelegate",
        new arrow.Struct([
          new arrow.Field("accountId", new arrow.Utf8(), true),
        ]),
        true,
      ),
      new arrow.Field(
        "onBehalfOf",
        new arrow.Struct([
          new arrow.Field("userId", new arrow.Utf8(), true),
          new arrow.Field("identityStoreArn", new arrow.Utf8(), true),
        ]),
        true,
      ),
      // Service-to-service request scope info.
      new arrow.Field(
        "inScopeOf",
        new arrow.Struct([
          new arrow.Field("sourceArn", new arrow.Utf8(), true),
          new arrow.Field("sourceAccount", new arrow.Utf8(), true),
          new arrow.Field("issuerType", new arrow.Utf8(), true),
          new arrow.Field("credentialsIssuedTo", new arrow.Utf8(), true),
        ]),
        true,
      ),
      // Credential ID for bearer token requests (e.g. IAM Identity Center access tokens).
      new arrow.Field("credentialId", new arrow.Utf8(), true),
      // External identity provider principal name (SAMLUser / WebIdentityUser only).
      new arrow.Field("identityProvider", new arrow.Utf8(), true),
    ]),
    CloudTrailVersion.VER_1_0,
    true,
  ),

  // The service that the request was made to. This name is typically a short form of the service name
  // without spaces plus .amazonaws.com. For example:
  // CloudFormation is cloudformation.amazonaws.com.
  // Amazon EC2 is ec2.amazonaws.com.
  // Amazon Simple Workflow Service is swf.amazonaws.com.
  // This convention has some exceptions. For example, the eventSource for Amazon CloudWatch
  // is monitoring.amazonaws.com.
  mf("eventSource", new arrow.Utf8(), CloudTrailVersion.VER_1_0, false),

  // The requested action, which is one of the actions in the API for that service.
  mf("eventName", new arrow.Utf8(), CloudTrailVersion.VER_1_0, false),

  // The AWS Region that the request was made to, such as us-east-2.
  mf("awsRegion", new arrow.Utf8(), CloudTrailVersion.VER_1_0, false),

  // The IP address that the request was made from. For actions that originate from the service console,
  // the address reported is for the underlying customer resource, not the console web server. For
  // services in AWS, only the DNS name is displayed.
  mf("sourceIPAddress", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // The agent through which the request was made, such as the AWS Management Console,
  // an AWS service, the AWS SDKs or the AWS CLI.
  // This field has a maximum size of 1 KB; content exceeding that limit is truncated. For event
  // data stores configured to have a maximum event size of 1 MB, the field content is only
  // truncated if the event payload exceeds 1 MB and the maximum field size is exceeded.
  //
  // The following are example values:
  // lambda.amazonaws.com – The request was made with AWS Lambda.
  // aws-sdk-java – The request was made with the AWS SDK for Java.
  // aws-sdk-ruby – The request was made with the AWS SDK for Ruby.
  // aws-cli/1.3.23 Python/2.7.6 Linux/2.6.18-164.el5 – The request was made with the AWS CLI installed on Linux.
  mf("userAgent", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // The AWS service error if the request returns an error. For an example that show
  // s this field, see Error code and message log example.
  // This field has a maximum size of 1 KB; content exceeding that limit is truncated.
  // For event data stores configured to have a maximum event size of 1 MB, the field
  // content is only truncated if the event payload exceeds 1 MB and the maximum field size is exceeded.
  // For network activity events, when there is a VPC endpoint policy violation, the error code is VpceAccessDenied.
  mf("errorCode", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // If the request returns an error, the description of the error. This message includes
  // messages for authorization failures. CloudTrail captures the message logged by the service
  // in its exception handling. For an example, see Error code and message log example.
  //
  // This field has a maximum size of 1 KB; content exceeding that limit is truncated. For
  // event data stores configured to have a maximum event size of 1 MB, the field content
  // is only truncated if the event payload exceeds 1 MB and the maximum field size is exceeded.
  //
  // For network activity events, when there is a VPC endpoint policy violation, the
  // errorMessage will always be the following message: The request was denied due to
  // a VPC endpoint policy. For more information about access denied events for VPC
  // endpoint policy violations, see Access denied error message examples in the IAM
  // User Guide. For an example network activity event showing a VPC endpoint policy
  // violation, see Network activity events in this guide.
  mf("errorMessage", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // The parameters, if any, that were sent with the request. These parameters are documented in the
  // API reference documentation for the appropriate AWS service. This field has a maximum size
  // of 100 KB. When the field size exceeds 100 KB, the requestParameters content is omitted. For
  // event data stores configured to have a maximum event size of 1 MB, the field content is only
  // omitted if the event payload exceeds 1 MB and the maximum field size is exceeded.
  mf("requestParameters", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // The response elements, if any, for actions that make changes (create, update, or delete
  // actions). For readOnly APIs, this field is null. If the action doesn't return response
  // elements, this field is null. The response elements for actions are documented in the
  // API reference documentation for the appropriate AWS service.
  // This field has a maximum size of 100 KB. When the field size exceeds 100 KB, the reponseElements
  // content is omitted. For event data stores configured to have a maximum event size of 1 MB,
  // the field content is only omitted if the event payload exceeds 1 MB and the maximum field
  // size is exceeded.
  //
  // The responseElements value is useful to help you trace a request with AWS Support. Both
  // x-amz-request-id and x-amz-id-2 contain information that helps you trace a request with
  // Support. These values are the same as those that the service returns in the response
  // to the request that initiates the events, so you can use them to match the event to the request.
  mf("responseElements", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // Additional data about the event that was not part of the request or response. This field
  // has a maximum size of 28 KB. When the field size exceeds 28 KB, the additionalEventData
  // content is omitted. For event data stores configured to have a maximum event size of 1 MB,
  // the field content is only omitted if the event payload exceeds 1 MB and the maximum
  // field size is exceeded.
  //
  // The content of additionalEventData is variable. For example, for AWS Management Console
  // sign-in events, additionalEventData could include the MFAUsed field with a value of Yes
  // if the request was made by a root or IAM user using multi-factor authentication (MFA).
  mf("additionalEventData", new arrow.Utf8(), CloudTrailVersion.VER_1_0, true),

  // The value that identifies the request. The service being called generates this value.
  // This field has a maximum size of 1 KB; content exceeding that limit is truncated.
  // For event data stores configured to have a maximum event size of 1 MB, the
  // field content is only truncated if the event payload exceeds 1 MB and the
  // maximum field size is exceeded.
  mf("requestID", new arrow.Utf8(), CloudTrailVersion.VER_1_01, true),

  // GUID generated by CloudTrail to uniquely identify each event. You can use this value to identify a
  // single event. For example, you can use the ID as a primary key to retrieve log data from a searchable database.
  mf("eventID", new arrow.Utf8(), CloudTrailVersion.VER_1_01, false),

  // Identifies the type of event that generated the event record. This can be the one of the following values:
  //
  // AwsApiCall – An API was called.
  // AwsServiceEvent – The service generated an event related to your trail. For example, this can occur when another account made a call with a resource that you own.
  // AwsConsoleAction – An action was taken in the console that was not an API call.
  // AwsConsoleSignIn – A user in your account (root, IAM, federated, SAML, or SwitchRole) signed in to the AWS Management Console.
  // AwsVpceEvents – CloudTrail network activity events enable VPC endpoint owners to record AWS API calls made using their VPC endpoints from a private VPC to the AWS service. To record network activity events, the VPC endpoint owner must enable network activity events for the event source.
  mf("eventType", new arrow.Utf8(), CloudTrailVersion.VER_1_02, false),

  // Identifies the API version associated with the AwsApiCall eventType value.
  mf("apiVersion", new arrow.Utf8(), CloudTrailVersion.VER_1_01, true),

  // A Boolean value that identifies whether the event is a management event. managementEvent is shown in an event record if eventVersion is 1.06 or higher, and the event type is one of the following:
  //
  // AwsApiCall
  // AwsConsoleAction
  // AwsConsoleSignIn
  // AwsServiceEvent
  mf("managementEvent", new arrow.Bool(), CloudTrailVersion.VER_1_06, true),

  // Identifies whether this operation is a read-only operation. This can be one of the following values:
  // true – The operation is read-only (for example, DescribeTrails).
  // false – The operation is write-only (for example, DeleteTrail).
  mf("readOnly", new arrow.Bool(), CloudTrailVersion.VER_1_01, true),

  // A list of resources accessed in the event. The field can contain the following information:
  // Resource ARNs
  // Account ID of the resource owner
  // Resource type identifier in the format: AWS::aws-service-name::data-type-name
  // For example, when an AssumeRole event is logged, the resources field can appear like the following:
  // ARN: arn:aws:iam::123456789012:role/myRole
  // Account ID: 123456789012
  // Resource type identifier: AWS::IAM::Role
  mf(
    "resources",
    new arrow.List(
      new arrow.Field(
        "item",
        new arrow.Struct([
          new arrow.Field("ARN", new arrow.Utf8(), true),
          new arrow.Field("accountId", new arrow.Utf8(), true),
          new arrow.Field("type", new arrow.Utf8(), true),
        ]),
        true,
      ),
    ),
    CloudTrailVersion.VER_1_01,
    true,
  ),

  // Represents the account ID that received this event. The recipientAccountID may be
  // different from the CloudTrail userIdentity element accountId. This can occur
  // in cross-account resource access. For example, if a KMS key, also known as an
  // AWS KMS key, was used by a separate account to call the Encrypt API, the
  // accountId and recipientAccountID values will be the same for the event delivered
  // to the account that made the call, but the values will be different for the
  // event that is delivered to the account that owns the KMS key.
  mf("recipientAccountId", new arrow.Utf8(), CloudTrailVersion.VER_1_02, true),

  // Identifies the service event, including what triggered the event and the result.
  // For more information, see AWS service events. This field has a maximum size
  // of 100 KB. When the field size exceeds 100 KB, the serviceEventDetails content is
  // omitted. For event data stores configured to have a maximum event size of 1 MB,
  // the field content is only omitted if the event payload exceeds 1 MB and the
  // maximum field size is exceeded.
  mf("serviceEventDetails", new arrow.Utf8(), CloudTrailVersion.VER_1_05, true),

  // GUID generated by CloudTrail to uniquely identify CloudTrail events from
  // the same AWS action that is sent to different AWS accounts.
  mf("sharedEventID", new arrow.Utf8(), CloudTrailVersion.VER_1_03, true),

  // Identifies the AWS account ID of the VPC endpoint owner for the corresponding
  // endpoint for which a request has traversed.
  mf("vpcEndpointId", new arrow.Utf8(), CloudTrailVersion.VER_1_04, true),

  // Identifies the AWS account ID of the VPC endpoint owner for the corresponding
  // endpoint for which a request has traversed.
  mf(
    "vpcEndpointAccountId",
    new arrow.Utf8(),
    CloudTrailVersion.VER_1_09,
    true,
  ),

  mf("eventCategory", new arrow.Utf8(), CloudTrailVersion.VER_1_07, false),

  mf("addendum", new arrow.Utf8(), CloudTrailVersion.VER_1_10, true),

  mf(
    "sessionCredentialFromConsole",
    new arrow.Bool(),
    CloudTrailVersion.VER_1_08,
    true,
  ),

  // eventContext — nested struct; requestContext and tagContext serialised as JSON strings
  // these are only present in enhriched mode and inside CloudTrail lake
  // (so we may never see these)
  mf(
    "eventContext",
    new arrow.Struct([
      new arrow.Field("requestContext", new arrow.Utf8(), true),
      new arrow.Field("tagContext", new arrow.Utf8(), true),
    ]),
    CloudTrailVersion.VER_1_11,
    true,
  ),

  // edge device details
  mf("edgeDeviceDetails", new arrow.Utf8(), CloudTrailVersion.VER_1_08, true),

  // Shows information about the Transport Layer Security (TLS) version, cipher suites, and
  // the fully qualified domain name (FQDN) of the client-provided host name used in
  // the service API call, which is typically the FQDN of the service endpoint. CloudTrail
  // still logs partial TLS details if expected information is missing or empty. For example,
  // if the TLS version and cipher suite are present, but the HOST header is empty,
  // available TLS details are still logged in the CloudTrail event.
  mf("tlsDetails", new arrow.Utf8(), CloudTrailVersion.VER_1_08, true),
]);

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
 * @param r
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
    responseElements: jsonRedacted(r.responseElements),
    additionalEventData: jsonRedacted(r.additionalEventData),
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
