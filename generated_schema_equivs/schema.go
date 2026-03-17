package cloudtrailschema

import "github.com/apache/arrow/go/v15/arrow"

// ── sessionContext sub-types ───────────────────────────────────────────────────

func CloudTrailSessionIssuerType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "userName", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "principalId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "arn", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "accountId", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

func CloudTrailWebIdFederationDataType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "federatedProvider", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "attributes", Type: arrow.StructOf(
			arrow.Field{Name: "appid", Type: arrow.BinaryTypes.String, Nullable: true},
			arrow.Field{Name: "aud", Type: arrow.BinaryTypes.String, Nullable: true},
		), Nullable: true},
	)
}

func CloudTrailSessionContextAttributesType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "creationDate", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "mfaAuthenticated", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "sessionCredentialFromConsole", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

func CloudTrailSessionContextType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "sessionIssuer", Type: CloudTrailSessionIssuerType(), Nullable: true},
		arrow.Field{Name: "webIdFederationData", Type: CloudTrailWebIdFederationDataType(), Nullable: true},
		arrow.Field{Name: "attributes", Type: CloudTrailSessionContextAttributesType(), Nullable: true},
		arrow.Field{Name: "assumedRoot", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "sourceIdentity", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "ec2RoleDelivery", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

// ── userIdentity sub-types ─────────────────────────────────────────────────────

func CloudTrailInvokedByDelegateType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "accountId", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

func CloudTrailOnBehalfOfType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "userId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "identityStoreArn", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

func CloudTrailInScopeOfType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "sourceArn", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "sourceAccount", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "issuerType", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "credentialsIssuedTo", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

func CloudTrailUserIdentityType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "userName", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "principalId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "arn", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "accountId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "accessKeyId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "sessionContext", Type: CloudTrailSessionContextType(), Nullable: true},
		arrow.Field{Name: "invokedBy", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "invokedByDelegate", Type: CloudTrailInvokedByDelegateType(), Nullable: true},
		arrow.Field{Name: "onBehalfOf", Type: CloudTrailOnBehalfOfType(), Nullable: true},
		arrow.Field{Name: "inScopeOf", Type: CloudTrailInScopeOfType(), Nullable: true},
		arrow.Field{Name: "credentialId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "identityProvider", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

// ── resources sub-type ─────────────────────────────────────────────────────────

func CloudTrailResourcesType() *arrow.ListType {
	return arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "ARN", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "accountId", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
	))
}

// ── eventContext sub-type ──────────────────────────────────────────────────────

func CloudTrailEventContextType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "requestContext", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tagContext", Type: arrow.BinaryTypes.String, Nullable: true},
	)
}

// ── Top-level field names ──────────────────────────────────────────────────────

const (
	CloudTrailEventTimeName                    = "eventTime"
	CloudTrailEventVersionName                 = "eventVersion"
	CloudTrailUserIdentityName                 = "userIdentity"
	CloudTrailEventSourceName                  = "eventSource"
	CloudTrailEventNameName                    = "eventName"
	CloudTrailAwsRegionName                    = "awsRegion"
	CloudTrailSourceIPAddressName              = "sourceIPAddress"
	CloudTrailUserAgentName                    = "userAgent"
	CloudTrailErrorCodeName                    = "errorCode"
	CloudTrailErrorMessageName                 = "errorMessage"
	CloudTrailRequestParametersName            = "requestParameters"
	CloudTrailResponseElementsName             = "responseElements"
	CloudTrailAdditionalEventDataName          = "additionalEventData"
	CloudTrailRequestIDName                    = "requestID"
	CloudTrailEventIDName                      = "eventID"
	CloudTrailEventTypeName                    = "eventType"
	CloudTrailApiVersionName                   = "apiVersion"
	CloudTrailManagementEventName              = "managementEvent"
	CloudTrailReadOnlyName                     = "readOnly"
	CloudTrailResourcesName                    = "resources"
	CloudTrailRecipientAccountIdName           = "recipientAccountId"
	CloudTrailServiceEventDetailsName          = "serviceEventDetails"
	CloudTrailSharedEventIDName                = "sharedEventID"
	CloudTrailVpcEndpointIdName                = "vpcEndpointId"
	CloudTrailVpcEndpointAccountIdName         = "vpcEndpointAccountId"
	CloudTrailEventCategoryName                = "eventCategory"
	CloudTrailAddendumName                     = "addendum"
	CloudTrailSessionCredentialFromConsoleName = "sessionCredentialFromConsole"
	CloudTrailEventContextName                 = "eventContext"
	CloudTrailEdgeDeviceDetailsName            = "edgeDeviceDetails"
	CloudTrailTlsDetailsName                   = "tlsDetails"
)

// ── Schema ─────────────────────────────────────────────────────────────────────

func AllFields() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: CloudTrailEventTimeName,                    Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: false},
		{Name: CloudTrailEventVersionName,                 Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailUserIdentityName,                 Type: CloudTrailUserIdentityType(), Nullable: true},
		{Name: CloudTrailEventSourceName,                  Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailEventNameName,                    Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailAwsRegionName,                    Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailSourceIPAddressName,              Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailUserAgentName,                    Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailErrorCodeName,                    Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailErrorMessageName,                 Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailRequestParametersName,            Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailResponseElementsName,             Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailAdditionalEventDataName,          Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailRequestIDName,                    Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailEventIDName,                      Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailEventTypeName,                    Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailApiVersionName,                   Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailManagementEventName,              Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: CloudTrailReadOnlyName,                     Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: CloudTrailResourcesName,                    Type: CloudTrailResourcesType(), Nullable: true},
		{Name: CloudTrailRecipientAccountIdName,           Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailServiceEventDetailsName,          Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailSharedEventIDName,                Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailVpcEndpointIdName,                Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailVpcEndpointAccountIdName,         Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailEventCategoryName,                Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: CloudTrailAddendumName,                     Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailSessionCredentialFromConsoleName, Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: CloudTrailEventContextName,                 Type: CloudTrailEventContextType(), Nullable: true},
		{Name: CloudTrailEdgeDeviceDetailsName,            Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: CloudTrailTlsDetailsName,                   Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}
