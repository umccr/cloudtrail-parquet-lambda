use polars::prelude::*;

// ── sessionContext sub-types ───────────────────────────────────────────────────

pub fn cloudtrail_session_issuer_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("type".into(), DataType::String),
        Field::new("userName".into(), DataType::String),
        Field::new("principalId".into(), DataType::String),
        Field::new("arn".into(), DataType::String),
        Field::new("accountId".into(), DataType::String),
    ])
}

pub fn cloudtrail_web_id_federation_data_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("federatedProvider".into(), DataType::String),
        Field::new(
            "attributes".into(),
            DataType::Struct(vec![
                Field::new("appid".into(), DataType::String),
                Field::new("aud".into(), DataType::String),
            ]),
        ),
    ])
}

pub fn cloudtrail_session_context_attributes_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("creationDate".into(), DataType::String),
        Field::new("mfaAuthenticated".into(), DataType::String),
        Field::new("sessionCredentialFromConsole".into(), DataType::String),
    ])
}

pub fn cloudtrail_session_context_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("sessionIssuer".into(), cloudtrail_session_issuer_dtype()),
        Field::new("webIdFederationData".into(), cloudtrail_web_id_federation_data_dtype()),
        Field::new("attributes".into(), cloudtrail_session_context_attributes_dtype()),
        Field::new("assumedRoot".into(), DataType::String),
        Field::new("sourceIdentity".into(), DataType::String),
        Field::new("ec2RoleDelivery".into(), DataType::String),
    ])
}

// ── userIdentity sub-types ─────────────────────────────────────────────────────

pub fn cloudtrail_invoked_by_delegate_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("accountId".into(), DataType::String),
    ])
}

pub fn cloudtrail_on_behalf_of_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("userId".into(), DataType::String),
        Field::new("identityStoreArn".into(), DataType::String),
    ])
}

pub fn cloudtrail_in_scope_of_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("sourceArn".into(), DataType::String),
        Field::new("sourceAccount".into(), DataType::String),
        Field::new("issuerType".into(), DataType::String),
        Field::new("credentialsIssuedTo".into(), DataType::String),
    ])
}

pub fn cloudtrail_user_identity_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("type".into(), DataType::String),
        Field::new("userName".into(), DataType::String),
        Field::new("principalId".into(), DataType::String),
        Field::new("arn".into(), DataType::String),
        Field::new("accountId".into(), DataType::String),
        Field::new("accessKeyId".into(), DataType::String),
        Field::new("sessionContext".into(), cloudtrail_session_context_dtype()),
        Field::new("invokedBy".into(), DataType::String),
        Field::new("invokedByDelegate".into(), cloudtrail_invoked_by_delegate_dtype()),
        Field::new("onBehalfOf".into(), cloudtrail_on_behalf_of_dtype()),
        Field::new("inScopeOf".into(), cloudtrail_in_scope_of_dtype()),
        Field::new("credentialId".into(), DataType::String),
        Field::new("identityProvider".into(), DataType::String),
    ])
}

// ── resources sub-type ─────────────────────────────────────────────────────────

pub fn cloudtrail_resources_dtype() -> DataType {
    DataType::List(Box::new(DataType::Struct(vec![
        Field::new("ARN".into(), DataType::String),
        Field::new("accountId".into(), DataType::String),
        Field::new("type".into(), DataType::String),
    ])))
}

// ── eventContext sub-type ──────────────────────────────────────────────────────

pub fn cloudtrail_event_context_dtype() -> DataType {
    DataType::Struct(vec![
        Field::new("requestContext".into(), DataType::String),
        Field::new("tagContext".into(), DataType::String),
    ])
}

// ── Top-level field names ──────────────────────────────────────────────────────

pub const CLOUDTRAIL_EVENT_TIME_NAME:                      &str = "eventTime";
pub const CLOUDTRAIL_EVENT_VERSION_NAME:                   &str = "eventVersion";
pub const CLOUDTRAIL_USER_IDENTITY_NAME:                   &str = "userIdentity";
pub const CLOUDTRAIL_EVENT_SOURCE_NAME:                    &str = "eventSource";
pub const CLOUDTRAIL_EVENT_NAME_NAME:                      &str = "eventName";
pub const CLOUDTRAIL_AWS_REGION_NAME:                      &str = "awsRegion";
pub const CLOUDTRAIL_SOURCE_IP_ADDRESS_NAME:               &str = "sourceIPAddress";
pub const CLOUDTRAIL_USER_AGENT_NAME:                      &str = "userAgent";
pub const CLOUDTRAIL_ERROR_CODE_NAME:                      &str = "errorCode";
pub const CLOUDTRAIL_ERROR_MESSAGE_NAME:                   &str = "errorMessage";
pub const CLOUDTRAIL_REQUEST_PARAMETERS_NAME:              &str = "requestParameters";
pub const CLOUDTRAIL_RESPONSE_ELEMENTS_NAME:               &str = "responseElements";
pub const CLOUDTRAIL_ADDITIONAL_EVENT_DATA_NAME:           &str = "additionalEventData";
pub const CLOUDTRAIL_REQUEST_ID_NAME:                      &str = "requestID";
pub const CLOUDTRAIL_EVENT_ID_NAME:                        &str = "eventID";
pub const CLOUDTRAIL_EVENT_TYPE_NAME:                      &str = "eventType";
pub const CLOUDTRAIL_API_VERSION_NAME:                     &str = "apiVersion";
pub const CLOUDTRAIL_MANAGEMENT_EVENT_NAME:                &str = "managementEvent";
pub const CLOUDTRAIL_READ_ONLY_NAME:                       &str = "readOnly";
pub const CLOUDTRAIL_RESOURCES_NAME:                       &str = "resources";
pub const CLOUDTRAIL_RECIPIENT_ACCOUNT_ID_NAME:            &str = "recipientAccountId";
pub const CLOUDTRAIL_SERVICE_EVENT_DETAILS_NAME:           &str = "serviceEventDetails";
pub const CLOUDTRAIL_SHARED_EVENT_ID_NAME:                 &str = "sharedEventID";
pub const CLOUDTRAIL_VPC_ENDPOINT_ID_NAME:                 &str = "vpcEndpointId";
pub const CLOUDTRAIL_VPC_ENDPOINT_ACCOUNT_ID_NAME:         &str = "vpcEndpointAccountId";
pub const CLOUDTRAIL_EVENT_CATEGORY_NAME:                  &str = "eventCategory";
pub const CLOUDTRAIL_ADDENDUM_NAME:                        &str = "addendum";
pub const CLOUDTRAIL_SESSION_CREDENTIAL_FROM_CONSOLE_NAME: &str = "sessionCredentialFromConsole";
pub const CLOUDTRAIL_EVENT_CONTEXT_NAME:                   &str = "eventContext";
pub const CLOUDTRAIL_EDGE_DEVICE_DETAILS_NAME:             &str = "edgeDeviceDetails";
pub const CLOUDTRAIL_TLS_DETAILS_NAME:                     &str = "tlsDetails";

// ── Schema ─────────────────────────────────────────────────────────────────────

pub fn all_fields() -> Schema {
    Schema::from_iter([
        Field::new(CLOUDTRAIL_EVENT_TIME_NAME.into(),                      DataType::Datetime(TimeUnit::Milliseconds, None)),
        Field::new(CLOUDTRAIL_EVENT_VERSION_NAME.into(),                   DataType::String),
        Field::new(CLOUDTRAIL_USER_IDENTITY_NAME.into(),                   cloudtrail_user_identity_dtype()),
        Field::new(CLOUDTRAIL_EVENT_SOURCE_NAME.into(),                    DataType::String),
        Field::new(CLOUDTRAIL_EVENT_NAME_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_AWS_REGION_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_SOURCE_IP_ADDRESS_NAME.into(),               DataType::String),
        Field::new(CLOUDTRAIL_USER_AGENT_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_ERROR_CODE_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_ERROR_MESSAGE_NAME.into(),                   DataType::String),
        Field::new(CLOUDTRAIL_REQUEST_PARAMETERS_NAME.into(),              DataType::String),
        Field::new(CLOUDTRAIL_RESPONSE_ELEMENTS_NAME.into(),               DataType::String),
        Field::new(CLOUDTRAIL_ADDITIONAL_EVENT_DATA_NAME.into(),           DataType::String),
        Field::new(CLOUDTRAIL_REQUEST_ID_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_EVENT_ID_NAME.into(),                        DataType::String),
        Field::new(CLOUDTRAIL_EVENT_TYPE_NAME.into(),                      DataType::String),
        Field::new(CLOUDTRAIL_API_VERSION_NAME.into(),                     DataType::String),
        Field::new(CLOUDTRAIL_MANAGEMENT_EVENT_NAME.into(),                DataType::Boolean),
        Field::new(CLOUDTRAIL_READ_ONLY_NAME.into(),                       DataType::Boolean),
        Field::new(CLOUDTRAIL_RESOURCES_NAME.into(),                       cloudtrail_resources_dtype()),
        Field::new(CLOUDTRAIL_RECIPIENT_ACCOUNT_ID_NAME.into(),            DataType::String),
        Field::new(CLOUDTRAIL_SERVICE_EVENT_DETAILS_NAME.into(),           DataType::String),
        Field::new(CLOUDTRAIL_SHARED_EVENT_ID_NAME.into(),                 DataType::String),
        Field::new(CLOUDTRAIL_VPC_ENDPOINT_ID_NAME.into(),                 DataType::String),
        Field::new(CLOUDTRAIL_VPC_ENDPOINT_ACCOUNT_ID_NAME.into(),         DataType::String),
        Field::new(CLOUDTRAIL_EVENT_CATEGORY_NAME.into(),                  DataType::String),
        Field::new(CLOUDTRAIL_ADDENDUM_NAME.into(),                        DataType::String),
        Field::new(CLOUDTRAIL_SESSION_CREDENTIAL_FROM_CONSOLE_NAME.into(), DataType::Boolean),
        Field::new(CLOUDTRAIL_EVENT_CONTEXT_NAME.into(),                   cloudtrail_event_context_dtype()),
        Field::new(CLOUDTRAIL_EDGE_DEVICE_DETAILS_NAME.into(),             DataType::String),
        Field::new(CLOUDTRAIL_TLS_DETAILS_NAME.into(),                     DataType::String),
    ])
}
