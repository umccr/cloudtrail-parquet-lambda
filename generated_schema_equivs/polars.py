# ── sessionContext sub-types ───────────────────────────────────────────────────

cloudtrail_session_issuer_dtype = pl.Struct([
    pl.Field("type", pl.String),
    pl.Field("userName", pl.String),
    pl.Field("principalId", pl.String),
    pl.Field("arn", pl.String),
    pl.Field("accountId", pl.String),
])

cloudtrail_web_id_federation_data_dtype = pl.Struct([
    pl.Field("federatedProvider", pl.String),
    pl.Field("attributes", pl.Struct([
        pl.Field("appid", pl.String),
        pl.Field("aud", pl.String),
    ])),
])

cloudtrail_session_context_attributes_dtype = pl.Struct([
    pl.Field("creationDate", pl.String),
    pl.Field("mfaAuthenticated", pl.String),
    pl.Field("sessionCredentialFromConsole", pl.String),
])

cloudtrail_session_context_dtype = pl.Struct([
    pl.Field("sessionIssuer", cloudtrail_session_issuer_dtype),
    pl.Field("webIdFederationData", cloudtrail_web_id_federation_data_dtype),
    pl.Field("attributes", cloudtrail_session_context_attributes_dtype),
    pl.Field("assumedRoot", pl.String),
    pl.Field("sourceIdentity", pl.String),
    pl.Field("ec2RoleDelivery", pl.String),
])

# ── userIdentity sub-types ─────────────────────────────────────────────────────

cloudtrail_invoked_by_delegate_dtype = pl.Struct([
    pl.Field("accountId", pl.String),
])

cloudtrail_on_behalf_of_dtype = pl.Struct([
    pl.Field("userId", pl.String),
    pl.Field("identityStoreArn", pl.String),
])

cloudtrail_in_scope_of_dtype = pl.Struct([
    pl.Field("sourceArn", pl.String),
    pl.Field("sourceAccount", pl.String),
    pl.Field("issuerType", pl.String),
    pl.Field("credentialsIssuedTo", pl.String),
])

cloudtrail_user_identity_dtype = pl.Struct([
    pl.Field("type", pl.String),
    pl.Field("userName", pl.String),
    pl.Field("principalId", pl.String),
    pl.Field("arn", pl.String),
    pl.Field("accountId", pl.String),
    pl.Field("accessKeyId", pl.String),
    pl.Field("sessionContext", cloudtrail_session_context_dtype),
    pl.Field("invokedBy", pl.String),
    pl.Field("invokedByDelegate", cloudtrail_invoked_by_delegate_dtype),
    pl.Field("onBehalfOf", cloudtrail_on_behalf_of_dtype),
    pl.Field("inScopeOf", cloudtrail_in_scope_of_dtype),
    pl.Field("credentialId", pl.String),
    pl.Field("identityProvider", pl.String),
])

# ── resources sub-type ─────────────────────────────────────────────────────────

cloudtrail_resources_dtype = pl.List(pl.Struct([
    pl.Field("ARN", pl.String),
    pl.Field("accountId", pl.String),
    pl.Field("type", pl.String),
]))

# ── eventContext sub-type ──────────────────────────────────────────────────────

cloudtrail_event_context_dtype = pl.Struct([
    pl.Field("requestContext", pl.String),
    pl.Field("tagContext", pl.String),
])

# ── Top-level fields ───────────────────────────────────────────────────────────

cloudtrail_event_time_name = "eventTime"
cloudtrail_event_time_dtype = pl.Datetime("ms")

cloudtrail_event_version_name = "eventVersion"
cloudtrail_event_version_dtype = pl.String

cloudtrail_user_identity_name = "userIdentity"

cloudtrail_event_source_name = "eventSource"
cloudtrail_event_source_dtype = pl.String

cloudtrail_event_name_name = "eventName"
cloudtrail_event_name_dtype = pl.String

cloudtrail_aws_region_name = "awsRegion"
cloudtrail_aws_region_dtype = pl.String

cloudtrail_source_ip_address_name = "sourceIPAddress"
cloudtrail_source_ip_address_dtype = pl.String

cloudtrail_user_agent_name = "userAgent"
cloudtrail_user_agent_dtype = pl.String

cloudtrail_error_code_name = "errorCode"
cloudtrail_error_code_dtype = pl.String

cloudtrail_error_message_name = "errorMessage"
cloudtrail_error_message_dtype = pl.String

cloudtrail_request_parameters_name = "requestParameters"
cloudtrail_request_parameters_dtype = pl.String

cloudtrail_response_elements_name = "responseElements"
cloudtrail_response_elements_dtype = pl.String

cloudtrail_additional_event_data_name = "additionalEventData"
cloudtrail_additional_event_data_dtype = pl.String

cloudtrail_request_id_name = "requestID"
cloudtrail_request_id_dtype = pl.String

cloudtrail_event_id_name = "eventID"
cloudtrail_event_id_dtype = pl.String

cloudtrail_event_type_name = "eventType"
cloudtrail_event_type_dtype = pl.String

cloudtrail_api_version_name = "apiVersion"
cloudtrail_api_version_dtype = pl.String

cloudtrail_management_event_name = "managementEvent"
cloudtrail_management_event_dtype = pl.Boolean

cloudtrail_read_only_name = "readOnly"
cloudtrail_read_only_dtype = pl.Boolean

cloudtrail_resources_name = "resources"

cloudtrail_recipient_account_id_name = "recipientAccountId"
cloudtrail_recipient_account_id_dtype = pl.String

cloudtrail_service_event_details_name = "serviceEventDetails"
cloudtrail_service_event_details_dtype = pl.String

cloudtrail_shared_event_id_name = "sharedEventID"
cloudtrail_shared_event_id_dtype = pl.String

cloudtrail_vpc_endpoint_id_name = "vpcEndpointId"
cloudtrail_vpc_endpoint_id_dtype = pl.String

cloudtrail_vpc_endpoint_account_id_name = "vpcEndpointAccountId"
cloudtrail_vpc_endpoint_account_id_dtype = pl.String

cloudtrail_event_category_name = "eventCategory"
cloudtrail_event_category_dtype = pl.String

cloudtrail_addendum_name = "addendum"
cloudtrail_addendum_dtype = pl.String

cloudtrail_session_credential_from_console_name = "sessionCredentialFromConsole"
cloudtrail_session_credential_from_console_dtype = pl.Boolean

cloudtrail_event_context_name = "eventContext"

cloudtrail_edge_device_details_name = "edgeDeviceDetails"
cloudtrail_edge_device_details_dtype = pl.String

cloudtrail_tls_details_name = "tlsDetails"
cloudtrail_tls_details_dtype = pl.String

# ── Schema dict ────────────────────────────────────────────────────────────────

all_fields = {
    cloudtrail_event_time_name:                      cloudtrail_event_time_dtype,
    cloudtrail_event_version_name:                   cloudtrail_event_version_dtype,
    cloudtrail_user_identity_name:                   cloudtrail_user_identity_dtype,
    cloudtrail_event_source_name:                    cloudtrail_event_source_dtype,
    cloudtrail_event_name_name:                      cloudtrail_event_name_dtype,
    cloudtrail_aws_region_name:                      cloudtrail_aws_region_dtype,
    cloudtrail_source_ip_address_name:               cloudtrail_source_ip_address_dtype,
    cloudtrail_user_agent_name:                      cloudtrail_user_agent_dtype,
    cloudtrail_error_code_name:                      cloudtrail_error_code_dtype,
    cloudtrail_error_message_name:                   cloudtrail_error_message_dtype,
    cloudtrail_request_parameters_name:              cloudtrail_request_parameters_dtype,
    cloudtrail_response_elements_name:               cloudtrail_response_elements_dtype,
    cloudtrail_additional_event_data_name:           cloudtrail_additional_event_data_dtype,
    cloudtrail_request_id_name:                      cloudtrail_request_id_dtype,
    cloudtrail_event_id_name:                        cloudtrail_event_id_dtype,
    cloudtrail_event_type_name:                      cloudtrail_event_type_dtype,
    cloudtrail_api_version_name:                     cloudtrail_api_version_dtype,
    cloudtrail_management_event_name:                cloudtrail_management_event_dtype,
    cloudtrail_read_only_name:                       cloudtrail_read_only_dtype,
    cloudtrail_resources_name:                       cloudtrail_resources_dtype,
    cloudtrail_recipient_account_id_name:            cloudtrail_recipient_account_id_dtype,
    cloudtrail_service_event_details_name:           cloudtrail_service_event_details_dtype,
    cloudtrail_shared_event_id_name:                 cloudtrail_shared_event_id_dtype,
    cloudtrail_vpc_endpoint_id_name:                 cloudtrail_vpc_endpoint_id_dtype,
    cloudtrail_vpc_endpoint_account_id_name:         cloudtrail_vpc_endpoint_account_id_dtype,
    cloudtrail_event_category_name:                  cloudtrail_event_category_dtype,
    cloudtrail_addendum_name:                        cloudtrail_addendum_dtype,
    cloudtrail_session_credential_from_console_name: cloudtrail_session_credential_from_console_dtype,
    cloudtrail_event_context_name:                   cloudtrail_event_context_dtype,
    cloudtrail_edge_device_details_name:             cloudtrail_edge_device_details_dtype,
    cloudtrail_tls_details_name:                     cloudtrail_tls_details_dtype,
}
