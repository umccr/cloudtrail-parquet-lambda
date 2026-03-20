import { type FlatRecord } from "./cloudtrail_to_arrow";

/**
 * Set up a mechanism by which high-volume but uninformative events
 * can be discarded entirely from our analytics store. This mirrors
 * the build in feature of AWS CloudTrails to discard "KMS" and "RDS"
 * events - but ours is better because that mechanism discards *all*
 * KMS events.
 *
 * TODO: make these configurable at the lambda installation level
 *       rather than require source editing
 */

// KMS crypto operations are discarded only when called by an AWS managed
// service (sourceIPAddress is a service hostname, not a real IP).  A human
// calling Decrypt/Encrypt/etc. directly is a security-relevant event.
const KMS_AUTOMATED_CRYPTO_OPS = new Set([
  "Decrypt",
  "Encrypt",
  "GenerateDataKey",
  "GenerateDataKeyWithoutPlaintext",
  "GenerateDataKeyPair",
  "GenerateDataKeyPairWithoutPlaintext",
  "ReEncryptFrom",
  "ReEncryptTo",
  "Sign",
  "Verify",
]);

// RDS polling events emitted constantly by monitoring agents and the RDS
// control plane itself.  We cannot use sourceIPAddress to distinguish human
// vs automated here (in-account monitoring runs from real IPs), so only the
// highest-volume, lowest-signal ops are listed.  Omitted intentionally:
//   DescribeDBParameterGroups / DescribeDBParameters  — config auditing
//   DescribeDBSubnetGroups                            — network config auditing
//   DownloadDBLogFilePortion                          — human log investigation
const RDS_MONITORING_OPS = new Set([
  "DescribeDBInstances",
  "DescribeDBClusters",
  "DescribeDBLogFiles",
  "DescribeEvents",
  "DescribeDBEngineVersions",
  "DescribeOptionGroups",
  "DescribePendingMaintenanceActions",
]);

// Returns true when the sourceIPAddress is an AWS-managed service endpoint
// (e.g. "s3.amazonaws.com") rather than a real IP address.  These calls are
// initiated by AWS on behalf of the customer, not by a human operator.
function isAwsServiceCaller(sourceIPAddress: string | null): boolean {
  if (!sourceIPAddress) return false;
  return (
    sourceIPAddress.endsWith(".amazonaws.com") ||
    sourceIPAddress.endsWith(".amazon.com")
  );
}

export function shouldDiscard(record: FlatRecord): boolean {
  if (record.eventName === null) return false;

  if (
    record.eventSource === "kms.amazonaws.com" &&
    KMS_AUTOMATED_CRYPTO_OPS.has(record.eventName) &&
    isAwsServiceCaller(record.sourceIPAddress)
  )
    return true;

  return (
    record.eventSource === "rds.amazonaws.com" &&
    RDS_MONITORING_OPS.has(record.eventName)
  );
}
