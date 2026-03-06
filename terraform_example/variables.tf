variable "name" {
  description = "Base name for all resources"
  type        = string
  default     = "cloudtrail-to-parquet"
}

variable "aws_region" {
  type    = string
  default = "ap-southeast-2"
}

# ── Image ──────────────────────────────────────────────────────────────────────

variable "ghcr_image" {
  description = <<-EOT
    Full GHCR image URI to deploy, including tag.
    e.g. "ghcr.io/my-org/cloudtrail-to-parquet:sha-abc1234"
    or   "ghcr.io/my-org/cloudtrail-to-parquet:latest"
  EOT
  type        = string
}

# ── Application config ─────────────────────────────────────────────────────────

variable "cloudtrail_base_path" {
  description = "S3 URI of the CloudTrail bucket prefix, must end with /"
  type        = string
}

variable "output_path" {
  description = "S3 URI where Parquet output is written, must end with /"
  type        = string
}

variable "organisation_id" {
  description = "AWS Organizations ID prefix (leave empty if none)"
  type        = string
  default     = ""
}

variable "process_date" {
  description = "Optional fixed date (YYYY-MM-DD) to set as PROCESS_DATE env var. Leave empty for normal 'yesterday' behaviour."
  type        = string
  default     = ""
}

# ── Lambda tuning ──────────────────────────────────────────────────────────────

variable "cron_schedule" {
  description = "EventBridge cron expression (UTC)"
  type        = string
  default     = "cron(0 1 * * ? *)"
}

variable "lambda_timeout_seconds" {
  type    = number
  default = 900
}

variable "lambda_memory_mb" {
  type    = number
  default = 1024
}
