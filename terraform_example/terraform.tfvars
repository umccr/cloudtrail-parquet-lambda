# terraform.tfvars.example
# ─────────────────────────────────────────────────────────────────────────────
# Copy to terraform.tfvars (git-ignored) and fill in your values.
# Sensitive values (ghcr_token, bucket paths) can also be supplied via env vars:
#   export TF_VAR_ghcr_token="ghp_..."
#   export TF_VAR_cloudtrail_base_path="s3://..."
#   export TF_VAR_output_path="s3://..."

name       = "cloudtrail-to-parquet"
aws_region = "ap-southeast-2"

# ── Image to deploy ───────────────────────────────────────────────────────────
ghcr_image    = "ghcr.io/umccr/cloudtrail-parquet-lambda:sha-949b397"

# ── Application ───────────────────────────────────────────────────────────────
cloudtrail_base_path = "s3://aws-cloudtrail-logs-843407916570-dcd05a9d/"
output_path          = "s3://aws-cloudtrail-logs-843407916570-dcd05a9d/test/"
organisation_id      = ""   # leave "" if no org-level CloudTrail

process_date = "2021-09-01"   # uncomment to pin a backfill date

# ── Lambda tuning ─────────────────────────────────────────────────────────────
cron_schedule          = "cron(0 1 * * ? *)"
lambda_timeout_seconds = 14*60
lambda_memory_mb       = 2048
