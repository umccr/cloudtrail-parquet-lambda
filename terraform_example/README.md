# cloudtrail-to-parquet — deploy

Standalone Terraform project that deploys the Lambda function from a pre-built
Docker image in GHCR. It has no knowledge of how the image was built — it just
mirrors the image into ECR and points the Lambda at it.

## How it works

```
ghcr.io/my-org/repo:sha-abc  ──(mirror)──▶  ECR repo  ──▶  Lambda
        ↑
  deployer's GHCR token
```

The `kreuzwerker/docker` Terraform provider handles the pull/push using the
credentials you supply. No local Docker daemon config or shell scripts needed.

## Prerequisites

- Terraform >= 1.5
- AWS credentials in the environment (`AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, etc.)
  with permissions to manage Lambda, ECR, IAM, EventBridge, and CloudWatch Logs
- A GitHub token with `read:packages` scope

## Usage

```bash
# 1. Copy and fill in variables
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars

# 2. Or supply sensitive values via env vars
export TF_VAR_ghcr_token="ghp_..."
export TF_VAR_cloudtrail_base_path="s3://my-bucket/"
export TF_VAR_output_path="s3://my-output-bucket/parquet/"

# 3. Deploy
terraform init
terraform apply

# 4. Deploy a specific image tag
terraform apply -var='ghcr_image=ghcr.io/my-org/cloudtrail-to-parquet:sha-abc1234'

# 5. Backfill a specific date (overrides the Lambda's "yesterday" default)
terraform apply -var='process_date=2024-01-15'
```

## Inputs

| Name | Description | Default |
|------|-------------|---------|
| `name` | Base name for all AWS resources | `cloudtrail-to-parquet` |
| `aws_region` | AWS region | `ap-southeast-2` |
| `ghcr_image` | Full GHCR image URI with tag | — |
| `ghcr_username` | GitHub username | — |
| `ghcr_token` | GitHub token with `read:packages` | — |
| `cloudtrail_base_path` | S3 URI for CloudTrail logs (trailing `/`) | — |
| `output_path` | S3 URI for Parquet output (trailing `/`) | — |
| `organisation_id` | AWS Org ID prefix, or `""` | `""` |
| `process_date` | Pinned date `YYYY-MM-DD` for backfills | `""` |
| `cron_schedule` | EventBridge cron expression | `cron(0 1 * * ? *)` |
| `lambda_timeout_seconds` | Lambda timeout | `900` |
| `lambda_memory_mb` | Lambda memory | `1024` |

## Outputs

| Name | Description |
|------|-------------|
| `ecr_repository_url` | ECR repo where mirrored images live |
| `ecr_image_uri` | Exact image URI deployed to Lambda |
| `lambda_function_name` | Lambda function name |
| `lambda_function_arn` | Lambda function ARN |
