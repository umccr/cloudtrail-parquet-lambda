terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # kreuzwerker/docker lets Terraform pull from GHCR and push to ECR
    # using the deployer's supplied credentials — no local Docker daemon config needed.
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

# ── Providers ─────────────────────────────────────────────────────────────────

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_ecr_authorization_token" "this" {}

locals {
  account_id   = data.aws_caller_identity.current.account_id
  ecr_registry = "${local.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
  ecr_repo_url = "${local.ecr_registry}/${var.name}"

  # Derive a stable ECR tag from the GHCR tag portion after the colon.
  # e.g. "ghcr.io/my-org/my-repo:sha-abc1234"  →  "sha-abc1234"
  image_tag    = split(":", var.ghcr_image)[1]
  ecr_image_uri = "${local.ecr_repo_url}:${local.image_tag}"
}

provider "docker" {
  # Push to ECR using the deployer's AWS credentials (token fetched above)
  registry_auth {
    address  = local.ecr_registry
    username = "AWS"
    password = data.aws_ecr_authorization_token.this.password
  }
}

# ── ECR repository ────────────────────────────────────────────────────────────

resource "aws_ecr_repository" "this" {
  name                 = var.name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "this" {
  repository = aws_ecr_repository.this.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# ── Mirror GHCR → ECR ─────────────────────────────────────────────────────────
# The docker provider pulls the image from GHCR and pushes it to ECR in one
# resource. No local docker daemon or shell scripts needed.

resource "docker_image" "source" {
  name          = var.ghcr_image
  platform = "linux/arm64"
  keep_locally  = false   # don't cache in local daemon — we're just mirroring
}

resource "docker_tag" "ecr" {
  source_image = docker_image.source.image_id
  target_image = local.ecr_image_uri
}

resource "docker_registry_image" "ecr" {
  name          = docker_tag.ecr.target_image
  keep_remotely = true    # don't delete from ECR when this resource is destroyed

  depends_on = [
    aws_ecr_repository.this,
    docker_tag.ecr,
  ]
}

# ── IAM ───────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_s3" {
  statement {
    sid     = "ReadCloudTrailLogs"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      "*"
    ]
  }

  statement {
    sid     = "WriteParquetOutput"
    actions = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [
      "*",
    ]
  }
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "s3-access"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_s3.json
}

# ── Lambda ────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = 30
}

resource "aws_lambda_function" "this" {
  function_name = var.name
  role          = aws_iam_role.lambda.arn
  package_type  = "Image"
  image_uri     = local.ecr_image_uri
  architectures = ["arm64"]

  timeout     = var.lambda_timeout_seconds
  memory_size = var.lambda_memory_mb

  environment {
    variables = {
      CLOUDTRAIL_BASE_PATH = var.cloudtrail_base_path
      OUTPUT_PATH          = var.output_path
      ORGANISATION_ID      = var.organisation_id
      PROCESS_DATE         = var.process_date
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.basic_execution,
    aws_cloudwatch_log_group.lambda,
    docker_registry_image.ecr,   # ensure image is in ECR before Lambda is created/updated
  ]
}

# ── EventBridge cron ──────────────────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.name}-daily"
  description         = "Trigger ${var.name} Lambda daily"
  schedule_expression = var.cron_schedule
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "${var.name}-lambda"
  arn       = aws_lambda_function.this.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}
