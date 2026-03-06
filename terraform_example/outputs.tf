output "ecr_repository_url" {
  description = "ECR repository URL (the mirrored image lives here)"
  value       = aws_ecr_repository.this.repository_url
}

output "ecr_image_uri" {
  description = "Full ECR image URI that was deployed to Lambda"
  value       = local.ecr_image_uri
}

output "lambda_function_name" {
  value = aws_lambda_function.this.function_name
}

output "lambda_function_arn" {
  value = aws_lambda_function.this.arn
}
