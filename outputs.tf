output "s3_ingestion_bucket" {
  description = "Name of the S3 bucket receiving raw file uploads"
  value       = aws_s3_bucket.ingestion.bucket
}

output "s3_script_bucket" {
  description = "Name of the S3 bucket hosting the Glue script & lookups"
  value       = aws_s3_bucket.script.bucket
}

output "lambda_validator_function_name" {
  description = "Name of the AWS Lambda Validator function"
  value       = aws_lambda_function.validator.function_name
}

output "step_functions_state_machine_arn" {
  description = "ARN of the AWS Step Functions State Machine Orchestrator"
  value       = aws_sfn_state_machine.pipeline_orchestrator.arn
}

output "glue_job_name" {
  description = "Name of the AWS Glue ETL Job"
  value       = aws_glue_job.pipeline_processor.name
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge trigger rule"
  value       = aws_cloudwatch_event_rule.s3_upload.arn
}

output "s3_raw_path" {
  description = "S3 path for uploading raw input CSV/JSON files"
  value       = "s3://${aws_s3_bucket.ingestion.bucket}/raw/"
}

output "s3_silver_path" {
  description = "S3 path where cleaned, joined Parquet files are stored"
  value       = "s3://${aws_s3_bucket.ingestion.bucket}/silver/sales_enriched/"
}

output "s3_gold_path" {
  description = "S3 path where customer aggregate Parquet files are stored"
  value       = "s3://${aws_s3_bucket.ingestion.bucket}/gold/customer_summary/"
}
