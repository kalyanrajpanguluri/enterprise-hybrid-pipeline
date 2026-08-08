# Generate random string for unique resource naming
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# -----------------------------------------------------------------------------
# S3 Ingestion Bucket (Raw, Silver, Gold Medallion Storage)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "ingestion" {
  bucket        = "${var.project_name}-ingest-${var.environment}-${random_string.suffix.result}"
  force_destroy = true
}

# Placeholder Medallion Folders in S3
resource "aws_s3_object" "raw_folder" {
  bucket = aws_s3_bucket.ingestion.id
  key    = "raw/"
}

resource "aws_s3_object" "silver_folder" {
  bucket = aws_s3_bucket.ingestion.id
  key    = "silver/"
}

resource "aws_s3_object" "gold_folder" {
  bucket = aws_s3_bucket.ingestion.id
  key    = "gold/"
}

resource "aws_s3_bucket_public_access_block" "ingestion" {
  bucket = aws_s3_bucket.ingestion.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "ingestion" {
  bucket = aws_s3_bucket.ingestion.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_notification" "ingestion_eventbridge" {
  bucket      = aws_s3_bucket.ingestion.id
  eventbridge = true
}

# -----------------------------------------------------------------------------
# S3 Script & Assets Bucket (Stores PySpark Glue job script & Lookups)
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "script" {
  bucket        = "${var.project_name}-assets-${var.environment}-${random_string.suffix.result}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "script" {
  bucket = aws_s3_bucket.script.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload PySpark script to S3 Script Bucket
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.script.id
  key    = "scripts/glue_job.py"
  source = "${path.module}/src/glue_job.py"
  etag   = filemd5("${path.module}/src/glue_job.py")
}

# Upload Products Catalog lookup dataset to S3 Script Bucket
resource "aws_s3_object" "products_catalog" {
  bucket = aws_s3_bucket.script.id
  key    = "data/products.csv"
  source = "${path.module}/sample_data/products.csv"
  etag   = filemd5("${path.module}/sample_data/products.csv")
}

# -----------------------------------------------------------------------------
# AWS Lambda Validator Function
# -----------------------------------------------------------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/src/lambda_validator.py"
  output_path = "${path.module}/lambda_validator.zip"
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role-${random_string.suffix.result}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-lambda-policy-${random_string.suffix.result}"
  description = "Permissions for Lambda validator function"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.ingestion.arn,
          "${aws_s3_bucket.ingestion.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

resource "aws_lambda_function" "validator" {
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  function_name    = "${var.project_name}-validator-${random_string.suffix.result}"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_validator.lambda_handler"
  runtime          = "python3.11"
  timeout          = 30
}

# -----------------------------------------------------------------------------
# IAM Role & Policies for AWS Glue ETL Job
# -----------------------------------------------------------------------------
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-role-${random_string.suffix.result}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "glue_custom_policy" {
  name        = "${var.project_name}-policy-${random_string.suffix.result}"
  description = "Permissions for Glue job to access S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          aws_s3_bucket.ingestion.arn,
          "${aws_s3_bucket.ingestion.arn}/*",
          aws_s3_bucket.script.arn,
          "${aws_s3_bucket.script.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_custom_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_custom_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_service_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# -----------------------------------------------------------------------------
# AWS Glue Job Definition
# -----------------------------------------------------------------------------
resource "aws_glue_job" "pipeline_processor" {
  name              = "${var.project_name}-job-${random_string.suffix.result}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = 10

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.script.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--S3_INPUT_PATH"                    = "s3://${aws_s3_bucket.ingestion.bucket}/"
    "--S3_SILVER_PATH"                   = "s3://${aws_s3_bucket.ingestion.bucket}/silver/sales_enriched/"
    "--S3_GOLD_PATH"                     = "s3://${aws_s3_bucket.ingestion.bucket}/gold/customer_summary/"
    "--S3_PRODUCTS_CATALOG_PATH"         = "s3://${aws_s3_bucket.script.bucket}/${aws_s3_object.products_catalog.key}"
  }

  depends_on = [
    aws_s3_object.glue_script,
    aws_s3_object.products_catalog,
    aws_iam_role_policy_attachment.glue_custom_attach
  ]
}

# -----------------------------------------------------------------------------
# AWS Glue Workflow & Trigger (Required for EventBridge target integration)
# -----------------------------------------------------------------------------
resource "aws_glue_workflow" "pipeline_workflow" {
  name = "${var.project_name}-workflow-${random_string.suffix.result}"
}

resource "aws_glue_trigger" "workflow_trigger" {
  name          = "${var.project_name}-trigger-${random_string.suffix.result}"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.pipeline_workflow.name

  actions {
    job_name = aws_glue_job.pipeline_processor.name
  }
}

# -----------------------------------------------------------------------------
# Amazon EventBridge Orchestration Trigger
# -----------------------------------------------------------------------------
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}-eb-role-${random_string.suffix.result}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "eventbridge_policy" {
  name        = "${var.project_name}-eb-policy-${random_string.suffix.result}"
  description = "Allows EventBridge to trigger AWS Glue workflow"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:StartWorkflowRun"
        Resource = aws_glue_workflow.pipeline_workflow.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_attach" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_policy.arn
}

resource "aws_cloudwatch_event_rule" "s3_upload" {
  name        = "${var.project_name}-s3-trigger-${random_string.suffix.result}"
  description = "Triggers AWS Glue Workflow when a CSV or JSON file is uploaded to S3 ingestion bucket"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.ingestion.bucket]
      }
      object = {
        key = [{
          "anything-but" = {
            prefix = "silver/"
          }
        }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "trigger_glue" {
  rule      = aws_cloudwatch_event_rule.s3_upload.name
  target_id = "TriggerAWSGlueWorkflow"
  arn       = aws_glue_workflow.pipeline_workflow.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}
