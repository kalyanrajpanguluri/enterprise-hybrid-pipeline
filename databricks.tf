# -----------------------------------------------------------------------------
# Databricks Integration Configuration (Optional / Hybrid Compute)
# -----------------------------------------------------------------------------

# Databricks workspace credentials (Set via environment variables or terraform.tfvars)
variable "databricks_host" {
  description = "Databricks Workspace URL (e.g. https://community.cloud.databricks.com)"
  type        = string
  default     = ""
}

variable "databricks_token" {
  description = "Databricks Personal Access Token (dapi...)"
  type        = string
  default     = ""
  sensitive   = true
}

# Example Databricks Notebook / Script upload configuration
resource "aws_s3_object" "databricks_script" {
  bucket = aws_s3_bucket.script.id
  key    = "scripts/databricks_delta_job.py"
  source = "${path.module}/src/databricks_delta_job.py"
  etag   = filemd5("${path.module}/src/databricks_delta_job.py")
}
