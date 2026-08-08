variable "aws_region" {
  description = "AWS region for provisioning resources"
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Project identifier for resource naming"
  type        = string
  default     = "enterprise-hybrid"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "glue_version" {
  description = "AWS Glue engine version"
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker node type (G.1X, G.2X, Flex)"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of worker nodes assigned to the Glue job"
  type        = number
  default     = 2
}
