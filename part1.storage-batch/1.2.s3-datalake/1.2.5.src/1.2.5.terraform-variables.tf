variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "bigdata-pipeline"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-2"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default = {
    Project     = "BigData Pipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}
