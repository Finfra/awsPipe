variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "bigdata-streaming"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "shard_count" {
  description = "Number of shards for main stream"
  type        = number
  default     = 2
}

variable "log_shard_count" {
  description = "Number of shards for log stream"
  type        = number
  default     = 1
}

variable "stream_mode" {
  description = "Stream mode: PROVISIONED or ON_DEMAND"
  type        = string
  default     = "PROVISIONED"
  
  validation {
    condition     = contains(["PROVISIONED", "ON_DEMAND"], var.stream_mode)
    error_message = "Stream mode must be either PROVISIONED or ON_DEMAND."
  }
}

variable "retention_period_hours" {
  description = "Data retention period in hours"
  type        = number
  default     = 24
  
  validation {
    condition     = var.retention_period_hours >= 24 && var.retention_period_hours <= 8760
    error_message = "Retention period must be between 24 and 8760 hours."
  }
}

variable "min_shard_count" {
  description = "Minimum number of shards for auto scaling"
  type        = number
  default     = 1
}

variable "max_shard_count" {
  description = "Maximum number of shards for auto scaling"
  type        = number
  default     = 10
}

variable "s3_bucket_arn" {
  description = "S3 bucket ARN for Firehose delivery"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default = {
    Environment = "dev"
    Project     = "bigdata-streaming"
    Owner       = "data-team"
  }
}
