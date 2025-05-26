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
  default     = "us-west-2"
}

variable "ec2_key_name" {
  description = "EC2 Key Pair name for EMR cluster access"
  type        = string
  default     = "my-emr-key"
}

variable "emr_release_label" {
  description = "EMR release version"
  type        = string
  default     = "emr-6.15.0"
}

variable "master_instance_type" {
  description = "EC2 instance type for EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_type" {
  description = "EC2 instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "core_instance_count" {
  description = "Number of EMR core instances"
  type        = number
  default     = 2
}

variable "ebs_volume_size" {
  description = "EBS volume size for EMR instances (GB)"
  type        = number
  default     = 32
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data and logs (from 1.2 lab)"
  type        = string
  default     = ""
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
