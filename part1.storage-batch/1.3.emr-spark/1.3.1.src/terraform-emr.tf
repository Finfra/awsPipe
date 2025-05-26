# Terraform EMR 클러스터 구성
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "cluster_name" {
  description = "EMR cluster name"
  type        = string
  default     = "spark-cluster"
}

variable "key_name" {
  description = "EC2 Key Pair name"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for EMR cluster"
  type        = string
}

variable "log_bucket" {
  description = "S3 bucket for EMR logs"
  type        = string
}

# S3 Bucket for logs
resource "aws_s3_bucket" "emr_logs" {
  bucket = var.log_bucket
}

resource "aws_s3_bucket_versioning" "emr_logs_versioning" {
  bucket = aws_s3_bucket.emr_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

# EMR Cluster
resource "aws_emr_cluster" "spark_cluster" {
  name          = var.cluster_name
  applications  = ["Hadoop", "Spark", "Zeppelin"]
  
  termination_protection            = false
  keep_job_flow_alive_when_no_steps = false
  
  ec2_attributes {
    key_name                          = var.key_name
    subnet_id                         = var.subnet_id
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }

  master_instance_group {
    instance_type = "m5.xlarge"
    
    ebs_config {
      size = 32
      type = "gp3"
    }
  }

  core_instance_group {
    instance_count = 2
    instance_type  = "r5.2xlarge"
    
    ebs_config {
      size = 64
      type = "gp3"
    }
    
    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = 2
        MaxCapacity = 10
      }
      Rules = [
        {
          Name = "ScaleOutMemoryPercentage"
          Description = "Scale out if YARNMemoryAvailablePercentage is less than 15"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = 1
              CoolDown = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              ComparisonOperator = "LESS_THAN"
              EvaluationPeriods = 1
              MetricName = "YARNMemoryAvailablePercentage"
              Namespace = "AWS/ElasticMapReduce"
              Period = 300
              Statistic = "AVERAGE"
              Threshold = 15.0
              Unit = "PERCENT"
            }
          }
        }
      ]
    })
  }

  service_role = aws_iam_role.emr_service_role.arn
  
  log_uri = "s3://${aws_s3_bucket.emr_logs.bucket}/logs/"
  
  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.adaptive.enabled" = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.sql.adaptive.skewJoin.enabled" = "true"
        "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
      }
    }
  ])

  tags = {
    Name        = var.cluster_name
    Environment = "Development"
    Project     = "BigDataPipeline"
  }
}

# Security Groups
resource "aws_security_group" "emr_master" {
  name_prefix = "emr-master-"
  
  ingress {
    from_port = 22
    to_port   = 22
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  ingress {
    from_port = 8080
    to_port   = 8080
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "emr_slave" {
  name_prefix = "emr-slave-"
  
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    security_groups = [aws_security_group.emr_master.id]
  }
  
  egress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# IAM Roles
resource "aws_iam_role" "emr_service_role" {
  name = "EMR_DefaultRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_instance_role" {
  name = "EMR_EC2_DefaultRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_instance_role_policy" {
  role       = aws_iam_role.emr_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_profile" {
  name = "EMR_EC2_DefaultRole"
  role = aws_iam_role.emr_instance_role.name
}

# Outputs
output "cluster_id" {
  description = "EMR cluster ID"
  value       = aws_emr_cluster.spark_cluster.id
}

output "cluster_name" {
  description = "EMR cluster name"
  value       = aws_emr_cluster.spark_cluster.name
}

output "master_public_dns" {
  description = "EMR master node public DNS"
  value       = aws_emr_cluster.spark_cluster.master_public_dns
}
