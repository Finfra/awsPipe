terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "lab_stream" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_hours

  shard_level_metrics = [
    "IncomingRecords",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded"
  ]

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.kinesis_key.id

  tags = {
    Environment = var.environment
    Project     = var.project_name
    Owner       = var.owner
  }
}

# KMS Key for encryption
resource "aws_kms_key" "kinesis_key" {
  description             = "KMS key for Kinesis encryption"
  deletion_window_in_days = 7

  tags = {
    Name        = "${var.project_name}-kinesis-key"
    Environment = var.environment
  }
}

resource "aws_kms_alias" "kinesis_key_alias" {
  name          = "alias/${var.project_name}-kinesis"
  target_key_id = aws_kms_key.kinesis_key.key_id
}

# CloudWatch 로그 그룹
resource "aws_cloudwatch_log_group" "kinesis_logs" {
  name              = "/aws/kinesis/${var.stream_name}"
  retention_in_days = 7

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# SNS Topic for alerts
resource "aws_sns_topic" "kinesis_alerts" {
  name = "${var.project_name}-kinesis-alerts"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# CloudWatch Alarms
resource "aws_cloudwatch_metric_alarm" "write_throttling" {
  alarm_name          = "${var.stream_name}-WriteThrottling"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "WriteProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors kinesis write throttling"
  alarm_actions       = [aws_sns_topic.kinesis_alerts.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.lab_stream.name
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_cloudwatch_metric_alarm" "read_throttling" {
  alarm_name          = "${var.stream_name}-ReadThrottling"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "ReadProvisionedThroughputExceeded"
  namespace           = "AWS/Kinesis"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors kinesis read throttling"
  alarm_actions       = [aws_sns_topic.kinesis_alerts.arn]

  dimensions = {
    StreamName = aws_kinesis_stream.lab_stream.name
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Role for Kinesis access
resource "aws_iam_role" "kinesis_access_role" {
  name = "${var.project_name}-kinesis-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_iam_role_policy" "kinesis_access_policy" {
  name = "kinesis-access-policy"
  role = aws_iam_role.kinesis_access_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.lab_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.kinesis_key.arn
      }
    ]
  })
}
