terraform {
  required_version = ">= 1.0"
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

# Kinesis Analytics 애플리케이션 (SQL 기반)
resource "aws_kinesisanalyticsv2_application" "realtime_analytics" {
  name                   = "${var.project_name}-realtime-analytics"
  runtime_environment    = "SQL-1_0"
  service_execution_role = aws_iam_role.analytics_execution_role.arn

  application_configuration {
    # SQL 코드 설정
    sql_application_configuration {
      input {
        name_prefix = "exampleNamePrefix"
        
        input_parallelism {
          count = 1
        }
        
        input_schema {
          record_column {
            name     = "timestamp"
            sql_type = "VARCHAR(32)"
            mapping  = "$.timestamp"
          }
          
          record_column {
            name     = "user_id"
            sql_type = "INTEGER"
            mapping  = "$.user_id"
          }
          
          record_column {
            name     = "event_type"
            sql_type = "VARCHAR(32)"
            mapping  = "$.event_type"
          }
          
          record_column {
            name     = "value"
            sql_type = "DOUBLE"
            mapping  = "$.value"
          }
          
          record_column {
            name     = "session_id"
            sql_type = "VARCHAR(64)"
            mapping  = "$.session_id"
          }
          
          record_format {
            record_format_type = "JSON"
            mapping_parameters {
              json_mapping_parameters {
                record_row_path = "$"
              }
            }
          }
        }
        
        kinesis_streams_input {
          resource_arn = var.kinesis_stream_arn
        }
      }

      output {
        name = "DESTINATION_SQL_STREAM"
        
        destination_schema {
          record_format_type = "JSON"
        }
        
        kinesis_streams_output {
          resource_arn = aws_kinesis_stream.analytics_output.arn
        }
      }
    }

    # 애플리케이션 코드 (SQL 쿼리)
    application_code_configuration {
      code_content_type = "PLAINTEXT"
      code_content {
        text_content = <<EOF
-- 실시간 사용자 활동 분석
CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    user_id INTEGER,
    event_count BIGINT,
    unique_sessions BIGINT,
    total_value DOUBLE,
    avg_value DOUBLE
);

-- 5분 윈도우로 사용자별 집계
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_SQL_STREAM"
SELECT STREAM
    ROWTIME_TO_TIMESTAMP(ROWTIME) AS window_start,
    ROWTIME_TO_TIMESTAMP(ROWTIME + INTERVAL '5' MINUTE) AS window_end,
    user_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS unique_sessions,
    SUM(value) AS total_value,
    AVG(value) AS avg_value
FROM SOURCE_SQL_STREAM_001
WHERE user_id IS NOT NULL
GROUP BY user_id, 
         RANGE_TIMESTAMP(ROWTIME RANGE INTERVAL '5' MINUTE);
EOF
      }
    }
  }

  tags = var.tags
}

# Analytics 출력용 Kinesis 스트림
resource "aws_kinesis_stream" "analytics_output" {
  name        = "${var.project_name}-analytics-output"
  shard_count = 1

  retention_period = 24
  
  encryption_type = "KMS"
  kms_key_id     = var.kms_key_arn

  tags = var.tags
}

# Kinesis Analytics 실행 역할
resource "aws_iam_role" "analytics_execution_role" {
  name = "${var.project_name}-analytics-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Analytics 권한 정책
resource "aws_iam_role_policy" "analytics_kinesis_policy" {
  name = "analytics-kinesis-access"
  role = aws_iam_role.analytics_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = var.kinesis_stream_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ]
        Resource = aws_kinesis_stream.analytics_output.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = var.kms_key_arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

# CloudWatch 로그 그룹 (Analytics 로그용)
resource "aws_cloudwatch_log_group" "analytics_logs" {
  name              = "/aws/kinesisanalytics/${var.project_name}"
  retention_in_days = 7

  tags = var.tags
}

# Analytics 성능 모니터링 알람
resource "aws_cloudwatch_metric_alarm" "analytics_millis_behind_latest" {
  alarm_name          = "${var.project_name}-analytics-lag"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "millisBehindLatest"
  namespace           = "AWS/KinesisAnalytics"
  period              = "300"
  statistic           = "Average"
  threshold           = "60000"  # 1분 지연
  alarm_description   = "Analytics application is lagging behind"
  alarm_actions       = [var.sns_topic_arn]

  dimensions = {
    Application = aws_kinesisanalyticsv2_application.realtime_analytics.name
  }

  tags = var.tags
}

# Lambda 함수 (Analytics 결과 처리용)
resource "aws_lambda_function" "analytics_processor" {
  filename         = "analytics_processor.zip"
  function_name    = "${var.project_name}-analytics-processor"
  role            = aws_iam_role.lambda_analytics_role.arn
  handler         = "index.handler"
  runtime         = "python3.9"
  timeout         = 300

  environment {
    variables = {
      OUTPUT_STREAM = aws_kinesis_stream.analytics_output.name
      S3_BUCKET     = var.results_s3_bucket
      SNS_TOPIC_ARN = var.sns_topic_arn
    }
  }

  tags = var.tags
}

# Lambda 이벤트 소스 매핑 (Kinesis → Lambda)
resource "aws_lambda_event_source_mapping" "analytics_trigger" {
  event_source_arn  = aws_kinesis_stream.analytics_output.arn
  function_name     = aws_lambda_function.analytics_processor.arn
  starting_position = "LATEST"
  batch_size        = 100
  
  # 에러 처리 설정
  maximum_batching_window_in_seconds = 5
  parallelization_factor = 2
  
  # DLQ 설정
  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.analytics_dlq.arn
    }
  }
}

# Lambda 실행 역할
resource "aws_iam_role" "lambda_analytics_role" {
  name = "${var.project_name}-lambda-analytics-role"

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

  tags = var.tags
}

# Lambda 권한 정책
resource "aws_iam_role_policy" "lambda_analytics_policy" {
  name = "lambda-analytics-policy"
  role = aws_iam_role.lambda_analytics_role.id

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
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.analytics_output.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ]
        Resource = "${var.results_s3_bucket_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = var.sns_topic_arn
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.analytics_dlq.arn
      }
    ]
  })
}

# DLQ (Dead Letter Queue)
resource "aws_sqs_queue" "analytics_dlq" {
  name = "${var.project_name}-analytics-dlq"
  
  message_retention_seconds = 1209600  # 14일
  
  tags = var.tags
}

# Lambda 함수 코드
data "archive_file" "analytics_processor_zip" {
  type        = "zip"
  output_path = "analytics_processor.zip"
  
  source {
    content = <<EOF
import json
import boto3
import base64
import os
from datetime import datetime

def handler(event, context):
    """Analytics 결과 처리 함수"""
    
    s3 = boto3.client('s3')
    sns = boto3.client('sns')
    
    bucket = os.environ['S3_BUCKET']
    sns_topic = os.environ['SNS_TOPIC_ARN']
    
    processed_records = []
    
    for record in event['Records']:
        try:
            # Kinesis 레코드 디코딩
            payload = json.loads(base64.b64decode(record['kinesis']['data']))
            
            print(f"처리 중인 레코드: {payload}")
            
            # 이상 값 감지
            if 'anomaly_score' in payload and abs(payload['anomaly_score']) > 2:
                # 이상 값 알림
                alert_message = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'alert_type': 'ANOMALY_DETECTED',
                    'user_id': payload.get('user_id'),
                    'anomaly_score': payload.get('anomaly_score'),
                    'value': payload.get('value'),
                    'event_type': payload.get('event_type')
                }
                
                sns.publish(
                    TopicArn=sns_topic,
                    Message=json.dumps(alert_message),
                    Subject='이상 값 감지 알림'
                )
                
                print(f"이상 값 알림 전송: 사용자 {payload.get('user_id')}")
            
            # S3에 결과 저장
            timestamp = datetime.utcnow()
            s3_key = f"analytics-results/{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/{timestamp.hour:02d}/{record['kinesis']['sequenceNumber']}.json"
            
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=json.dumps(payload, default=str),
                ContentType='application/json'
            )
            
            processed_records.append({
                'record_id': record['kinesis']['sequenceNumber'],
                's3_location': f"s3://{bucket}/{s3_key}",
                'status': 'SUCCESS'
            })
            
        except Exception as e:
            print(f"레코드 처리 오류: {e}")
            processed_records.append({
                'record_id': record['kinesis']['sequenceNumber'],
                'status': 'ERROR',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': {
            'processed_count': len(processed_records),
            'records': processed_records
        }
    }
EOF
    filename = "index.py"
  }
}

# CloudWatch 대시보드 (Analytics 모니터링)
resource "aws_cloudwatch_dashboard" "analytics_dashboard" {
  dashboard_name = "${var.project_name}-analytics-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/KinesisAnalytics", "InputRecords", "Application", aws_kinesisanalyticsv2_application.realtime_analytics.name],
            [".", "OutputRecords", ".", "."],
            [".", "KPUs", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          title   = "Analytics Application Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.analytics_processor.function_name],
            [".", "Errors", ".", "."],
            [".", "Invocations", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.region
          title   = "Lambda Processor Metrics"
          period  = 300
        }
      }
    ]
  })
}
