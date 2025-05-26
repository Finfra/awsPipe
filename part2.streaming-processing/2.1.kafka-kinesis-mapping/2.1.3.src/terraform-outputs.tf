output "main_stream_name" {
  description = "Name of the main Kinesis stream"
  value       = aws_kinesis_stream.main_stream.name
}

output "main_stream_arn" {
  description = "ARN of the main Kinesis stream"
  value       = aws_kinesis_stream.main_stream.arn
}

output "log_stream_name" {
  description = "Name of the log Kinesis stream"
  value       = aws_kinesis_stream.log_stream.name
}

output "log_stream_arn" {
  description = "ARN of the log Kinesis stream"
  value       = aws_kinesis_stream.log_stream.arn
}

output "firehose_delivery_stream_name" {
  description = "Name of the Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.s3_delivery.name
}

output "firehose_log_delivery_stream_name" {
  description = "Name of the log Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.log_delivery.name
}

output "kms_key_id" {
  description = "KMS key ID for Kinesis encryption"
  value       = aws_kms_key.kinesis_key.key_id
}

output "kms_key_arn" {
  description = "KMS key ARN for Kinesis encryption"
  value       = aws_kms_key.kinesis_key.arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for alerts"
  value       = aws_sns_topic.kinesis_alerts.arn
}

output "lambda_function_name" {
  description = "Lambda function name for shard scaling"
  value       = aws_lambda_function.shard_scaler.function_name
}

output "analytics_application_name" {
  description = "Kinesis Analytics application name"
  value       = aws_kinesisanalyticsv2_application.realtime_analytics.name
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name for Firehose"
  value       = aws_cloudwatch_log_group.firehose_logs.name
}

# 연결 정보
output "kinesis_endpoint" {
  description = "Kinesis service endpoint"
  value       = "https://kinesis.${var.region}.amazonaws.com"
}

output "firehose_endpoint" {
  description = "Firehose service endpoint"
  value       = "https://firehose.${var.region}.amazonaws.com"
}

# 사용 예시
output "producer_example" {
  description = "Example command to send data to Kinesis"
  value = <<EOF
# AWS CLI로 데이터 전송
aws kinesis put-record \
  --stream-name ${aws_kinesis_stream.main_stream.name} \
  --partition-key "user123" \
  --data '{"user_id": 123, "event": "click", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'

# Python boto3로 데이터 전송
import boto3
import json
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='${var.region}')

response = kinesis.put_record(
    StreamName='${aws_kinesis_stream.main_stream.name}',
    Data=json.dumps({
        'user_id': 123,
        'event': 'click',
        'timestamp': datetime.utcnow().isoformat()
    }),
    PartitionKey='user123'
)
EOF
}

output "consumer_example" {
  description = "Example command to read data from Kinesis"
  value = <<EOF
# 샤드 목록 조회
aws kinesis list-shards --stream-name ${aws_kinesis_stream.main_stream.name}

# 샤드 이터레이터 생성 (최신 데이터부터)
aws kinesis get-shard-iterator \
  --stream-name ${aws_kinesis_stream.main_stream.name} \
  --shard-id shardId-000000000000 \
  --shard-iterator-type LATEST

# 레코드 읽기
aws kinesis get-records --shard-iterator <iterator-from-previous-command>
EOF
}

# 모니터링 대시보드 URL
output "cloudwatch_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=Kinesis-${aws_kinesis_stream.main_stream.name}"
}

# 비용 정보
output "estimated_monthly_cost" {
  description = "Estimated monthly cost breakdown"
  value = {
    main_stream_shards    = "$${var.shard_count * 0.015 * 24 * 30} (${var.shard_count} shards)"
    log_stream_shards     = "$${var.log_shard_count * 0.015 * 24 * 30} (${var.log_shard_count} shards)"
    put_requests          = "Variable based on usage ($0.014 per million PUT requests)"
    data_retrieval        = "Variable based on usage ($0.040 per million GET requests)"
    firehose_delivery     = "Variable based on usage ($0.029 per GB)"
    analytics_kpu         = "Variable based on usage ($0.11 per KPU hour)"
    total_fixed_cost      = "$${(var.shard_count + var.log_shard_count) * 0.015 * 24 * 30}"
  }
}
