#!/bin/bash
# S3 및 CloudWatch 결과 확인 스크립트

set -e

BUCKET_NAME=$1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$BUCKET_NAME" ]; then
  echo "사용법: $0 <bucket-name>"
  exit 1
fi

echo "[1/3] 변환된 데이터 확인:"
aws s3 ls s3://$BUCKET_NAME/transformed/ --recursive

echo "[2/3] 오류 데이터 확인:"
aws s3 ls s3://$BUCKET_NAME/errors/ --recursive

echo "[3/3] Lambda 및 Firehose CloudWatch 메트릭 확인:"
aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Invocations \
    --dimensions Name=FunctionName,Value=etl-transform \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum

aws cloudwatch get-metric-statistics \
    --namespace AWS/KinesisFirehose \
    --metric-name DeliveryToS3.Records \
    --dimensions Name=DeliveryStreamName,Value=etl-demo-stream \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum
