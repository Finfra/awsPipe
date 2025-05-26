#!/bin/bash
# 리소스 정리 스크립트

set -e

BUCKET_NAME=$1
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ -z "$BUCKET_NAME" ]; then
  echo "사용법: $0 <bucket-name>"
  exit 1
fi

echo "[1/5] Firehose 스트림 삭제"
aws firehose delete-delivery-stream --delivery-stream-name etl-demo-stream

echo "[2/5] Lambda 함수 삭제"
aws lambda delete-function --function-name etl-transform

echo "[3/5] S3 버킷 비우기 및 삭제"
aws s3 rm s3://$BUCKET_NAME --recursive
aws s3 rb s3://$BUCKET_NAME

echo "[4/5] IAM 역할 및 정책 삭제"
aws iam delete-role-policy --role-name firehose-etl-role --policy-name firehose-etl-policy || true
aws iam delete-role --role-name firehose-etl-role || true
aws iam detach-role-policy --role-name lambda-etl-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole || true
aws iam delete-role --role-name lambda-etl-role || true

echo "[5/5] 정리 완료"
