#!/bin/bash
# 데이터 품질 모니터링 Lambda 함수 배포 스크립트

set -e

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "[1/3] Lambda 함수 패키징"
zip quality-monitor.zip data_quality_monitor.py

echo "[2/3] Lambda 함수 생성"
aws lambda create-function \
    --function-name data-quality-monitor \
    --runtime python3.9 \
    --role arn:aws:iam::$ACCOUNT_ID:role/lambda-etl-role \
    --handler data_quality_monitor.lambda_handler \
    --zip-file fileb://quality-monitor.zip \
    --timeout 300 \
    --memory-size 256

echo "[3/3] CloudWatch 메트릭 권한 추가"
aws iam put-role-policy \
    --role-name lambda-etl-role \
    --policy-name cloudwatch-policy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData",
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }]
    }'

echo "데이터 품질 모니터링 Lambda 함수 생성 완료: data-quality-monitor"
