#!/bin/bash
# Lambda 함수 배포 스크립트

set -e

# 1. Lambda 함수 패키징
zip etl-lambda.zip etl_transform.py

echo "[1/3] Lambda 패키징 완료: etl-lambda.zip 생성"

# 2. IAM 역할 생성
aws iam create-role \
    --role-name lambda-etl-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

aws iam attach-role-policy \
    --role-name lambda-etl-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

echo "[2/3] Lambda IAM 역할 생성 및 정책 연결 완료"

# 3. Lambda 함수 생성
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/lambda-etl-role"

aws lambda create-function \
    --function-name etl-transform \
    --runtime python3.9 \
    --role $ROLE_ARN \
    --handler etl_transform.lambda_handler \
    --zip-file fileb://etl-lambda.zip \
    --timeout 300

echo "[3/3] Lambda 함수 생성 완료: etl-transform"
