#!/bin/bash
# Firehose, S3, IAM 설정 스크립트

set -e

BUCKET_NAME="etl-demo-$(date +%s)"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 1. S3 버킷 생성
echo "[1/4] S3 버킷 생성: $BUCKET_NAME"
aws s3 mb s3://$BUCKET_NAME

# 2. Firehose 전송 역할 생성
aws iam create-role \
    --role-name firehose-etl-role \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "firehose.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

# 3. S3 및 Lambda 권한 부여
aws iam put-role-policy \
    --role-name firehose-etl-role \
    --policy-name firehose-etl-policy \
    --policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::'$BUCKET_NAME'",
                    "arn:aws:s3:::'$BUCKET_NAME'/*"
                ]
            },
            {
                "Effect": "Allow",
                "Action": "lambda:InvokeFunction",
                "Resource": "arn:aws:lambda:*:*:function:etl-transform"
            }
        ]
    }'

echo "[2/4] Firehose IAM 역할 및 정책 생성 완료"

# 4. Firehose 스트림 생성
aws firehose create-delivery-stream \
    --delivery-stream-name etl-demo-stream \
    --extended-s3-destination-configuration '{
        "RoleARN": "arn:aws:iam::'$ACCOUNT_ID':role/firehose-etl-role",
        "BucketARN": "arn:aws:s3:::'$BUCKET_NAME'",
        "Prefix": "transformed/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
        "ErrorOutputPrefix": "errors/",
        "BufferingHints": {"SizeInMBs": 64, "IntervalInSeconds": 300},
        "CompressionFormat": "GZIP",
        "ProcessingConfiguration": {
            "Enabled": true,
            "Processors": [{
                "Type": "Lambda",
                "Parameters": [{
                    "ParameterName": "LambdaArn",
                    "ParameterValue": "arn:aws:lambda:us-east-1:'$ACCOUNT_ID':function:etl-transform"
                }]
            }]
        }
    }'

echo "[3/4] Firehose 스트림 생성 완료: etl-demo-stream"
echo "[4/4] S3 버킷: $BUCKET_NAME"
