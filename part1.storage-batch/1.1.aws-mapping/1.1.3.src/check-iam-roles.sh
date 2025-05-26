#!/bin/bash

echo "=== AWS IAM 역할 및 환경 확인 ==="
echo

# 현재 사용자 정보 확인
echo "1.1. 현재 AWS 사용자 정보 확인"
aws sts get-caller-identity
echo

# EMR 관련 역할 확인
echo "1.2. EMR 관련 역할 확인"
echo "EMR Service Role 확인:"
aws iam get-role --role-name EMR_DefaultRole 2>/dev/null || echo "EMR_DefaultRole 없음"
echo

echo "EMR EC2 Instance Role 확인:"
aws iam get-role --role-name EMR_EC2_DefaultRole 2>/dev/null || echo "EMR_EC2_DefaultRole 없음"
echo

# 모든 EMR 관련 역할 리스트
echo "1.3. 모든 EMR 관련 역할 조회"
aws iam list-roles --query 'Roles[?contains(RoleName, `EMR`)].{RoleName:RoleName,CreateDate:CreateDate}' --output table
echo

# 현재 사용자의 권한 확인
echo "1.4. 현재 사용자 권한 확인"
echo "S3 접근 권한 테스트:"
aws s3 ls 2>/dev/null && echo "S3 접근 가능" || echo "S3 접근 불가"
echo

echo "EMR 접근 권한 테스트:"
aws emr list-clusters --max-items 1 2>/dev/null && echo "EMR 접근 가능" || echo "EMR 접근 불가"
echo

# AWS CLI 구성 확인
echo "1.5. AWS CLI 구성 확인"
echo "현재 리전: $(aws configure get region)"
echo "출력 형식: $(aws configure get output)"
echo

echo "=== 환경 확인 완료 ==="

# IAM 역할 확인 스크립트 (예시)

echo "IAM 역할 확인 중..."
aws iam list-roles --output table
