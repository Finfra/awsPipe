#!/bin/bash

# EMR Studio 생성 오류 해결 스크립트
# 사용법: ./emr-studio.sh

set -e

echo "=== EMR Studio 생성 오류 해결 스크립트 ==="
echo "현재 시간: $(date)"
echo

# 환경 변수 확인
if [ ! -f ~/BUCKET_NAME ]; then
    echo "❌ 오류: ~/BUCKET_NAME 파일이 존재하지 않습니다."
    exit 1
fi

BUCKET_NAME=$(cat ~/BUCKET_NAME)
PROJECT_NAME="bigdata-pipeline"

echo "📋 설정 정보:"
echo "  - S3 버킷: $BUCKET_NAME"
echo "  - 프로젝트: $PROJECT_NAME"
echo

# 1. 기존 리소스 정리
echo "🧹 1단계: 기존 리소스 정리"
echo "기존 EMR Studio 리소스를 정리합니다..."

terraform destroy \
    -var="s3_bucket_name=$BUCKET_NAME" \
    -var="project_name=$PROJECT_NAME" \
    -auto-approve || echo "⚠️  기존 리소스가 없거나 정리 중 오류 발생 (계속 진행)"

echo "✅ 기존 리소스 정리 완료"
echo

# 2. 기존 파일 백업
echo "💾 2단계: 기존 설정 파일 백업"
if [ -f emr-studio.tf ]; then
    cp emr-studio.tf emr-studio.tf.backup.$(date +%Y%m%d_%H%M%S)
    echo "✅ 기존 파일 백업 완료"
else
    echo "ℹ️  기존 파일이 없습니다."
fi
echo

echo "✅ 새로운 설정 파일 적용 완료"
echo

# 4. Terraform 초기화
echo "🚀 4단계: Terraform 초기화"
terraform init
echo "✅ Terraform 초기화 완료"
echo

# 5. Terraform 계획 확인
echo "📋 5단계: Terraform 계획 확인"
terraform plan \
    -var="s3_bucket_name=$BUCKET_NAME" \
    -var="project_name=$PROJECT_NAME"
echo

# 6. 사용자 확인
echo "❓ 위의 계획을 확인하셨나요? 계속 진행하시겠습니까? (y/N)"
read -r response
if [[ ! "$response" =~ ^[Yy]$ ]]; then
    echo "❌ 사용자가 취소했습니다."
    exit 1
fi

# 7. Terraform 적용
echo "🚀 6단계: Terraform 적용"
terraform apply \
    -var="s3_bucket_name=$BUCKET_NAME" \
    -var="project_name=$PROJECT_NAME" \
    -auto-approve

echo
echo "🎉 EMR Studio 생성 완료!"
echo

# 8. 결과 확인
echo "📊 7단계: 결과 확인"
echo "EMR Studio 정보:"
terraform output

echo
echo "S3 버킷 정책 확인:"
aws s3api get-bucket-policy --bucket "$BUCKET_NAME" --output table || echo "⚠️  버킷 정책 확인 실패"

echo
echo "EMR Studio 목록:"
aws emr list-studios --output table

echo
echo "✅ 모든 단계가 완료되었습니다!"
echo "🔗 EMR Studio URL을 확인하여 접속해보세요."
