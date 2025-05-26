#!/bin/bash
# 라이프사이클 정책 적용 및 데이터 확인

BUCKET_NAME=$(cat ~/BUCKET_NAME)

# 라이프사이클 정책 적용
aws s3api put-bucket-lifecycle-configuration \
    --bucket $BUCKET_NAME \
    --lifecycle-configuration file://1.2.4_src/lifecycle-policy.json

echo "라이프사이클 정책 적용 완료"

# 버킷 구조 확인
echo "=== 버킷 구조 ==="
aws s3 ls s3://$BUCKET_NAME/ --recursive

# 특정 파티션 확인
echo "=== 파티션 데이터 ==="
aws s3 ls s3://$BUCKET_NAME/raw-data/year=2024/month=01/day=15/

# 샘플 데이터 확인
echo "=== 샘플 데이터 ==="
aws s3 cp s3://$BUCKET_NAME/raw-data/year=2024/month=01/day=15/data.jsonl - | head -3
