#!/bin/bash
# 리소스 정리 스크립트

BUCKET_NAME=$(cat ~/BUCKET_NAME)

if [ -z "$BUCKET_NAME" ]; then
    echo "버킷 이름을 찾을 수 없습니다."
    exit 1
fi

echo "버킷 정리 중: $BUCKET_NAME"

# 버킷 내용 삭제
aws s3 rm s3://$BUCKET_NAME --recursive

# 버킷 삭제
aws s3 rb s3://$BUCKET_NAME

# 임시 파일 정리
rm -f ~/BUCKET_NAME

echo "정리 완료"
