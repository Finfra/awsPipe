#!/bin/bash
# S3 버킷 생성 및 기본 설정

# 버킷 이름 설정
read -p "자기번호=" user_num
BUCKET_NAME="ligkn-$user_num"
echo $BUCKET_NAME > ~/BUCKET_NAME
echo "설정한 $BUCKET_NAME 입니다."

# 버킷 생성
aws s3 mb s3://$BUCKET_NAME --region ap-northeast-2

# 버전 관리 활성화
aws s3api put-bucket-versioning \
    --bucket $BUCKET_NAME \
    --versioning-configuration Status=Enabled

# 퍼블릭 액세스 차단
aws s3api put-public-access-block \
    --bucket $BUCKET_NAME \
    --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# 기본 폴더 구조 생성
aws s3api put-object --bucket $BUCKET_NAME --key raw-data/
aws s3api put-object --bucket $BUCKET_NAME --key processed-data/
aws s3api put-object --bucket $BUCKET_NAME --key archived-data/

echo "S3 버킷 생성 완료: $BUCKET_NAME"
