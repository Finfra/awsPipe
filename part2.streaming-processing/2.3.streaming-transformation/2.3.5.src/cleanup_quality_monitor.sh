#!/bin/bash
# 품질 모니터링 리소스 정리 스크립트

set -e

STREAM_NAME=${1:-"log-streaming-demo"}

echo "[1/5] Lambda 이벤트 소스 매핑 삭제"
# 이벤트 소스 매핑 UUID 조회 및 삭제
MAPPING_UUID=$(aws lambda list-event-source-mappings \
    --function-name data-quality-monitor \
    --query 'EventSourceMappings[0].UUID' \
    --output text)

if [ "$MAPPING_UUID" != "None" ] && [ "$MAPPING_UUID" != "" ]; then
    aws lambda delete-event-source-mapping --uuid $MAPPING_UUID
    echo "이벤트 소스 매핑 삭제 완료: $MAPPING_UUID"
else
    echo "삭제할 이벤트 소스 매핑 없음"
fi

echo "[2/5] Lambda 함수 삭제"
aws lambda delete-function --function-name data-quality-monitor || echo "Lambda 함수 이미 삭제됨"

echo "[3/5] CloudWatch 알람 삭제"
aws cloudwatch delete-alarms \
    --alarm-names "High-Invalid-Data" "Low-Data-Quality-Rate" || echo "일부 알람이 존재하지 않음"

echo "[4/5] CloudWatch 대시보드 삭제"
aws cloudwatch delete-dashboards \
    --dashboard-names "DataQualityDashboard" || echo "대시보드가 존재하지 않음"

echo "[5/5] IAM 정책 정리"
aws iam delete-role-policy \
    --role-name lambda-etl-role \
    --policy-name cloudwatch-policy || echo "CloudWatch 정책이 존재하지 않음"

echo "품질 모니터링 리소스 정리 완료"
echo "참고: Kinesis 스트림 '$STREAM_NAME'은 유지됩니다."
