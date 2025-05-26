#!/bin/bash
# Kinesis 트리거 및 CloudWatch 설정 스크립트

set -e

STREAM_NAME=${1:-"log-streaming-demo"}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "[1/4] Lambda 트리거 연결: $STREAM_NAME"
aws lambda create-event-source-mapping \
    --function-name data-quality-monitor \
    --event-source-arn arn:aws:kinesis:us-east-1:$ACCOUNT_ID:stream/$STREAM_NAME \
    --starting-position LATEST \
    --batch-size 100 \
    --maximum-batching-window-in-seconds 5

echo "[2/4] CloudWatch 대시보드 생성"
cat > quality-dashboard.json << 'EOF'
{
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    ["DataQuality/Streaming", "TotalRecords", {"label": "총 레코드"}],
                    [".", "ValidRecords", {"label": "유효 레코드"}],
                    [".", "InvalidRecords", {"label": "무효 레코드"}]
                ],
                "period": 300,
                "stat": "Sum",
                "region": "us-east-1",
                "title": "데이터 품질 현황",
                "yAxis": {"left": {"min": 0}}
            }
        },
        {
            "type": "metric",
            "x": 12,
            "y": 0,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    ["DataQuality/Streaming", "DataQualityRate"]
                ],
                "period": 300,
                "stat": "Average",
                "region": "us-east-1",
                "title": "데이터 품질률 (%)",
                "yAxis": {"left": {"min": 0, "max": 100}}
            }
        }
    ]
}
EOF

aws cloudwatch put-dashboard \
    --dashboard-name "DataQualityDashboard" \
    --dashboard-body file://quality-dashboard.json

echo "[3/4] 품질 알람 설정"
# 무효 데이터 비율 알람
aws cloudwatch put-metric-alarm \
    --alarm-name "High-Invalid-Data" \
    --alarm-description "무효 데이터 50개 이상 감지" \
    --metric-name InvalidRecords \
    --namespace DataQuality/Streaming \
    --statistic Sum \
    --period 300 \
    --threshold 50 \
    --comparison-operator GreaterThanThreshold \
    --evaluation-periods 1 \
    --alarm-actions arn:aws:sns:us-east-1:$ACCOUNT_ID:data-quality-alerts

# 품질률 알람
aws cloudwatch put-metric-alarm \
    --alarm-name "Low-Data-Quality-Rate" \
    --alarm-description "데이터 품질률 80% 미만" \
    --metric-name DataQualityRate \
    --namespace DataQuality/Streaming \
    --statistic Average \
    --period 300 \
    --threshold 80 \
    --comparison-operator LessThanThreshold \
    --evaluation-periods 2

echo "[4/4] 설정 완료"
echo "대시보드 URL: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DataQualityDashboard"
rm -f quality-dashboard.json
