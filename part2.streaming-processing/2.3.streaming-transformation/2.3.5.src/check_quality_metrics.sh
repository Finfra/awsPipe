#!/bin/bash
# 품질 모니터링 결과 확인 스크립트

set -e

STREAM_NAME=${1:-"log-streaming-demo"}

echo "[1/4] CloudWatch 품질 메트릭 확인"
echo "총 레코드 수:"
aws cloudwatch get-metric-statistics \
    --namespace DataQuality/Streaming \
    --metric-name TotalRecords \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum \
    --query 'Datapoints[*].[Timestamp,Sum]' \
    --output table

echo "유효 레코드 수:"
aws cloudwatch get-metric-statistics \
    --namespace DataQuality/Streaming \
    --metric-name ValidRecords \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum \
    --query 'Datapoints[*].[Timestamp,Sum]' \
    --output table

echo "무효 레코드 수:"
aws cloudwatch get-metric-statistics \
    --namespace DataQuality/Streaming \
    --metric-name InvalidRecords \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum \
    --query 'Datapoints[*].[Timestamp,Sum]' \
    --output table

echo "[2/4] Lambda 함수 로그 확인"
aws logs filter-log-events \
    --log-group-name /aws/lambda/data-quality-monitor \
    --start-time $(date -d '1 hour ago' +%s)000 \
    --query 'events[*].[eventTimestamp,message]' \
    --output table

echo "[3/4] 알람 상태 확인"
aws cloudwatch describe-alarms \
    --alarm-names "High-Invalid-Data" "Low-Data-Quality-Rate" \
    --query 'MetricAlarms[*].[AlarmName,StateValue,StateReason]' \
    --output table

echo "[4/4] Kinesis 스트림 메트릭 확인"
aws cloudwatch get-metric-statistics \
    --namespace AWS/Kinesis \
    --metric-name IncomingRecords \
    --dimensions Name=StreamName,Value=$STREAM_NAME \
    --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
    --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
    --period 300 \
    --statistics Sum \
    --query 'Datapoints[*].[Timestamp,Sum]' \
    --output table

echo "대시보드 URL: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=DataQualityDashboard"
