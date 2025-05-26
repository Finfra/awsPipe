import json
import boto3
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def lambda_handler(event, context):
    quality_metrics = {
        'total_records': 0,
        'valid_records': 0,
        'invalid_records': 0
    }
    
    for record in event['Records']:
        try:
            payload = json.loads(record['kinesis']['data'])
            quality_metrics['total_records'] += 1
            
            if validate_record_quality(payload):
                quality_metrics['valid_records'] += 1
            else:
                quality_metrics['invalid_records'] += 1
                
        except Exception:
            quality_metrics['invalid_records'] += 1
    
    # CloudWatch 메트릭 전송
    send_quality_metrics(quality_metrics)
    
    return {'statusCode': 200}

def validate_record_quality(record):
    """데이터 품질 검증 로직"""
    # 필수 필드 검증
    required_fields = ['timestamp', 'event_type']
    for field in required_fields:
        if field not in record or not record[field]:
            return False
    
    # 타임스탬프 형식 검증
    try:
        datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
    except:
        return False
    
    # 추가 비즈니스 로직 검증
    if record.get('user_id') and not isinstance(record['user_id'], int):
        return False
    
    return True

def send_quality_metrics(metrics):
    """CloudWatch 메트릭 전송"""
    try:
        cloudwatch.put_metric_data(
            Namespace='DataQuality/Streaming',
            MetricData=[
                {
                    'MetricName': 'TotalRecords',
                    'Value': metrics['total_records'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'ValidRecords', 
                    'Value': metrics['valid_records'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'InvalidRecords',
                    'Value': metrics['invalid_records'],
                    'Unit': 'Count'
                },
                {
                    'MetricName': 'DataQualityRate',
                    'Value': (metrics['valid_records'] / max(metrics['total_records'], 1)) * 100,
                    'Unit': 'Percent'
                }
            ]
        )
        print(f"메트릭 전송 완료: {metrics}")
    except Exception as e:
        print(f"메트릭 전송 오류: {e}")
