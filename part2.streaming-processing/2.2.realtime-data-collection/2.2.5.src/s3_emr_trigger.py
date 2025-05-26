#!/usr/bin/env python3
"""
S3 이벤트 기반 EMR 작업 트리거
S3에 새로운 파일이 업로드되면 EMR에서 Spark 작업을 자동 실행
"""

import json
import boto3
import os
from datetime import datetime
import urllib.parse

def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    
    # 환경 변수에서 EMR 클러스터 ID 가져오기
    emr_cluster_id = os.environ.get('EMR_CLUSTER_ID')
    if not emr_cluster_id:
        return {
            'statusCode': 400,
            'body': json.dumps('EMR_CLUSTER_ID environment variable not set')
        }
    
    # S3 이벤트 처리
    for record in event['Records']:
        # S3 이벤트 정보 추출
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        print(f"Processing S3 event: {bucket_name}/{object_key}")
        
        # streaming-data 디렉토리의 파일만 처리
        if not object_key.startswith('streaming-data/'):
            print(f"Skipping non-streaming-data file: {object_key}")
            continue
        
        # 파티션 정보 추출 (year/month/day/hour)
        path_parts = object_key.split('/')
        if len(path_parts) < 5:  # streaming-data/year=.../month=.../day=.../hour=.../
            print(f"Invalid path structure: {object_key}")
            continue
        
        # 입력 및 출력 경로 설정
        input_path = f"s3://{bucket_name}/streaming-data/"
        output_path = f"s3://{bucket_name}/processed-data/"
        
        # EMR Step 실행
        step_response = submit_emr_step(
            emr_cluster_id, 
            input_path, 
            output_path,
            object_key
        )
        
        if step_response:
            print(f"EMR Step submitted successfully: {step_response}")
        else:
            print("Failed to submit EMR Step")
    
    return {
        'statusCode': 200,
        'body': json.dumps('S3 event processed successfully')
    }

def submit_emr_step(cluster_id, input_path, output_path, trigger_file):
    """EMR Step 제출"""
    
    emr_client = boto3.client('emr')
    
    # 실행 시각 기반 고유 식별자
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Spark 애플리케이션 설정
    step_config = {
        'Name': f'streaming-data-processing-{timestamp}',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--master', 'yarn',
                '--conf', 'spark.sql.adaptive.enabled=true',
                '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
                '--conf', 'spark.hadoop.fs.s3a.multipart.size=134217728',
                '--conf', 'spark.sql.parquet.compression.codec=snappy',
                '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
                '--driver-memory', '2g',
                '--executor-memory', '2g',
                '--executor-cores', '2',
                '--num-executors', '2',
                f's3://{input_path.split("/")[2]}/scripts/streaming_processor.py',
                input_path,
                f'{output_path}batch-{timestamp}'
            ]
        }
    }
    
    try:
        # EMR Step 추가
        response = emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step_config]
        )
        
        step_id = response['StepIds'][0]
        
        # CloudWatch 로그에 정보 기록
        print(f"EMR Step added successfully:")
        print(f"  Cluster ID: {cluster_id}")
        print(f"  Step ID: {step_id}")
        print(f"  Input Path: {input_path}")
        print(f"  Output Path: {output_path}batch-{timestamp}")
        print(f"  Trigger File: {trigger_file}")
        
        # Step 상태 확인
        step_status = emr_client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        
        print(f"  Step Status: {step_status['Step']['Status']['State']}")
        
        return {
            'step_id': step_id,
            'cluster_id': cluster_id,
            'status': step_status['Step']['Status']['State'],
            'timestamp': timestamp
        }
        
    except Exception as e:
        print(f"Error submitting EMR step: {str(e)}")
        
        # CloudWatch 알람 발생 (선택사항)
        send_failure_notification(cluster_id, str(e))
        
        return None

def send_failure_notification(cluster_id, error_message):
    """EMR Step 실패 시 알림 전송"""
    
    try:
        sns_client = boto3.client('sns')
        
        # SNS 토픽 ARN (환경 변수에서 가져오기)
        topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if not topic_arn:
            print("SNS_TOPIC_ARN not configured, skipping notification")
            return
        
        message = {
            'alarm': 'EMR Step Submission Failed',
            'timestamp': datetime.utcnow().isoformat(),
            'cluster_id': cluster_id,
            'error': error_message
        }
        
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message, indent=2),
            Subject='EMR Pipeline Alert'
        )
        
        print("Failure notification sent to SNS")
        
    except Exception as e:
        print(f"Failed to send SNS notification: {str(e)}")

def check_cluster_status(cluster_id):
    """EMR 클러스터 상태 확인"""
    
    try:
        emr_client = boto3.client('emr')
        
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_status = response['Cluster']['Status']['State']
        
        print(f"Cluster {cluster_id} status: {cluster_status}")
        
        if cluster_status not in ['WAITING', 'RUNNING']:
            print(f"Cluster is not ready for new steps: {cluster_status}")
            return False
        
        return True
        
    except Exception as e:
        print(f"Error checking cluster status: {str(e)}")
        return False

def get_processing_timestamp_from_path(object_key):
    """S3 객체 경로에서 처리 시간 추출"""
    
    # streaming-data/year=2024/month=01/day=15/hour=10/file.parquet
    # 에서 시간 정보 추출
    
    try:
        parts = object_key.split('/')
        year = parts[1].split('=')[1]
        month = parts[2].split('=')[1]
        day = parts[3].split('=')[1]
        hour = parts[4].split('=')[1]
        
        return f"{year}-{month}-{day}T{hour}:00:00Z"
        
    except (IndexError, ValueError):
        return datetime.utcnow().isoformat()

# 로컬 테스트용
if __name__ == "__main__":
    # 테스트 이벤트
    test_event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'test-streaming-bucket'},
                'object': {'key': 'streaming-data/year=2024/month=01/day=15/hour=10/part-00000-xyz.parquet'}
            }
        }]
    }
    
    # 환경 변수 설정 (테스트용)
    os.environ['EMR_CLUSTER_ID'] = 'j-XXXXXXXXXXXXX'
    
    result = lambda_handler(test_event, None)
    print(f"Test result: {result}")
