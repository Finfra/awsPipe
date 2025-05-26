#!/usr/bin/env python3
"""
Kinesis → S3 → EMR 완전 파이프라인 구축
전체 데이터 파이프라인을 통합하여 실시간 스트리밍부터 배치 처리까지
"""

import boto3
import json
import time
import threading
from datetime import datetime, timedelta
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedPipeline:
    """통합 데이터 파이프라인"""
    
    def __init__(self, config):
        self.config = config
        self.region = config['region']
        
        # AWS 클라이언트 초기화
        self.kinesis = boto3.client('kinesis', region_name=self.region)
        self.firehose = boto3.client('firehose', region_name=self.region)
        self.s3 = boto3.client('s3', region_name=self.region)
        self.emr = boto3.client('emr', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        self.events = boto3.client('events', region_name=self.region)
        
        # 파이프라인 상태
        self.pipeline_status = {
            'kinesis_stream': 'NOT_CREATED',
            'firehose_stream': 'NOT_CREATED',
            'emr_cluster': 'NOT_CREATED',
            's3_bucket': 'NOT_CREATED',
            'lambda_functions': 'NOT_CREATED'
        }
    
    def create_transform_lambda(self):
        """데이터 변환용 Lambda 함수 생성"""
        
        function_name = f"{self.config['project_name']}-data-transform"
        
        logger.info(f"변환 Lambda 함수 생성: {function_name}")
        
        # Lambda 함수 코드
        transform_code = '''
import json
import base64
from datetime import datetime
import gzip

def lambda_handler(event, context):
    """Firehose 데이터 변환 함수"""
    
    output_records = []
    
    for record in event['records']:
        try:
            # 입력 데이터 디코딩
            payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))
            
            # 데이터 변환 및 enrichment
            transformed_payload = {
                'timestamp': payload.get('timestamp', datetime.utcnow().isoformat()),
                'event_type': payload.get('event_type', 'unknown'),
                'user_id': payload.get('user_id'),
                'session_id': payload.get('session_id'),
                'ip_address': payload.get('ip_address'),
                'value': payload.get('value', 0),
                'processed_at': datetime.utcnow().isoformat(),
                'data_quality': 'valid' if all(k in payload for k in ['timestamp', 'event_type']) else 'incomplete'
            }
            
            # 출력 데이터 인코딩
            output_data = json.dumps(transformed_payload) + '\\n'
            encoded_data = base64.b64encode(output_data.encode('utf-8')).decode('utf-8')
            
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': encoded_data
            })
            
        except Exception as e:
            # 처리 실패한 레코드
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed'
            })
    
    return {'records': output_records}
'''
        
        try:
            # Lambda 함수 생성
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=self.config['lambda_role_arn'],
                Handler='index.lambda_handler',
                Code={'ZipFile': transform_code.encode()},
                Timeout=300,
                MemorySize=256,
                Tags={
                    'Project': self.config['project_name'],
                    'Purpose': 'DataTransformation'
                }
            )
            
            lambda_arn = response['FunctionArn']
            logger.info(f"변환 Lambda 함수 생성 완료: {lambda_arn}")
            
            return lambda_arn
            
        except self.lambda_client.exceptions.ResourceConflictException:
            # 함수가 이미 존재하는 경우
            response = self.lambda_client.get_function(FunctionName=function_name)
            logger.info("변환 Lambda 함수가 이미 존재함")
            return response['Configuration']['FunctionArn']
            
        except Exception as e:
            logger.error(f"변환 Lambda 함수 생성 실패: {e}")
            raise
    
    def create_emr_cluster(self):
        """EMR 클러스터 생성"""
        
        cluster_name = f"{self.config['project_name']}-streaming-cluster"
        
        logger.info(f"EMR 클러스터 생성: {cluster_name}")
        
        try:
            response = self.emr.run_job_flow(
                Name=cluster_name,
                ReleaseLabel='emr-6.15.0',
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hadoop'},
                    {'Name': 'Hive'}
                ],
                
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1
                        },
                        {
                            'Name': 'Core',
                            'Market': 'SPOT',
                            'BidPrice': '0.20',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.large',
                            'InstanceCount': 2
                        }
                    ],
                    'Ec2KeyName': self.config.get('ec2_key_name'),
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                    'Ec2SubnetId': self.config.get('subnet_id')
                },
                
                ServiceRole=self.config['emr_service_role'],
                JobFlowRole=self.config['emr_ec2_instance_profile'],
                
                LogUri=f"s3://{self.config['s3_bucket_name']}/emr-logs/",
                
                # Spark 설정 최적화
                Configurations=[
                    {
                        'Classification': 'spark-defaults',
                        'Properties': {
                            'spark.sql.adaptive.enabled': 'true',
                            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                            'spark.hadoop.fs.s3a.multipart.size': '134217728',
                            'spark.sql.parquet.compression.codec': 'snappy',
                            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
                        }
                    }
                ],
                
                # 자동 종료 설정
                AutoTerminationPolicy={
                    'IdleTimeout': 3600  # 1시간 idle 후 종료
                },
                
                Tags=[
                    {'Key': 'Project', 'Value': self.config['project_name']},
                    {'Key': 'Environment', 'Value': self.config.get('environment', 'dev')},
                    {'Key': 'Purpose', 'Value': 'StreamingDataProcessing'}
                ]
            )
            
            cluster_id = response['JobFlowId']
            self.config['emr_cluster_id'] = cluster_id
            
            self.pipeline_status['emr_cluster'] = 'CREATED'
            logger.info(f"EMR 클러스터 생성 완료: {cluster_id}")
            
            return cluster_id
            
        except Exception as e:
            logger.error(f"EMR 클러스터 생성 실패: {e}")
            self.pipeline_status['emr_cluster'] = 'FAILED'
            raise
    
    def upload_spark_jobs(self):
        """Spark 작업 스크립트를 S3에 업로드"""
        
        logger.info("Spark 작업 스크립트 업로드")
        
        # 실시간 데이터 처리 스크립트
        realtime_processor = '''
#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    return SparkSession.builder \\
        .appName("RealtimeDataProcessor") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .getOrCreate()

def process_streaming_data(spark, input_path, output_path):
    """실시간 스트리밍 데이터 처리"""
    
    print(f"처리 시작: {input_path} -> {output_path}")
    
    # 데이터 읽기
    df = spark.read.json(input_path)
    
    # 데이터 품질 필터링
    quality_df = df.filter(
        col("data_quality") == "valid"
    ).filter(
        col("timestamp").isNotNull() & 
        col("event_type").isNotNull()
    )
    
    # 실시간 집계
    # 1. 사용자별 활동 요약
    user_activity = quality_df \\
        .groupBy("user_id") \\
        .agg(
            count("*").alias("total_events"),
            sum("value").alias("total_value"),
            countDistinct("session_id").alias("unique_sessions"),
            max("timestamp").alias("last_activity"),
            collect_set("event_type").alias("event_types")
        )
    
    # 2. 시간별 트렌드
    hourly_trends = quality_df \\
        .withColumn("hour", date_format("timestamp", "yyyy-MM-dd HH:00:00")) \\
        .groupBy("hour", "event_type") \\
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            avg("value").alias("avg_value"),
            sum("value").alias("total_value")
        )
    
    # 3. 이상 탐지
    window_spec = Window.partitionBy("event_type")
    
    anomalies = quality_df \\
        .withColumn("avg_value", avg("value").over(window_spec)) \\
        .withColumn("stddev_value", stddev("value").over(window_spec)) \\
        .withColumn("z_score", 
            abs(col("value") - col("avg_value")) / col("stddev_value")
        ) \\
        .filter(col("z_score") > 3) \\
        .select("timestamp", "user_id", "event_type", "value", "z_score")
    
    # 결과 저장
    user_activity.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/user-activity/")
    hourly_trends.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/hourly-trends/")
    anomalies.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/anomalies/")
    
    print("처리 완료")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit script.py <input_path> <output_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    spark = create_spark_session()
    try:
        process_streaming_data(spark, input_path, output_path)
    finally:
        spark.stop()
'''
        
        try:
            # S3에 스크립트 업로드
            self.s3.put_object(
                Bucket=self.config['s3_bucket_name'],
                Key='scripts/realtime_processor.py',
                Body=realtime_processor,
                ContentType='text/x-python'
            )
            
            logger.info("Spark 스크립트 업로드 완료")
            
        except Exception as e:
            logger.error(f"Spark 스크립트 업로드 실패: {e}")
            raise
    
    def create_s3_trigger_lambda(self):
        """S3 이벤트 트리거 Lambda 함수 생성"""
        
        function_name = f"{self.config['project_name']}-s3-trigger"
        
        logger.info(f"S3 트리거 Lambda 함수 생성: {function_name}")
        
        trigger_code = f'''
import json
import boto3
import os
from urllib.parse import unquote_plus
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    emr = boto3.client('emr')
    
    cluster_id = os.environ.get('EMR_CLUSTER_ID')
    if not cluster_id:
        logger.error("EMR_CLUSTER_ID not set")
        return {{'statusCode': 500}}
    
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"Processing S3 object: s3://{{bucket}}/{{key}}")
            
            # 스트리밍 데이터 파일만 처리
            if key.startswith('streaming-data/') and not key.endswith('/'):
                # 파티션 경로에서 시간 정보 추출
                path_parts = key.split('/')
                if len(path_parts) >= 5:
                    year = path_parts[1].split('=')[1]
                    month = path_parts[2].split('=')[1]
                    day = path_parts[3].split('=')[1]
                    hour = path_parts[4].split('=')[1]
                    
                    input_path = f"s3://{{bucket}}/streaming-data/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/"
                    output_path = f"s3://{{bucket}}/processed-data/year={{year}}/month={{month}}/day={{day}}/hour={{hour}}/"
                    
                    # EMR Step 제출
                    response = emr.add_job_flow_steps(
                        JobFlowId=cluster_id,
                        Steps=[{{
                            'Name': f'Process-{{year}}-{{month}}-{{day}}-{{hour}}',
                            'ActionOnFailure': 'CONTINUE',
                            'HadoopJarStep': {{
                                'Jar': 'command-runner.jar',
                                'Args': [
                                    'spark-submit',
                                    '--deploy-mode', 'cluster',
                                    '--driver-memory', '2g',
                                    '--executor-memory', '2g',
                                    '--executor-cores', '2',
                                    '--num-executors', '4',
                                    f"s3://{{bucket}}/scripts/realtime_processor.py",
                                    input_path,
                                    output_path
                                ]
                            }}
                        }}]
                    )
                    
                    logger.info(f"EMR step submitted: {{response['StepIds'][0]}}")
                    
        except Exception as e:
            logger.error(f"Error processing S3 event: {{e}}")
    
    return {{'statusCode': 200}}
'''
        
        try:
            # Lambda 함수 생성
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=self.config['lambda_role_arn'],
                Handler='index.lambda_handler',
                Code={'ZipFile': trigger_code.encode()},
                Environment={
                    'Variables': {
                        'EMR_CLUSTER_ID': self.config.get('emr_cluster_id', '')
                    }
                },
                Timeout=300,
                Tags={
                    'Project': self.config['project_name'],
                    'Purpose': 'S3EventTrigger'
                }
            )
            
            lambda_arn = response['FunctionArn']
            logger.info(f"S3 트리거 Lambda 함수 생성 완료: {lambda_arn}")
            
            return lambda_arn
            
        except self.lambda_client.exceptions.ResourceConflictException:
            # 함수가 이미 존재하는 경우 업데이트
            self.lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=trigger_code.encode()
            )
            
            self.lambda_client.update_function_configuration(
                FunctionName=function_name,
                Environment={
                    'Variables': {
                        'EMR_CLUSTER_ID': self.config.get('emr_cluster_id', '')
                    }
                }
            )
            
            response = self.lambda_client.get_function(FunctionName=function_name)
            logger.info("S3 트리거 Lambda 함수 업데이트 완료")
            return response['Configuration']['FunctionArn']
            
        except Exception as e:
            logger.error(f"S3 트리거 Lambda 함수 생성 실패: {e}")
            raise
    
    def setup_monitoring(self):
        """파이프라인 모니터링 설정"""
        
        logger.info("모니터링 및 알람 설정")
        
        try:
            # Kinesis 스트림 모니터링
            self.cloudwatch.put_metric_alarm(
                AlarmName=f"{self.config['project_name']}-kinesis-high-incoming-records",
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=2,
                MetricName='IncomingRecords',
                Namespace='AWS/Kinesis',
                Period=300,
                Statistic='Sum',
                Threshold=10000,
                ActionsEnabled=True,
                AlarmActions=[self.config.get('sns_topic_arn', '')],
                Dimensions=[
                    {
                        'Name': 'StreamName',
                        'Value': self.config['kinesis_stream_name']
                    }
                ]
            )
            
            # EMR 클러스터 모니터링
            if 'emr_cluster_id' in self.config:
                self.cloudwatch.put_metric_alarm(
                    AlarmName=f"{self.config['project_name']}-emr-failed-steps",
                    ComparisonOperator='GreaterThanThreshold',
                    EvaluationPeriods=1,
                    MetricName='StepsFailed',
                    Namespace='AWS/ElasticMapReduce',
                    Period=300,
                    Statistic='Sum',
                    Threshold=0,
                    ActionsEnabled=True,
                    AlarmActions=[self.config.get('sns_topic_arn', '')],
                    Dimensions=[
                        {
                            'Name': 'JobFlowId',
                            'Value': self.config['emr_cluster_id']
                        }
                    ]
                )
            
            logger.info("모니터링 설정 완료")
            
        except Exception as e:
            logger.warning(f"모니터링 설정 실패 (계속 진행): {e}")
    
    def build_complete_pipeline(self):
        """완전한 파이프라인 구축"""
        
        logger.info("=== 통합 데이터 파이프라인 구축 시작 ===")
        
        try:
            # 1. S3 버킷 생성
            s3_bucket_arn = self.create_s3_bucket()
            
            # 2. Kinesis 스트림 생성
            kinesis_stream_arn = self.create_kinesis_stream()
            
            # 3. Spark 작업 스크립트 업로드
            self.upload_spark_jobs()
            
            # 4. Firehose 스트림 생성
            self.create_firehose_stream(kinesis_stream_arn, s3_bucket_arn)
            
            # 5. EMR 클러스터 생성
            cluster_id = self.create_emr_cluster()
            
            # 6. S3 이벤트 트리거 Lambda 생성
            trigger_lambda_arn = self.create_s3_trigger_lambda()
            
            # 7. 모니터링 설정
            self.setup_monitoring()
            
            # 8. 파이프라인 상태 출력
            self.print_pipeline_status()
            
            logger.info("=== 통합 파이프라인 구축 완료 ===")
            
            return {
                'status': 'SUCCESS',
                'kinesis_stream': self.config['kinesis_stream_name'],
                'firehose_stream': self.config['firehose_stream_name'],
                's3_bucket': self.config['s3_bucket_name'],
                'emr_cluster_id': cluster_id,
                'lambda_trigger': trigger_lambda_arn
            }
            
        except Exception as e:
            logger.error(f"파이프라인 구축 실패: {e}")
            self.print_pipeline_status()
            raise
    
    def print_pipeline_status(self):
        """파이프라인 상태 출력"""
        
        logger.info("=== 파이프라인 상태 ===")
        for component, status in self.pipeline_status.items():
            status_icon = "✅" if status in ['CREATED', 'EXISTS'] else "❌" if status == 'FAILED' else "⏳"
            logger.info(f"{status_icon} {component}: {status}")
    
    def test_pipeline(self):
        """파이프라인 테스트"""
        
        logger.info("파이프라인 통합 테스트 시작")
        
        # 테스트 데이터 생성
        test_records = [
            {
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': 'page_view',
                'user_id': 12345,
                'session_id': 'test_session_1',
                'value': 100,
                'ip_address': '192.168.1.100'
            },
            {
                'timestamp': datetime.utcnow().isoformat(),
                'event_type': 'click',
                'user_id': 12346,
                'session_id': 'test_session_2',
                'value': 50,
                'ip_address': '192.168.1.101'
            }
        ]
        
        try:
            # Kinesis에 테스트 데이터 전송
            for i, record in enumerate(test_records):
                response = self.kinesis.put_record(
                    StreamName=self.config['kinesis_stream_name'],
                    Data=json.dumps(record),
                    PartitionKey=f"test_key_{i}"
                )
                
                logger.info(f"테스트 데이터 전송: {response['SequenceNumber']}")
            
            logger.info("테스트 데이터 전송 완료")
            logger.info("데이터가 S3에 저장되고 EMR에서 처리될 때까지 대기...")
            
            return True
            
        except Exception as e:
            logger.error(f"파이프라인 테스트 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    
    # 파이프라인 설정
    config = {
        'region': 'us-east-1',
        'project_name': 'integrated-streaming-pipeline',
        'environment': 'dev',
        
        # Kinesis 설정
        'kinesis_stream_name': 'integrated-data-stream',
        'shard_count': 2,
        
        # Firehose 설정
        'firehose_stream_name': 'integrated-firehose-stream',
        'firehose_role_arn': 'arn:aws:iam::123456789012:role/firehose-delivery-role',
        
        # S3 설정
        's3_bucket_name': 'integrated-streaming-data-bucket',
        
        # EMR 설정
        'emr_service_role': 'EMR_DefaultRole',
        'emr_ec2_instance_profile': 'EMR_EC2_DefaultRole',
        'ec2_key_name': 'my-key-pair',
        'subnet_id': 'subnet-12345678',
        
        # Lambda 설정
        'lambda_role_arn': 'arn:aws:iam::123456789012:role/lambda-execution-role',
        
        # 모니터링 설정
        'sns_topic_arn': 'arn:aws:sns:us-east-1:123456789012:pipeline-alerts'
    }
    
    try:
        # 파이프라인 인스턴스 생성
        pipeline = IntegratedPipeline(config)
        
        # 완전한 파이프라인 구축
        result = pipeline.build_complete_pipeline()
        
        # 테스트 실행
        test_result = pipeline.test_pipeline()
        
        if test_result:
            logger.info("🎉 통합 파이프라인 구축 및 테스트 성공!")
        else:
            logger.warning("⚠️ 파이프라인 구축 성공, 테스트 부분적 성공")
        
        # 결과 출력
        logger.info("=== 구축 결과 ===")
        for key, value in result.items():
            logger.info(f"{key}: {value}")
        
    except Exception as e:
        logger.error(f"파이프라인 구축 실패: {e}")
        raise

if __name__ == "__main__":
    main()
