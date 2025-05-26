#!/usr/bin/env python3
"""
S3/EMR 연동 파이프라인
Kinesis → S3 → EMR 배치 처리를 연결하는 완전한 파이프라인
"""

import boto3
import json
import time
from datetime import datetime, timedelta
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KinesisS3EMRPipeline:
    """Kinesis → S3 → EMR 통합 파이프라인"""
    
    def __init__(self, config):
        self.config = config
        self.kinesis = boto3.client('kinesis', region_name=config['region'])
        self.firehose = boto3.client('firehose', region_name=config['region'])
        self.s3 = boto3.client('s3', region_name=config['region'])
        self.emr = boto3.client('emr', region_name=config['region'])
        self.lambda_client = boto3.client('lambda', region_name=config['region'])
        
    def setup_complete_pipeline(self):
        """완전한 파이프라인 설정"""
        
        logger.info("=== Kinesis → S3 → EMR 파이프라인 설정 시작 ===")
        
        try:
            # 1. Spark 처리 스크립트를 S3에 업로드
            self.upload_spark_script()
            
            # 2. Firehose 전송 스트림 생성
            firehose_response = self.create_firehose_delivery_stream()
            
            # 3. EMR 클러스터 생성
            cluster_id = self.create_emr_cluster()
            self.config['emr_cluster_id'] = cluster_id
            
            # 4. S3 이벤트 트리거 설정
            self.create_s3_event_trigger()
            
            # 5. 모니터링 설정
            self.setup_monitoring()
            
            logger.info("=== 파이프라인 설정 완료 ===")
            logger.info(f"EMR 클러스터 ID: {cluster_id}")
            logger.info(f"Firehose 스트림: {self.config['firehose_stream_name']}")
            
            return {
                'cluster_id': cluster_id,
                'firehose_stream': self.config['firehose_stream_name'],
                'status': 'SUCCESS'
            }
            
        except Exception as e:
            logger.error(f"파이프라인 설정 실패: {e}")
            raise
    
    def upload_spark_script(self):
        """Spark 처리 스크립트를 S3에 업로드"""
        
        logger.info("Spark 스크립트 S3 업로드")
        
        spark_script = '''
#!/usr/bin/env python3
"""
스트리밍 데이터 배치 처리 Spark 작업
S3의 파티션된 데이터를 읽어서 집계 처리 후 결과를 S3에 저장
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import argparse

def create_spark_session():
    return SparkSession.builder \\
        .appName("StreamingDataProcessor") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728") \\
        .config("spark.sql.parquet.compression.codec", "snappy") \\
        .getOrCreate()

def process_streaming_data(spark, input_path, output_path):
    """스트리밍 데이터 처리"""
    
    print(f"처리 시작: {input_path} -> {output_path}")
    
    # 스트리밍 데이터 읽기
    df = spark.read.parquet(input_path)
    
    print(f"읽은 레코드 수: {df.count()}")
    
    # 실시간 집계 처리
    # 1. 사용자별 활동 요약
    user_activity = df \\
        .filter(col("user_id").isNotNull()) \\
        .groupBy("user_id") \\
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("unique_sessions"), 
            sum("value").alias("total_value"),
            avg("value").alias("avg_value"),
            collect_list("event_type").alias("event_types"),
            max("timestamp").alias("last_activity")
        )
    
    # 2. 시간별 트렌드 분석
    hourly_trends = df \\
        .withColumn("hour", hour("timestamp")) \\
        .groupBy("hour", "event_type") \\
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            sum("value").alias("total_value")
        ) \\
        .orderBy("hour", "event_type")
    
    # 3. 이상 값 탐지
    stats = df.select(
        mean("value").alias("mean_value"),
        stddev("value").alias("stddev_value")
    ).collect()[0]
    
    anomalies = df \\
        .withColumn("z_score", 
            abs(col("value") - lit(stats["mean_value"])) / lit(stats["stddev_value"])
        ) \\
        .filter(col("z_score") > 3) \\
        .select("timestamp", "user_id", "event_type", "value", "z_score")
    
    # 결과 저장
    user_activity \\
        .coalesce(1) \\
        .write \\
        .mode("overwrite") \\
        .parquet(f"{output_path}/user-activity/")
    
    hourly_trends \\
        .coalesce(1) \\
        .write \\
        .mode("overwrite") \\
        .parquet(f"{output_path}/hourly-trends/")
    
    anomalies \\
        .coalesce(1) \\
        .write \\
        .mode("overwrite") \\
        .parquet(f"{output_path}/anomalies/")
    
    print("처리 완료")
    
    return {
        'user_activity_count': user_activity.count(),
        'hourly_trends_count': hourly_trends.count(),
        'anomalies_count': anomalies.count()
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help='입력 S3 경로')
    parser.add_argument('--output', required=True, help='출력 S3 경로')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        results = process_streaming_data(spark, args.input, args.output)
        print(f"처리 결과: {results}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
'''
        
        # S3에 스크립트 업로드
        script_key = 'scripts/streaming_processor.py'
        
        try:
            self.s3.put_object(
                Bucket=self.config['s3_bucket'],
                Key=script_key,
                Body=spark_script,
                ContentType='text/x-python'
            )
            
            logger.info(f"Spark 스크립트 업로드 완료: s3://{self.config['s3_bucket']}/{script_key}")
            
        except Exception as e:
            logger.error(f"Spark 스크립트 업로드 실패: {e}")
            raise
    
    def create_firehose_delivery_stream(self):
        """Kinesis → S3 Firehose 스트림 생성"""
        
        logger.info("Firehose 전송 스트림 생성")
        
        try:
            # 기존 스트림 확인
            try:
                self.firehose.describe_delivery_stream(
                    DeliveryStreamName=self.config['firehose_stream_name']
                )
                logger.info("Firehose 스트림이 이미 존재함")
                return None
            except self.firehose.exceptions.ResourceNotFoundException:
                pass  # 스트림이 없으므로 생성 진행
            
            response = self.firehose.create_delivery_stream(
                DeliveryStreamName=self.config['firehose_stream_name'],
                DeliveryStreamType='KinesisStreamAsSource',
                KinesisStreamSourceConfiguration={
                    'KinesisStreamARN': self.config['kinesis_stream_arn'],
                    'RoleARN': self.config['firehose_role_arn']
                },
                ExtendedS3DestinationConfiguration={
                    'RoleARN': self.config['firehose_role_arn'],
                    'BucketARN': self.config['s3_bucket_arn'],
                    'Prefix': 'streaming-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
                    'ErrorOutputPrefix': 'streaming-errors/',
                    
                    # 버퍼링 설정
                    'BufferingHints': {
                        'SizeInMBs': 128,
                        'IntervalInSeconds': 300
                    },
                    
                    # 압축 설정
                    'CompressionFormat': 'SNAPPY',
                    
                    # CloudWatch 로깅
                    'CloudWatchLoggingOptions': {
                        'Enabled': True,
                        'LogGroupName': f"/aws/kinesisfirehose/{self.config['firehose_stream_name']}"
                    }
                }
            )
            
            logger.info(f"Firehose 스트림 생성 완료: {response['DeliveryStreamARN']}")
            return response
            
        except Exception as e:
            logger.error(f"Firehose 스트림 생성 실패: {e}")
            raise
    
    def create_emr_cluster(self):
        """EMR 클러스터 생성"""
        
        logger.info("EMR 클러스터 생성")
        
        try:
            response = self.emr.run_job_flow(
                Name=f"{self.config['project_name']}-streaming-cluster",
                ReleaseLabel='emr-6.15.0',
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hadoop'}
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
                
                LogUri=f"s3://{self.config['s3_bucket']}/emr-logs/",
                
                Configurations=[
                    {
                        'Classification': 'spark-defaults',
                        'Properties': {
                            'spark.sql.adaptive.enabled': 'true',
                            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                            'spark.hadoop.fs.s3a.multipart.size': '134217728',
                            'spark.sql.parquet.compression.codec': 'snappy'
                        }
                    }
                ],
                
                AutoTerminationPolicy={
                    'IdleTimeout': 14400  # 4시간
                },
                
                Tags=[
                    {'Key': 'Project', 'Value': self.config['project_name']},
                    {'Key': 'Environment', 'Value': self.config.get('environment', 'dev')},
                    {'Key': 'Purpose', 'Value': 'StreamingDataProcessing'}
                ]
            )
            
            cluster_id = response['JobFlowId']
            logger.info(f"EMR 클러스터 생성 완료: {cluster_id}")
            
            return cluster_id
            
        except Exception as e:
            logger.error(f"EMR 클러스터 생성 실패: {e}")
            raise
    
    def create_s3_event_trigger(self):
        """S3 이벤트 기반 EMR 작업 트리거 생성"""
        
        logger.info("S3 이벤트 트리거 생성")
        
        lambda_function_code = f'''
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
        logger.error("EMR_CLUSTER_ID 환경변수가 설정되지 않음")
        return {{'statusCode': 500, 'body': 'Missing EMR_CLUSTER_ID'}}
    
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"처리 중인 S3 객체: s3://{{bucket}}/{{key}}")
            
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
                    
                    # EMR 작업 제출
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
                                    f"s3://{{bucket}}/scripts/streaming_processor.py",
                                    '--input', input_path,
                                    '--output', output_path
                                ]
                            }}
                        }}]
                    )
                    
                    step_id = response['StepIds'][0]
                    logger.info(f"EMR 작업 제출 완료: {{step_id}}")
                    
        except Exception as e:
            logger.error(f"S3 이벤트 처리 오류: {{e}}")
    
    return {{'statusCode': 200, 'body': 'Success'}}
'''
        
        function_name = f"{self.config['project_name']}-s3-emr-trigger"
        
        try:
            # Lambda 함수 생성
            self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.9',
                Role=self.config['lambda_role_arn'],
                Handler='index.lambda_handler',
                Code={'ZipFile': lambda_function_code.encode()},
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
            logger.info(f"Lambda 트리거 함수 생성 완료: {function_name}")
            
        except self.lambda_client.exceptions.ResourceConflictException:
            # 함수가 이미 존재하는 경우 업데이트
            self.lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=lambda_function_code.encode()
            )
            
            self.lambda_client.update_function_configuration(
                FunctionName=function_name,
                Environment={
                    'Variables': {
                        'EMR_CLUSTER_ID': self.config.get('emr_cluster_id', '')
                    }
                }
            )
            logger.info(f"Lambda 트리거 함수 업데이트 완료: {function_name}")
    
    def setup_monitoring(self):
        """파이프라인 모니터링 설정"""
        
        logger.info("모니터링 설정")
        
        cloudwatch = boto3.client('cloudwatch', region_name=self.config['region'])
        
        # EMR 클러스터 모니터링 알람
        try:
            cloudwatch.put_metric_alarm(
                AlarmName=f"{self.config['project_name']}-emr-cluster-failed-steps",
                ComparisonOperator='GreaterThanThreshold',
                EvaluationPeriods=1,
                MetricName='StepsFailed',
                Namespace='AWS/ElasticMapReduce',
                Period=300,
                Statistic='Sum',
                Threshold=0,
                ActionsEnabled=True,
                AlarmActions=[self.config.get('sns_topic_arn', '')],
                AlarmDescription='EMR cluster has failed steps',
                Dimensions=[
                    {
                        'Name': 'JobFlowId',
                        'Value': self.config.get('emr_cluster_id', '')
                    }
                ]
            )
            
            logger.info("EMR 모니터링 알람 생성 완료")
            
        except Exception as e:
            logger.warning(f"모니터링 알람 생성 실패: {e}")
    
    def test_pipeline(self):
        """파이프라인 테스트"""
        
        logger.info("파이프라인 테스트 시작")
        
        # 테스트 데이터 Kinesis에 전송
        test_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': 12345,
            'event_type': 'test_event',
            'value': 100.0,
            'session_id': 'test_session_123'
        }
        
        try:
            response = self.kinesis.put_record(
                StreamName=self.config['kinesis_stream_name'],
                Data=json.dumps(test_data),
                PartitionKey='test_key'
            )
            
            logger.info(f"테스트 데이터 전송 완료: {response['SequenceNumber']}")
            
            # Firehose가 S3에 데이터를 저장할 때까지 대기 (최대 5분)
            logger.info("데이터가 S3에 저장될 때까지 대기 중...")
            time.sleep(300)
            
            # S3에서 데이터 확인
            s3_objects = self.s3.list_objects_v2(
                Bucket=self.config['s3_bucket'],
                Prefix='streaming-data/'
            )
            
            if 'Contents' in s3_objects:
                logger.info(f"S3에 {len(s3_objects['Contents'])}개 파일 저장됨")
                return True
            else:
                logger.warning("S3에 데이터가 저장되지 않음")
                return False
                
        except Exception as e:
            logger.error(f"파이프라인 테스트 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    
    # 설정 예시
    config = {
        'region': 'us-east-1',
        'project_name': 'streaming-pipeline',
        'environment': 'dev',
        
        # Kinesis
        'kinesis_stream_name': 'streaming-data-stream',
        'kinesis_stream_arn': 'arn:aws:kinesis:us-east-1:123456789012:stream/streaming-data-stream',
        
        # Firehose
        'firehose_stream_name': 'streaming-to-s3',
        'firehose_role_arn': 'arn:aws:iam::123456789012:role/firehose-delivery-role',
        
        # S3
        's3_bucket': 'my-streaming-data-bucket',
        's3_bucket_arn': 'arn:aws:s3:::my-streaming-data-bucket',
        
        # EMR
        'emr_service_role': 'EMR_DefaultRole',
        'emr_ec2_instance_profile': 'EMR_EC2_DefaultRole',
        'ec2_key_name': 'my-key-pair',
        'subnet_id': 'subnet-12345678',
        
        # Lambda
        'lambda_role_arn': 'arn:aws:iam::123456789012:role/lambda-execution-role',
        
        # 모니터링
        'sns_topic_arn': 'arn:aws:sns:us-east-1:123456789012:pipeline-alerts'
    }
    
    try:
        pipeline = KinesisS3EMRPipeline(config)
        
        # 파이프라인 설정
        result = pipeline.setup_complete_pipeline()
        
        # 테스트 실행
        test_result = pipeline.test_pipeline()
        
        if test_result:
            logger.info("✅ 파이프라인 테스트 성공")
        else:
            logger.warning("⚠️ 파이프라인 테스트 부분적 성공")
        
        logger.info("=== 파이프라인 설정 완료 ===")
        logger.info(f"EMR 클러스터: {result['cluster_id']}")
        logger.info(f"Firehose 스트림: {result['firehose_stream']}")
        
    except Exception as e:
        logger.error(f"파이프라인 설정 실패: {e}")
        raise

if __name__ == "__main__":
    main()
