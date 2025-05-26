#!/usr/bin/env python3
"""
Spark 스트리밍 데이터 처리기
S3의 Parquet 파일들을 읽어서 배치 처리 후 결과를 다시 S3에 저장
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import datetime, timedelta

def create_spark_session():
    """Spark 세션 생성"""
    return SparkSession.builder \
        .appName("StreamingDataProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def process_log_data(spark, input_path, output_path):
    """로그 데이터 처리"""
    
    print(f"Reading data from: {input_path}")
    
    # 스트리밍 데이터 읽기
    df = spark.read.parquet(input_path)
    
    print(f"Input data count: {df.count()}")
    
    # 데이터 스키마 확인
    df.printSchema()
    
    # 기본 통계 집계
    log_stats = df.groupBy("log_type") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("ip_address").alias("unique_ips"),
            avg("response_size").alias("avg_response_size"),
            max(col("timestamp")).alias("latest_timestamp"),
            min(col("timestamp")).alias("earliest_timestamp")
        )
    
    # 시간대별 이벤트 집계
    hourly_stats = df.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00")) \
        .groupBy("hour", "log_type") \
        .agg(
            count("*").alias("events_per_hour"),
            countDistinct("ip_address").alias("unique_ips_per_hour"),
            avg("response_size").alias("avg_response_size_per_hour")
        ) \
        .orderBy("hour", "log_type")
    
    # HTTP 상태 코드별 집계
    status_stats = df.filter(col("status_code").isNotNull()) \
        .groupBy("status_code") \
        .agg(
            count("*").alias("status_count"),
            (count("*") * 100.0 / df.count()).alias("status_percentage")
        ) \
        .orderBy("status_code")
    
    # 상위 IP 주소별 요청 수
    top_ips = df.groupBy("ip_address") \
        .agg(count("*").alias("request_count")) \
        .orderBy(desc("request_count")) \
        .limit(100)
    
    # 결과 저장
    print(f"Saving results to: {output_path}")
    
    # 파티션별로 저장
    log_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/log_statistics")
    
    hourly_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/hourly_statistics")
    
    status_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/status_statistics")
    
    top_ips.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/top_ips")
    
    # 처리 요약 출력
    print("=== 처리 완료 요약 ===")
    print(f"총 처리 레코드: {df.count():,}")
    print("로그 타입별 통계:")
    log_stats.show()
    
    print("시간대별 상위 5개 집계:")
    hourly_stats.show(5)
    
    print("HTTP 상태 코드 분포:")
    status_stats.show()
    
    return {
        'total_records': df.count(),
        'processing_timestamp': datetime.utcnow().isoformat(),
        'output_path': output_path
    }

def process_application_logs(spark, input_path, output_path):
    """애플리케이션 로그 전용 처리"""
    
    print(f"Processing application logs from: {input_path}")
    
    df = spark.read.parquet(input_path)
    
    # 애플리케이션 로그 필터링
    app_logs = df.filter(col("log_type") == "application")
    
    if app_logs.count() == 0:
        print("No application logs found")
        return
    
    # 로그 레벨별 집계
    log_level_stats = app_logs.groupBy("level") \
        .agg(
            count("*").alias("count"),
            countDistinct("logger").alias("unique_loggers"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .orderBy("level")
    
    # 에러 로그 상세 분석
    error_logs = app_logs.filter(col("level").isin(["ERROR", "WARN"])) \
        .groupBy("logger", "level") \
        .agg(
            count("*").alias("error_count"),
            collect_set("message").alias("error_messages")
        ) \
        .orderBy(desc("error_count"))
    
    # 사용자별 활동 통계
    user_activity = app_logs.filter(col("user_id").isNotNull()) \
        .groupBy("user_id") \
        .agg(
            count("*").alias("activity_count"),
            countDistinct("logger").alias("modules_used"),
            max("timestamp").alias("last_activity")
        ) \
        .orderBy(desc("activity_count")) \
        .limit(50)
    
    # 결과 저장
    log_level_stats.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/log_level_statistics")
    
    error_logs.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/error_analysis")
    
    user_activity.coalesce(1).write.mode("overwrite") \
        .parquet(f"{output_path}/user_activity")
    
    print("애플리케이션 로그 처리 완료")
    log_level_stats.show()

def main():
    """메인 실행 함수"""
    
    if len(sys.argv) != 3:
        print("Usage: spark-submit streaming_processor.py <input_s3_path> <output_s3_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Spark 세션 생성
    spark = create_spark_session()
    
    try:
        # 로그 데이터 처리
        result = process_log_data(spark, input_path, output_path)
        
        # 애플리케이션 로그 별도 처리
        process_application_logs(spark, input_path, f"{output_path}/application_analysis")
        
        # 처리 완료 메타데이터 저장
        metadata_df = spark.createDataFrame([result])
        metadata_df.coalesce(1).write.mode("overwrite") \
            .parquet(f"{output_path}/processing_metadata")
        
        print("모든 처리 완료!")
        
    except Exception as e:
        print(f"처리 중 오류 발생: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
