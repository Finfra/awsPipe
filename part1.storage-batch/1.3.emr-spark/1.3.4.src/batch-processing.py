#!/usr/bin/env python3
"""
기본 Spark 데이터 처리 작업
S3의 파티션된 데이터를 읽어서 집계 처리 후 결과를 S3에 저장
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import argparse

def create_spark_session(app_name="BasicDataProcessing"):
    """Spark 세션 생성"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_event_data(spark, input_path, output_path):
    """이벤트 데이터 처리"""
    
    print(f"데이터 읽기 시작: {input_path}")
    
    # S3에서 파티션된 데이터 읽기
    df = spark.read \
        .option("mergeSchema", "false") \
        .parquet(input_path)
    
    print(f"읽은 데이터: {df.count():,} 레코드")
    
    # 기본 통계
    df.printSchema()
    print("데이터 샘플:")
    df.show(5)
    
    # 데이터 처리 - 사용자별 일별 활동 집계
    daily_user_activity = df \
        .filter(col("event_type").isin(["click", "view", "purchase"])) \
        .groupBy("user_id", "year", "month", "day", "event_type") \
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value"),
            avg("value").alias("avg_value")
        ) \
        .orderBy("year", "month", "day", "user_id")
    
    # 월별 요약 통계
    monthly_summary = df \
        .groupBy("year", "month", "category") \
        .agg(
            countDistinct("user_id").alias("unique_users"),
            count("*").alias("total_events"),
            sum("value").alias("total_revenue"),
            avg("value").alias("avg_order_value")
        ) \
        .orderBy("year", "month")
    
    print("처리 결과 미리보기:")
    print("일별 사용자 활동:")
    daily_user_activity.show(10)
    
    print("월별 요약:")
    monthly_summary.show()
    
    # 결과 저장
    print(f"결과 저장 시작: {output_path}")
    
    # 일별 활동 데이터 저장 (파티셔닝)
    daily_user_activity \
        .coalesce(4) \
        .write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"{output_path}/daily-activity/")
    
    # 월별 요약 저장
    monthly_summary \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_path}/monthly-summary/")
    
    print("데이터 처리 완료")
    
    return daily_user_activity, monthly_summary

def analyze_performance_metrics(spark, df):
    """성능 메트릭 분석"""
    
    print("\n=== 성능 메트릭 분석 ===")
    
    # 파티션 정보
    print(f"현재 파티션 수: {df.rdd.getNumPartitions()}")
    
    # 데이터 분포 확인
    print("월별 데이터 분포:")
    df.groupBy("year", "month").count().orderBy("year", "month").show()
    
    # 카테고리별 분포
    print("카테고리별 분포:")
    df.groupBy("category").count().show()
    
    # 이벤트 타입별 분포
    print("이벤트 타입별 분포:")
    df.groupBy("event_type").count().show()

def main():
    parser = argparse.ArgumentParser(description='기본 Spark 데이터 처리')
    parser.add_argument('--input', required=True, help='입력 S3 경로')
    parser.add_argument('--output', required=True, help='출력 S3 경로')
    parser.add_argument('--app-name', default='BasicDataProcessing', help='Spark 앱 이름')
    
    args = parser.parse_args()
    
    # Spark 세션 생성
    spark = create_spark_session(args.app_name)
    
    try:
        # 데이터 처리 실행
        daily_activity, monthly_summary = process_event_data(
            spark, args.input, args.output
        )
        
        # 성능 분석
        analyze_performance_metrics(spark, spark.read.parquet(args.input))
        
        print("\n작업 완료!")
        print(f"결과 확인: {args.output}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
