#!/usr/bin/env python3
"""
최적화된 Spark 데이터 처리 작업
고급 최적화 기법들을 적용한 성능 향상된 데이터 처리
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import argparse
import time

def create_optimized_spark_session(app_name="OptimizedDataProcessing"):
    """최적화된 Spark 세션 생성"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.hadoop.fs.s3a.multipart.size", "134217728") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def optimize_dataframe_partitions(df, target_partition_size_mb=128):
    """DataFrame 파티션 최적화"""
    
    # 현재 파티션 수
    current_partitions = df.rdd.getNumPartitions()
    
    # 데이터 크기 추정 (MB)
    estimated_size_mb = df.rdd.map(lambda x: len(str(x))).reduce(lambda a, b: a + b) / 1024 / 1024
    
    # 최적 파티션 수 계산
    optimal_partitions = max(int(estimated_size_mb / target_partition_size_mb), 1)
    
    print(f"파티션 최적화: {current_partitions} -> {optimal_partitions}")
    print(f"추정 데이터 크기: {estimated_size_mb:.2f} MB")
    
    if optimal_partitions < current_partitions:
        return df.coalesce(optimal_partitions)
    elif optimal_partitions > current_partitions:
        return df.repartition(optimal_partitions)
    else:
        return df

def process_with_caching_strategy(spark, input_path, output_path):
    """캐싱 전략을 활용한 데이터 처리"""
    
    print(f"최적화된 데이터 처리 시작: {input_path}")
    
    # 데이터 읽기
    start_time = time.time()
    df = spark.read.parquet(input_path)
    read_time = time.time() - start_time
    print(f"데이터 읽기 완료: {read_time:.2f}초")
    
    # 파티션 최적화
    df_optimized = optimize_dataframe_partitions(df)
    
    # 자주 사용될 데이터는 캐시
    df_cached = df_optimized.cache()
    
    # 캐시 워밍업
    record_count = df_cached.count()
    print(f"총 레코드 수: {record_count:,}")
    
    # 복잡한 집계 작업들
    results = {}
    
    # 1. 사용자별 행동 패턴 분석
    start_time = time.time()
    user_behavior = df_cached \
        .groupBy("user_id") \
        .agg(
            countDistinct("event_type").alias("distinct_events"),
            count("*").alias("total_events"),
            sum("value").alias("lifetime_value"),
            first("category").alias("primary_category"),
            max("timestamp").alias("last_activity")
        ) \
        .filter(col("total_events") >= 5)  # 활성 사용자만
    
    user_behavior.cache()  # 추가 분석에 사용될 예정
    active_users = user_behavior.count()
    behavior_time = time.time() - start_time
    
    print(f"사용자 행동 분석 완료: {active_users:,}명, {behavior_time:.2f}초")
    
    # 2. 상품 성과 분석 (브로드캐스트 조인 활용)
    start_time = time.time()
    
    # 상품별 기본 통계
    product_stats = df_cached \
        .filter(col("event_type") == "purchase") \
        .groupBy("category") \
        .agg(
            count("*").alias("purchase_count"),
            sum("value").alias("total_revenue"),
            avg("value").alias("avg_price"),
            countDistinct("user_id").alias("unique_buyers")
        )
    
    product_time = time.time() - start_time
    print(f"상품 성과 분석 완료: {product_time:.2f}초")
    
    # 3. 시계열 트렌드 분석
    start_time = time.time()
    
    # 윈도우 함수를 활용한 트렌드 분석
    from pyspark.sql.window import Window
    
    daily_trends = df_cached \
        .withColumn("date", to_date("timestamp")) \
        .groupBy("date", "category") \
        .agg(
            count("*").alias("daily_events"),
            sum("value").alias("daily_revenue"),
            countDistinct("user_id").alias("daily_active_users")
        )
    
    # 7일 이동평균 계산
    window_7d = Window.partitionBy("category").orderBy("date").rowsBetween(-6, 0)
    
    trends_with_ma = daily_trends \
        .withColumn("events_7d_ma", avg("daily_events").over(window_7d)) \
        .withColumn("revenue_7d_ma", avg("daily_revenue").over(window_7d)) \
        .withColumn("users_7d_ma", avg("daily_active_users").over(window_7d))
    
    trend_time = time.time() - start_time
    print(f"시계열 트렌드 분석 완료: {trend_time:.2f}초")
    
    # 4. 고급 집계 - Cube와 Rollup
    start_time = time.time()
    
    # 다차원 집계
    multi_dim_analysis = df_cached \
        .cube("category", "event_type") \
        .agg(
            count("*").alias("event_count"),
            sum("value").alias("total_value"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .orderBy("category", "event_type")
    
    cube_time = time.time() - start_time
    print(f"다차원 집계 완료: {cube_time:.2f}초")
    
    # 결과 저장 (파티션 최적화 적용)
    print("결과 저장 시작...")
    
    # 각 결과를 적절한 파티션 수로 저장
    user_behavior \
        .coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_path}/user-behavior/")
    
    trends_with_ma \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .partitionBy("category") \
        .parquet(f"{output_path}/daily-trends/")
    
    multi_dim_analysis \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_path}/multi-dimensional/")
    
    # 캐시 해제
    df_cached.unpersist()
    user_behavior.unpersist()
    
    print("최적화된 처리 완료!")
    
    return {
        'user_behavior': user_behavior,
        'trends': trends_with_ma,
        'multi_dim': multi_dim_analysis,
        'performance': {
            'read_time': read_time,
            'behavior_time': behavior_time,
            'product_time': product_time,
            'trend_time': trend_time,
            'cube_time': cube_time
        }
    }

def performance_comparison(spark, input_path):
    """최적화 전후 성능 비교"""
    
    print("\n=== 성능 비교 테스트 ===")
    
    # 기본 설정으로 읽기
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    start_time = time.time()
    df_basic = spark.read.parquet(input_path)
    basic_count = df_basic.count()
    basic_time = time.time() - start_time
    
    # 최적화 설정으로 읽기
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    start_time = time.time()
    df_optimized = spark.read.parquet(input_path)
    optimized_count = df_optimized.count()
    optimized_time = time.time() - start_time
    
    print(f"기본 설정: {basic_time:.2f}초 ({basic_count:,} 레코드)")
    print(f"최적화 설정: {optimized_time:.2f}초 ({optimized_count:,} 레코드)")
    print(f"성능 향상: {((basic_time - optimized_time) / basic_time * 100):.1f}%")

def analyze_query_plan(spark, df):
    """쿼리 실행 계획 분석"""
    
    print("\n=== 쿼리 실행 계획 분석 ===")
    
    # 복잡한 쿼리 생성
    complex_query = df \
        .filter(col("event_type") == "purchase") \
        .groupBy("category") \
        .agg(sum("value").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))
    
    # 물리적 실행 계획 출력
    print("물리적 실행 계획:")
    complex_query.explain(mode="formatted")
    
    # 논리적 실행 계획
    print("\n논리적 실행 계획:")
    complex_query.explain(extended=True)

def main():
    parser = argparse.ArgumentParser(description='최적화된 Spark 데이터 처리')
    parser.add_argument('--input', required=True, help='입력 S3 경로')
    parser.add_argument('--output', required=True, help='출력 S3 경로')
    parser.add_argument('--app-name', default='OptimizedDataProcessing', help='Spark 앱 이름')
    parser.add_argument('--performance-test', action='store_true', help='성능 비교 테스트 실행')
    
    args = parser.parse_args()
    
    # 최적화된 Spark 세션 생성
    spark = create_optimized_spark_session(args.app_name)
    
    try:
        # 메인 데이터 처리
        results = process_with_caching_strategy(
            spark, args.input, args.output
        )
        
        # 성능 비교 테스트 (옵션)
        if args.performance_test:
            performance_comparison(spark, args.input)
        
        # 쿼리 계획 분석
        df = spark.read.parquet(args.input)
        analyze_query_plan(spark, df)
        
        # 성능 메트릭 출력
        print("\n=== 처리 시간 요약 ===")
        perf = results['performance']
        total_time = sum(perf.values())
        
        for task, duration in perf.items():
            percentage = (duration / total_time) * 100
            print(f"{task:<15}: {duration:>6.2f}초 ({percentage:>5.1f}%)")
        print(f"{'총 처리 시간':<15}: {total_time:>6.2f}초")
        
        print(f"\n작업 완료! 결과 확인: {args.output}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
