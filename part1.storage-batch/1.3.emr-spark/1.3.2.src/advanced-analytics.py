# Spark DataFrame 고급 연산 함수들
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

def complex_business_analysis(events_df, users_df, products_df):
    """복합 비즈니스 분석"""
    
    # 1. 브로드캐스트 조인으로 성능 최적화
    enriched_events = events_df \
        .join(broadcast(users_df), "user_id") \
        .join(broadcast(products_df), "product_id")
    
    # 2. 복잡한 집계 쿼리
    regional_analysis = enriched_events \
        .groupBy("region", "product_category", "user_segment") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            sum("value").alias("total_revenue"),
            avg("value").alias("avg_order_value"),
            # 중앙값 계산
            expr("percentile_approx(value, 0.5)").alias("median_order_value"),
            # 분위수 계산
            expr("percentile_approx(value, array(0.25, 0.75))").alias("quartiles")
        )
    
    # 3. 피벗 테이블
    pivot_analysis = enriched_events \
        .groupBy("region") \
        .pivot("product_category") \
        .agg(sum("value").alias("revenue"))
    
    # 4. 윈도우 함수로 랭킹
    window_spec = Window.partitionBy("region").orderBy(desc("total_revenue"))
    
    ranked_analysis = regional_analysis \
        .withColumn("revenue_rank", 
                   row_number().over(window_spec)) \
        .withColumn("revenue_percentile", 
                   percent_rank().over(window_spec))
    
    return {
        'enriched_events': enriched_events,
        'regional_analysis': regional_analysis,
        'pivot_analysis': pivot_analysis,
        'ranked_analysis': ranked_analysis
    }

def create_cohort_analysis(spark, events_df):
    """코호트 분석 (SQL 사용)"""
    
    events_df.createOrReplaceTempView("events")
    
    cohort_analysis = spark.sql("""
        WITH user_first_purchase AS (
            SELECT 
                user_id,
                MIN(DATE(timestamp)) as first_purchase_date,
                DATE_FORMAT(MIN(timestamp), 'yyyy-MM') as cohort_month
            FROM events 
            WHERE event_type = 'purchase'
            GROUP BY user_id
        ),
        user_purchases AS (
            SELECT 
                e.user_id,
                DATE_FORMAT(e.timestamp, 'yyyy-MM') as purchase_month,
                f.cohort_month,
                MONTHS_BETWEEN(e.timestamp, f.first_purchase_date) as month_diff
            FROM events e
            JOIN user_first_purchase f ON e.user_id = f.user_id
            WHERE e.event_type = 'purchase'
        )
        SELECT 
            cohort_month,
            month_diff,
            COUNT(DISTINCT user_id) as users,
            COUNT(*) as purchases,
            SUM(value) as revenue
        FROM user_purchases
        GROUP BY cohort_month, month_diff
        ORDER BY cohort_month, month_diff
    """)
    
    return cohort_analysis

def calculate_moving_averages(df, value_col, date_col, windows=[7, 30, 90]):
    """이동평균 계산"""
    
    # 날짜 기준 윈도우
    window_specs = {}
    for window_size in windows:
        window_specs[f"ma_{window_size}"] = Window \
            .orderBy(date_col) \
            .rowsBetween(-window_size + 1, 0)
    
    # 이동평균 계산
    result_df = df
    for ma_name, window_spec in window_specs.items():
        result_df = result_df.withColumn(
            ma_name,
            avg(value_col).over(window_spec)
        )
    
    return result_df

def detect_anomalies(df, value_col, threshold_std=2.5):
    """이상치 탐지 (Z-score 기반)"""
    
    # 통계값 계산
    stats = df.agg(
        avg(value_col).alias("mean_val"),
        stddev(value_col).alias("std_val")
    ).collect()[0]
    
    mean_val = stats["mean_val"]
    std_val = stats["std_val"]
    
    # Z-score 계산 및 이상치 플래그
    anomaly_df = df \
        .withColumn("z_score", 
                   abs((col(value_col) - lit(mean_val)) / lit(std_val))) \
        .withColumn("is_anomaly", 
                   col("z_score") > threshold_std)
    
    return anomaly_df

def create_feature_engineering(df):
    """피처 엔지니어링"""
    
    # 시간 기반 피처
    time_features = df \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("day_of_week", dayofweek("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("quarter", quarter("timestamp")) \
        .withColumn("is_weekend", 
                   when(dayofweek("timestamp").isin([1, 7]), 1).otherwise(0))
    
    # 사용자 행동 피처
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    
    behavior_features = time_features \
        .withColumn("prev_event_time", 
                   lag("timestamp", 1).over(user_window)) \
        .withColumn("time_since_last_event", 
                   (col("timestamp").cast("long") - col("prev_event_time").cast("long")) / 60) \
        .withColumn("cumulative_events", 
                   row_number().over(user_window)) \
        .withColumn("cumulative_value", 
                   sum("value").over(user_window))
    
    # 집계 피처 (지난 N일간)
    date_window_7d = Window \
        .partitionBy("user_id") \
        .orderBy("timestamp") \
        .rangeBetween(-7 * 24 * 3600, 0)  # 7일간
    
    aggregated_features = behavior_features \
        .withColumn("events_last_7d", 
                   count("*").over(date_window_7d)) \
        .withColumn("value_last_7d", 
                   sum("value").over(date_window_7d)) \
        .withColumn("avg_value_last_7d", 
                   avg("value").over(date_window_7d))
    
    return aggregated_features

def optimize_dataframe_operations(df):
    """DataFrame 연산 최적화"""
    
    # 1. 캐싱 전략
    if df.rdd.getNumPartitions() > 1:
        df_cached = df.cache()
        df_cached.count()  # 캐시 워밍업
        print(f"캐시된 파티션 수: {df_cached.rdd.getNumPartitions()}")
        return df_cached
    
    return df

def create_summary_statistics(df, group_cols, value_cols):
    """요약 통계 생성"""
    
    # 기본 집계
    basic_agg = df.groupBy(*group_cols).agg(
        *[count(col).alias(f"{col}_count") for col in value_cols],
        *[sum(col).alias(f"{col}_sum") for col in value_cols],
        *[avg(col).alias(f"{col}_avg") for col in value_cols],
        *[min(col).alias(f"{col}_min") for col in value_cols],
        *[max(col).alias(f"{col}_max") for col in value_cols]
    )
    
    # 고급 통계
    advanced_agg = df.groupBy(*group_cols).agg(
        *[expr(f"percentile_approx({col}, 0.5)").alias(f"{col}_median") for col in value_cols],
        *[expr(f"percentile_approx({col}, array(0.25, 0.75))").alias(f"{col}_quartiles") for col in value_cols],
        *[stddev(col).alias(f"{col}_stddev") for col in value_cols],
        *[variance(col).alias(f"{col}_variance") for col in value_cols]
    )
    
    # 조인하여 결합
    summary_stats = basic_agg.join(advanced_agg, group_cols)
    
    return summary_stats

def memory_efficient_processing(spark, input_path, output_path, processing_func):
    """메모리 효율적 대용량 데이터 처리"""
    
    # 스트리밍 처리 설정
    spark.conf.set("spark.sql.streaming.maxBatchDuration", "30s")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    streaming_df = spark \
        .readStream \
        .option("maxFilesPerTrigger", 1) \
        .parquet(input_path)
    
    # 사용자 정의 처리 함수 적용
    processed_df = processing_func(streaming_df)
    
    # 출력
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", f"{output_path}/_checkpoint") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    return query

def create_ml_features(events_df):
    """머신러닝용 피처 생성"""
    
    from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
    
    # 범주형 변수 인덱싱
    indexers = []
    categorical_cols = ["event_type", "category", "device_type"]
    
    for col_name in categorical_cols:
        if col_name in events_df.columns:
            indexer = StringIndexer(
                inputCol=col_name, 
                outputCol=f"{col_name}_indexed"
            )
            indexers.append(indexer)
    
    # 피처 벡터화
    feature_cols = [f"{col}_indexed" for col in categorical_cols if col in events_df.columns]
    feature_cols.extend(["value", "hour", "day_of_week"])
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    
    # 정규화
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features"
    )
    
    return indexers, assembler, scaler

if __name__ == "__main__":
    print("✅ 고급 DataFrame 연산 함수들이 로드되었습니다.")
    print("   - complex_business_analysis(): 복합 비즈니스 분석")
    print("   - create_cohort_analysis(): 코호트 분석")
    print("   - calculate_moving_averages(): 이동평균 계산")
    print("   - detect_anomalies(): 이상치 탐지")
    print("   - create_feature_engineering(): 피처 엔지니어링")
    print("   - optimize_dataframe_operations(): 성능 최적화")
    print("   - create_summary_statistics(): 요약 통계")
    print("   - memory_efficient_processing(): 메모리 효율적 처리")
    print("   - create_ml_features(): ML 피처 생성")
