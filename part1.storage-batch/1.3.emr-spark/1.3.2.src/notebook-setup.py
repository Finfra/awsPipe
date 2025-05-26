# PySpark 환경 설정 및 초기화
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Spark 세션 생성 (EMR Notebooks에서 자동 설정)
def create_spark_session(app_name="EMR-Notebook-Analysis"):
    """EMR Notebook용 Spark 세션 생성"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print(f"✅ Spark Session 생성 완료")
    print(f"   Spark Version: {spark.version}")
    print(f"   Spark UI: {spark.sparkContext.uiWebUrl}")
    print(f"   파티션 수: {spark.sparkContext.defaultParallelism}")
    
    return spark

# S3 데이터 로드 함수
def load_datalake_data(spark, bucket_name, layer, table_name):
    """데이터 레이크에서 데이터 로드"""
    
    s3_path = f"s3://{bucket_name}/{layer}/{table_name}/"
    
    try:
        df = spark.read.parquet(s3_path)
        print(f"✅ 로드 완료: {s3_path}")
        print(f"   레코드 수: {df.count():,}")
        print(f"   컬럼 수: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ 로드 실패: {e}")
        return None

# 데이터 탐색 함수
def explore_dataframe(df, sample_size=10000):
    """DataFrame 탐색적 분석"""
    
    print("=== 데이터셋 기본 정보 ===")
    print(f"전체 레코드 수: {df.count():,}")
    print(f"컬럼 수: {len(df.columns)}")
    
    # 스키마 출력
    print("\n=== 스키마 정보 ===")
    df.printSchema()
    
    # 샘플링으로 성능 최적화
    sample_df = df.sample(0.1).limit(sample_size)
    
    # 수치형 컬럼 통계
    print("\n=== 수치형 컬럼 통계 ===")
    numeric_cols = [field.name for field in df.schema.fields 
                   if field.dataType.typeName() in ['integer', 'long', 'double', 'float']]
    
    if numeric_cols:
        sample_df.select(numeric_cols).describe().show()
    
    # 범주형 컬럼 분포
    print("\n=== 범주형 컬럼 분포 ===")
    categorical_cols = [field.name for field in df.schema.fields 
                       if field.dataType.typeName() == 'string']
    
    for col in categorical_cols[:5]:  # 상위 5개만
        print(f"\n{col} 분포:")
        sample_df.groupBy(col).count().orderBy(desc("count")).show(10)
    
    # 결측값 확인
    print("\n=== 결측값 확인 ===")
    null_counts = []
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            null_counts.append((col, null_count))
    
    if null_counts:
        for col, count in null_counts:
            print(f"{col}: {count:,} ({count/df.count()*100:.1f}%)")
    else:
        print("결측값 없음")

# 사용자 행동 분석 함수
def analyze_user_behavior(events_df):
    """사용자 행동 패턴 분석"""
    
    from datetime import datetime
    
    # RFM 분석 (Recency, Frequency, Monetary)
    current_date = datetime.now()
    
    user_rfm = events_df \
        .groupBy("user_id") \
        .agg(
            # Recency: 마지막 활동으로부터 며칠 지났는지
            datediff(lit(current_date), max("timestamp")).alias("recency_days"),
            # Frequency: 총 이벤트 수
            count("*").alias("frequency"),
            # Monetary: 총 거래액
            sum("value").alias("monetary_value")
        )
    
    # RFM 점수 계산 (1-5 스케일)
    rfm_window = Window.orderBy("recency_days")
    freq_window = Window.orderBy(desc("frequency"))
    mon_window = Window.orderBy(desc("monetary_value"))
    
    user_scores = user_rfm \
        .withColumn("R_score", 
                   (5 - floor(percent_rank().over(rfm_window) * 5)).cast("int")) \
        .withColumn("F_score", 
                   (floor(percent_rank().over(freq_window) * 5) + 1).cast("int")) \
        .withColumn("M_score", 
                   (floor(percent_rank().over(mon_window) * 5) + 1).cast("int"))
    
    # 고객 세그먼트 분류
    user_segments = user_scores \
        .withColumn("customer_segment",
            when((col("R_score") >= 4) & (col("F_score") >= 4) & (col("M_score") >= 4), "Champions")
            .when((col("R_score") >= 3) & (col("F_score") >= 3), "Loyal Customers")
            .when((col("R_score") >= 4) & (col("F_score") < 2), "New Customers")
            .when((col("R_score") < 2) & (col("F_score") >= 3), "At Risk")
            .otherwise("Others")
        )
    
    return user_segments

# 세션 분석 함수
def calculate_user_sessions(events_df):
    """사용자별 세션 분석"""
    
    # 사용자별 세션 윈도우
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    
    # 이전 이벤트와의 시간 차이
    events_with_lag = events_df \
        .withColumn("prev_timestamp", 
                   lag("timestamp", 1).over(user_window)) \
        .withColumn("time_diff_minutes", 
                   (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60)
    
    # 세션 구분 (30분 이상 간격시 새 세션)
    session_events = events_with_lag \
        .withColumn("new_session", 
                   when(col("time_diff_minutes") > 30, 1).otherwise(0)) \
        .withColumn("session_id", 
                   sum("new_session").over(user_window))
    
    # 세션별 메트릭
    session_metrics = session_events \
        .groupBy("user_id", "session_id") \
        .agg(
            count("*").alias("events_per_session"),
            (max("timestamp").cast("long") - min("timestamp").cast("long")).alias("session_duration_seconds"),
            sum("value").alias("session_value"),
            collect_list("event_type").alias("event_sequence")
        )
    
    return session_metrics

if __name__ == "__main__":
    # 실행 예시
    spark = create_spark_session()
    
    # 데이터 로드 예시
    # events_df = load_datalake_data(spark, "my-bucket", "silver", "events")
    # explore_dataframe(events_df)
    
    print("✅ 환경 설정 완료. 노트북에서 함수들을 사용하세요.")
