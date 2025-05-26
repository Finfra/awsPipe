# Spark DataFrame 기본 연산 함수들
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def create_optimized_spark_session(app_name="DataFrame-API-Guide"):
    """최적화된 Spark 세션 생성"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print(f"✅ Spark Session 생성 완료")
    print(f"   App Name: {app_name}")
    print(f"   Spark Version: {spark.version}")
    print(f"   Default Parallelism: {spark.sparkContext.defaultParallelism}")
    
    return spark

def define_common_schemas():
    """자주 사용되는 스키마 정의"""
    
    # 사용자 스키마
    user_schema = StructType([
        StructField("user_id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("signup_date", DateType(), True),
        StructField("preferences", MapType(StringType(), StringType()), True),
        StructField("tags", ArrayType(StringType()), True)
    ])
    
    # 이벤트 스키마
    event_schema = StructType([
        StructField("event_id", LongType(), False),
        StructField("user_id", LongType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])
    
    # 주문 스키마
    order_schema = StructType([
        StructField("order_id", LongType(), False),
        StructField("user_id", LongType(), False),
        StructField("product_id", LongType(), False),
        StructField("amount", DecimalType(10, 2), False),
        StructField("order_date", DateType(), False),
        StructField("status", StringType(), False)
    ])
    
    return {
        'user': user_schema,
        'event': event_schema,
        'order': order_schema
    }

def load_data_with_schema(spark, file_path, schema_name, file_format="parquet"):
    """스키마를 사용한 데이터 로드"""
    
    schemas = define_common_schemas()
    
    if schema_name not in schemas:
        raise ValueError(f"Unknown schema: {schema_name}. Available: {list(schemas.keys())}")
    
    schema = schemas[schema_name]
    
    try:
        if file_format.lower() == "parquet":
            df = spark.read.schema(schema).parquet(file_path)
        elif file_format.lower() == "json":
            df = spark.read.schema(schema).option("multiline", "true").json(file_path)
        elif file_format.lower() == "csv":
            df = spark.read.schema(schema).option("header", "true").csv(file_path)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        print(f"✅ 데이터 로드 완료: {file_path}")
        print(f"   레코드 수: {df.count():,}")
        print(f"   스키마: {schema_name}")
        
        return df
        
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        return None

def basic_dataframe_operations(df):
    """기본 DataFrame 연산"""
    
    print("=== 기본 정보 ===")
    print(f"레코드 수: {df.count():,}")
    print(f"컬럼 수: {len(df.columns)}")
    print(f"파티션 수: {df.rdd.getNumPartitions()}")
    
    print("\n=== 컬럼 정보 ===")
    for field in df.schema.fields:
        nullable = "nullable" if field.nullable else "not null"
        print(f"  {field.name}: {field.dataType} ({nullable})")
    
    print("\n=== 데이터 미리보기 ===")
    df.show(5, truncate=False)
    
    print("\n=== 기본 통계 ===")
    df.describe().show()
    
    # 기본 연산 예제
    operations = {}
    
    # 컬럼 선택
    if "user_id" in df.columns and "name" in df.columns:
        operations['selected'] = df.select("user_id", "name")
    
    # 필터링
    if "age" in df.columns:
        operations['filtered'] = df.filter(col("age") >= 18)
    
    # 정렬
    if "timestamp" in df.columns:
        operations['sorted'] = df.orderBy(desc("timestamp"))
    elif "signup_date" in df.columns:
        operations['sorted'] = df.orderBy(desc("signup_date"))
    
    # 중복 제거
    if "email" in df.columns:
        operations['unique'] = df.dropDuplicates(["email"])
    
    return operations

def advanced_transformations(df):
    """고급 DataFrame 변환"""
    
    enhanced_df = df
    
    # 나이 그룹 추가 (age 컬럼이 있는 경우)
    if "age" in df.columns:
        enhanced_df = enhanced_df.withColumn("age_group", 
            when(col("age") < 18, "minor")
            .when(col("age") < 65, "adult")
            .otherwise("senior")
        )
    
    # 이메일 도메인 추출 (email 컬럼이 있는 경우)
    if "email" in df.columns:
        enhanced_df = enhanced_df.withColumn("email_domain", 
            regexp_extract(col("email"), r"@(.+)", 1)
        )
    
    # 날짜 관련 변환
    date_cols = [col for col in df.columns if "date" in col.lower() or col == "timestamp"]
    for date_col in date_cols:
        enhanced_df = enhanced_df \
            .withColumn(f"{date_col}_year", year(col(date_col))) \
            .withColumn(f"{date_col}_month", month(col(date_col))) \
            .withColumn(f"{date_col}_dayofweek", dayofweek(col(date_col)))
    
    # 복잡한 데이터 타입 처리
    for field in df.schema.fields:
        if isinstance(field.dataType, MapType):
            enhanced_df = enhanced_df \
                .withColumn(f"{field.name}_keys", map_keys(col(field.name))) \
                .withColumn(f"{field.name}_values", map_values(col(field.name)))
        
        elif isinstance(field.dataType, ArrayType):
            enhanced_df = enhanced_df \
                .withColumn(f"{field.name}_size", size(col(field.name)))
    
    # 수치형 변환
    numeric_cols = [field.name for field in df.schema.fields 
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))]
    
    for col_name in numeric_cols[:3]:  # 처음 3개만
        if col_name != "user_id" and col_name != "event_id":  # ID 컬럼 제외
            enhanced_df = enhanced_df \
                .withColumn(f"{col_name}_log", log(col(col_name) + 1)) \
                .withColumn(f"{col_name}_sqrt", sqrt(abs(col(col_name))))
    
    return enhanced_df

def create_sample_data(spark):
    """샘플 데이터 생성 (테스트용)"""
    
    # 사용자 데이터
    users_data = [
        (1, "Alice Johnson", "alice@gmail.com", 28, "2023-01-15"),
        (2, "Bob Smith", "bob@yahoo.com", 35, "2023-02-20"),
        (3, "Charlie Brown", "charlie@gmail.com", 42, "2023-01-10"),
        (4, "Diana Wilson", "diana@hotmail.com", 31, "2023-03-05"),
        (5, "Eve Davis", "eve@gmail.com", 26, "2023-02-28")
    ]
    
    users_schema = ["user_id", "name", "email", "age", "signup_date"]
    users_df = spark.createDataFrame(users_data, users_schema)
    users_df = users_df.withColumn("signup_date", to_date(col("signup_date")))
    
    # 이벤트 데이터
    events_data = [
        (101, 1, "login", "2024-01-01 10:00:00", 0.0),
        (102, 1, "view", "2024-01-01 10:05:00", 0.0),
        (103, 1, "purchase", "2024-01-01 10:30:00", 99.99),
        (104, 2, "login", "2024-01-01 11:00:00", 0.0),
        (105, 2, "view", "2024-01-01 11:10:00", 0.0),
        (106, 3, "login", "2024-01-01 12:00:00", 0.0),
        (107, 3, "purchase", "2024-01-01 12:30:00", 149.99),
        (108, 1, "view", "2024-01-02 09:00:00", 0.0)
    ]
    
    events_schema = ["event_id", "user_id", "event_type", "timestamp", "value"]
    events_df = spark.createDataFrame(events_data, events_schema)
    events_df = events_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    return users_df, events_df

def dataframe_info_summary(df, df_name="DataFrame"):
    """DataFrame 정보 요약"""
    
    print(f"\n{'='*50}")
    print(f"{df_name} 정보 요약")
    print(f"{'='*50}")
    
    # 기본 정보
    record_count = df.count()
    column_count = len(df.columns)
    partition_count = df.rdd.getNumPartitions()
    
    print(f"📊 레코드 수: {record_count:,}")
    print(f"📋 컬럼 수: {column_count}")
    print(f"🗂️  파티션 수: {partition_count}")
    
    # 스키마 정보
    print(f"\n📝 스키마:")
    for field in df.schema.fields:
        print(f"   {field.name}: {field.dataType}")
    
    # 샘플 데이터
    print(f"\n🔍 샘플 데이터:")
    df.show(3, truncate=True)
    
    # 결측값 체크
    print(f"\n❓ 결측값 체크:")
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"   {col_name}: {null_count:,} ({null_count/record_count*100:.1f}%)")
    
    return {
        'record_count': record_count,
        'column_count': column_count,
        'partition_count': partition_count
    }

if __name__ == "__main__":
    # 샘플 실행
    spark = create_optimized_spark_session()
    
    # 샘플 데이터 생성
    users_df, events_df = create_sample_data(spark)
    
    # 기본 연산 테스트
    print("=== 사용자 데이터 분석 ===")
    user_info = dataframe_info_summary(users_df, "Users")
    user_ops = basic_dataframe_operations(users_df)
    
    print("\n=== 이벤트 데이터 분석 ===")
    event_info = dataframe_info_summary(events_df, "Events")
    event_ops = basic_dataframe_operations(events_df)
    
    print("✅ 기본 연산 함수 로드 완료")
