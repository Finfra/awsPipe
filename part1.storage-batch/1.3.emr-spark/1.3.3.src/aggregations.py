# Spark DataFrame 집계 및 그룹화 연산
from pyspark.sql.functions import *
from pyspark.sql.types import *

def basic_aggregations(df):
    """기본 집계 연산"""
    
    print("=== 전체 통계 ===")
    
    # 전체 통계 계산
    agg_exprs = [count("*").alias("total_records")]
    
    # 수치형 컬럼에 대한 통계
    numeric_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType, DecimalType)):
            if field.name not in ["user_id", "event_id", "order_id"]:  # ID 컬럼 제외
                numeric_cols.append(field.name)
                agg_exprs.extend([
                    avg(field.name).alias(f"{field.name}_avg"),
                    min(field.name).alias(f"{field.name}_min"),
                    max(field.name).alias(f"{field.name}_max"),
                    stddev(field.name).alias(f"{field.name}_stddev")
                ])
    
    # 범주형 컬럼에 대한 distinct count
    categorical_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            categorical_cols.append(field.name)
            agg_exprs.append(countDistinct(field.name).alias(f"{field.name}_distinct"))
    
    # 날짜 컬럼에 대한 min/max
    date_cols = [field.name for field in df.schema.fields 
                if isinstance(field.dataType, (DateType, TimestampType))]
    
    for date_col in date_cols:
        agg_exprs.extend([
            min(date_col).alias(f"{date_col}_earliest"),
            max(date_col).alias(f"{date_col}_latest")
        ])
    
    overall_stats = df.agg(*agg_exprs)
    overall_stats.show()
    
    return overall_stats, numeric_cols, categorical_cols, date_cols

def group_by_aggregations(df, group_cols=None):
    """그룹별 집계 연산"""
    
    if group_cols is None:
        # 자동으로 그룹화 컬럼 선택
        group_cols = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType) and field.name not in ["name", "email"]:
                group_cols.append(field.name)
        
        if not group_cols and "user_id" in df.columns:
            group_cols = ["user_id"]
    
    if not group_cols:
        print("⚠️  그룹화할 컬럼이 없습니다.")
        return None
    
    print(f"=== {', '.join(group_cols)} 기준 그룹 집계 ===")
    
    # 기본 집계
    agg_exprs = [count("*").alias("count")]
    
    # 수치형 컬럼 집계
    for field in df.schema.fields:
        if (isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType, DecimalType)) 
            and field.name not in group_cols 
            and field.name not in ["user_id", "event_id", "order_id"]):
            
            agg_exprs.extend([
                sum(field.name).alias(f"{field.name}_sum"),
                avg(field.name).alias(f"{field.name}_avg"),
                min(field.name).alias(f"{field.name}_min"),
                max(field.name).alias(f"{field.name}_max")
            ])
    
    # 범주형 컬럼의 첫 번째 값들 수집
    for field in df.schema.fields:
        if (isinstance(field.dataType, StringType) 
            and field.name not in group_cols 
            and field.name not in ["email", "name"]):
            
            agg_exprs.append(collect_set(field.name).alias(f"{field.name}_values"))
    
    grouped_df = df.groupBy(*group_cols).agg(*agg_exprs)
    
    # 상위 결과만 표시
    print(f"상위 10개 그룹:")
    grouped_df.orderBy(desc("count")).show(10, truncate=False)
    
    return grouped_df

def time_series_aggregations(df):
    """시계열 집계 분석"""
    
    print("=== 시계열 집계 분석 ===")
    
    # 날짜/시간 컬럼 찾기
    date_cols = [field.name for field in df.schema.fields 
                if isinstance(field.dataType, (DateType, TimestampType))]
    
    if not date_cols:
        print("⚠️  날짜/시간 컬럼이 없습니다.")
        return None
    
    date_col = date_cols[0]
    print(f"날짜 컬럼: {date_col}")
    
    # 시계열 집계 함수들
    time_agg_df = df \
        .withColumn("year", year(col(date_col))) \
        .withColumn("month", month(col(date_col))) \
        .withColumn("day", dayofmonth(col(date_col))) \
        .withColumn("hour", hour(col(date_col))) \
        .withColumn("dayofweek", dayofweek(col(date_col)))
    
    # 월별 집계
    monthly_agg = time_agg_df \
        .groupBy("year", "month") \
        .agg(count("*").alias("count")) \
        .orderBy("year", "month")
    
    print("월별 집계:")
    monthly_agg.show()
    
    # 요일별 집계
    dayofweek_agg = time_agg_df \
        .groupBy("dayofweek") \
        .agg(count("*").alias("count")) \
        .withColumn("day_name", 
            when(col("dayofweek") == 1, "Sunday")
            .when(col("dayofweek") == 2, "Monday")
            .when(col("dayofweek") == 3, "Tuesday")
            .when(col("dayofweek") == 4, "Wednesday")
            .when(col("dayofweek") == 5, "Thursday")
            .when(col("dayofweek") == 6, "Friday")
            .when(col("dayofweek") == 7, "Saturday")
        ) \
        .orderBy("dayofweek")
    
    print("요일별 집계:")
    dayofweek_agg.show()
    
    return monthly_agg, dayofweek_agg

def advanced_aggregations(df):
    """고급 집계 함수 활용"""
    
    print("=== 고급 집계 분석 ===")
    
    results = {}
    
    # 백분위수 계산
    numeric_cols = [field.name for field in df.schema.fields 
                   if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType))
                   and field.name not in ["user_id", "event_id", "order_id"]]
    
    if numeric_cols:
        percentile_exprs = []
        for col_name in numeric_cols[:2]:  # 처음 2개만
            percentile_exprs.extend([
                expr(f"percentile_approx({col_name}, 0.25)").alias(f"{col_name}_q1"),
                expr(f"percentile_approx({col_name}, 0.5)").alias(f"{col_name}_median"),
                expr(f"percentile_approx({col_name}, 0.75)").alias(f"{col_name}_q3")
            ])
        
        if percentile_exprs:
            percentile_stats = df.select(*percentile_exprs)
            print("백분위수 통계:")
            percentile_stats.show(truncate=False)
            results['percentiles'] = percentile_stats
    
    # 조건부 집계
    if "value" in df.columns:
        conditional_agg = df.agg(
            sum(when(col("value") > 0, col("value")).otherwise(0)).alias("positive_value_sum"),
            count(when(col("value") > 0, 1)).alias("positive_value_count"),
            sum(when(col("value") == 0, 1).otherwise(0)).alias("zero_value_count"),
            avg(when(col("value") > 0, col("value"))).alias("positive_value_avg")
        )
        
        print("조건부 집계:")
        conditional_agg.show()
        results['conditional'] = conditional_agg
    
    return results

if __name__ == "__main__":
    print("✅ 집계 연산 함수들이 로드되었습니다.")
    print("   - basic_aggregations(): 기본 집계 통계")
    print("   - group_by_aggregations(): 그룹별 집계")
    print("   - time_series_aggregations(): 시계열 집계")
    print("   - advanced_aggregations(): 고급 집계 (백분위수, 조건부)")
