# Spark DataFrame 윈도우 함수
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

def create_window_sample_data(spark):
    """윈도우 함수 예제용 샘플 데이터 생성"""
    
    # 매출 데이터
    sales_data = [
        ("Alice", "North", "2024-01", 1000),
        ("Alice", "North", "2024-02", 1200),
        ("Alice", "North", "2024-03", 1100),
        ("Bob", "South", "2024-01", 800),
        ("Bob", "South", "2024-02", 900),
        ("Bob", "South", "2024-03", 850),
        ("Charlie", "North", "2024-01", 1300),
        ("Charlie", "North", "2024-02", 1400),
        ("Charlie", "North", "2024-03", 1250),
        ("Diana", "South", "2024-01", 950),
        ("Diana", "South", "2024-02", 1000),
        ("Diana", "South", "2024-03", 1050)
    ]
    
    sales_df = spark.createDataFrame(sales_data, ["salesperson", "region", "month", "sales"])
    
    # 주식 가격 데이터
    stock_data = [
        ("AAPL", "2024-01-01", 150.0),
        ("AAPL", "2024-01-02", 152.0),
        ("AAPL", "2024-01-03", 148.0),
        ("AAPL", "2024-01-04", 155.0),
        ("AAPL", "2024-01-05", 153.0),
        ("GOOGL", "2024-01-01", 2800.0),
        ("GOOGL", "2024-01-02", 2820.0),
        ("GOOGL", "2024-01-03", 2790.0),
        ("GOOGL", "2024-01-04", 2850.0),
        ("GOOGL", "2024-01-05", 2830.0)
    ]
    
    stock_df = spark.createDataFrame(stock_data, ["symbol", "date", "price"])
    stock_df = stock_df.withColumn("date", to_date(col("date")))
    
    return sales_df, stock_df

def ranking_functions(df):
    """랭킹 함수 예제"""
    
    print("=== 랭킹 함수 ===")
    
    # 전체 랭킹 윈도우
    overall_window = Window.orderBy(desc("sales"))
    
    # 지역별 랭킹 윈도우
    region_window = Window.partitionBy("region").orderBy(desc("sales"))
    
    # 월별 지역 랭킹 윈도우
    monthly_region_window = Window.partitionBy("region", "month").orderBy(desc("sales"))
    
    ranking_df = df \
        .withColumn("overall_rank", row_number().over(overall_window)) \
        .withColumn("overall_dense_rank", dense_rank().over(overall_window)) \
        .withColumn("overall_percent_rank", percent_rank().over(overall_window)) \
        .withColumn("region_rank", row_number().over(region_window)) \
        .withColumn("monthly_region_rank", row_number().over(monthly_region_window))
    
    print("랭킹 함수 결과:")
    ranking_df.select(
        "salesperson", "region", "month", "sales",
        "overall_rank", "region_rank", "monthly_region_rank"
    ).orderBy("overall_rank").show()
    
    return ranking_df

def analytical_functions(df):
    """분석 함수 예제"""
    
    print("=== 분석 함수 ===")
    
    # 시간순 윈도우 (월별 정렬)
    time_window = Window.partitionBy("salesperson").orderBy("month")
    
    analytical_df = df \
        .withColumn("prev_month_sales", lag("sales", 1).over(time_window)) \
        .withColumn("next_month_sales", lead("sales", 1).over(time_window)) \
        .withColumn("first_month_sales", first("sales").over(time_window)) \
        .withColumn("last_month_sales", last("sales").over(time_window)) \
        .withColumn("sales_growth", 
                   (col("sales") - col("prev_month_sales")) / col("prev_month_sales") * 100) \
        .withColumn("vs_first_month", 
                   (col("sales") - col("first_month_sales")) / col("first_month_sales") * 100)
    
    print("분석 함수 결과:")
    analytical_df.select(
        "salesperson", "month", "sales",
        "prev_month_sales", "sales_growth", "vs_first_month"
    ).orderBy("salesperson", "month").show()
    
    return analytical_df

def aggregate_window_functions(df):
    """집계 윈도우 함수 예제"""
    
    print("=== 집계 윈도우 함수 ===")
    
    # 누적 집계 윈도우
    cumulative_window = Window.partitionBy("salesperson").orderBy("month")
    
    # 이동 평균 윈도우 (현재 + 이전 2개월)
    moving_avg_window = Window.partitionBy("salesperson").orderBy("month").rowsBetween(-2, 0)
    
    # 범위 기반 윈도우 (전체 기간)
    full_range_window = Window.partitionBy("region")
    
    aggregate_df = df \
        .withColumn("cumulative_sales", sum("sales").over(cumulative_window)) \
        .withColumn("running_avg", avg("sales").over(cumulative_window)) \
        .withColumn("moving_avg_3m", avg("sales").over(moving_avg_window)) \
        .withColumn("region_total_sales", sum("sales").over(full_range_window)) \
        .withColumn("region_avg_sales", avg("sales").over(full_range_window)) \
        .withColumn("sales_vs_region_avg", 
                   col("sales") / col("region_avg_sales")) \
        .withColumn("contribution_to_region", 
                   col("sales") / col("region_total_sales") * 100)
    
    print("집계 윈도우 함수 결과:")
    aggregate_df.select(
        "salesperson", "region", "month", "sales",
        "cumulative_sales", "moving_avg_3m", "contribution_to_region"
    ).orderBy("salesperson", "month").show()
    
    return aggregate_df

def advanced_window_techniques(stock_df):
    """고급 윈도우 기법"""
    
    print("=== 고급 윈도우 기법 ===")
    
    # 주식별 시간순 윈도우
    stock_window = Window.partitionBy("symbol").orderBy("date")
    
    # 이동 평균 윈도우 (5일)
    moving_avg_5d = Window.partitionBy("symbol").orderBy("date").rowsBetween(-4, 0)
    
    # 범위 기반 윈도우 (지난 3일)
    range_3d = Window.partitionBy("symbol").orderBy("date").rangeBetween(-3*24*3600, 0)
    
    advanced_df = stock_df \
        .withColumn("prev_price", lag("price", 1).over(stock_window)) \
        .withColumn("price_change", col("price") - col("prev_price")) \
        .withColumn("price_change_pct", 
                   (col("price") - col("prev_price")) / col("prev_price") * 100) \
        .withColumn("moving_avg_5d", avg("price").over(moving_avg_5d)) \
        .withColumn("price_vs_ma", col("price") / col("moving_avg_5d")) \
        .withColumn("min_price_5d", min("price").over(moving_avg_5d)) \
        .withColumn("max_price_5d", max("price").over(moving_avg_5d)) \
        .withColumn("volatility_5d", 
                   stddev("price").over(moving_avg_5d))
    
    print("고급 윈도우 기법 결과:")
    advanced_df.select(
        "symbol", "date", "price", "price_change_pct",
        "moving_avg_5d", "price_vs_ma", "volatility_5d"
    ).orderBy("symbol", "date").show()
    
    return advanced_df

def window_frame_specifications():
    """윈도우 프레임 명세 가이드"""
    
    print("=== 윈도우 프레임 명세 ===")
    
    frame_examples = {
        "ROWS BETWEEN": {
            "설명": "행 기반 윈도우 프레임",
            "예제": [
                "rowsBetween(-2, 0)  # 이전 2행 ~ 현재 행",
                "rowsBetween(0, 2)   # 현재 행 ~ 다음 2행", 
                "rowsBetween(-1, 1)  # 이전 1행 ~ 다음 1행",
                "rowsBetween(Window.unboundedPreceding, Window.currentRow)  # 처음 ~ 현재"
            ]
        },
        "RANGE BETWEEN": {
            "설명": "값 범위 기반 윈도우 프레임",
            "예제": [
                "rangeBetween(-86400, 0)  # 1일 전 ~ 현재 (초 단위)",
                "rangeBetween(-7*24*3600, 0)  # 1주일 전 ~ 현재",
                "rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)  # 전체"
            ]
        }
    }
    
    for frame_type, info in frame_examples.items():
        print(f"\n📋 {frame_type}:")
        print(f"   {info['설명']}")
        for example in info['예제']:
            print(f"   - {example}")

def window_performance_tips():
    """윈도우 함수 성능 팁"""
    
    print("=== 윈도우 함수 성능 팁 ===")
    
    performance_tips = [
        "1. 파티션 키로 데이터를 적절히 분할",
        "2. 정렬 키 최소화 (필요한 컬럼만)",
        "3. 윈도우 프레임 크기 제한",
        "4. 동일한 윈도우 정의 재사용",
        "5. 필요한 윈도우 함수만 계산",
        "6. 파티션 수 최적화",
        "7. 메모리 설정 조정",
        "8. 스필링 최소화"
    ]
    
    for tip in performance_tips:
        print(f"  ✅ {tip}")
    
    print("\n=== 성능 최적화 예제 ===")
    
    # 윈도우 재사용 예제
    print("\n# 윈도우 정의 재사용:")
    print("window_spec = Window.partitionBy('region').orderBy('date')")
    print("df.withColumn('rank', row_number().over(window_spec))")
    print("  .withColumn('lag_value', lag('value').over(window_spec))")
    
    # 파티션 최적화 예제
    print("\n# 파티션 최적화:")
    print("df.repartition('partition_key').window_operation()")

def common_window_patterns():
    """자주 사용되는 윈도우 패턴"""
    
    print("=== 자주 사용되는 윈도우 패턴 ===")
    
    patterns = {
        "순위 매기기": {
            "용도": "그룹별 상위 N개 선택",
            "코드": "row_number().over(Window.partitionBy('group').orderBy(desc('value')))"
        },
        "이동 평균": {
            "용도": "시계열 데이터 평활화",
            "코드": "avg('value').over(Window.orderBy('date').rowsBetween(-6, 0))"
        },
        "누적 합계": {
            "용도": "누적 실적 계산",
            "코드": "sum('value').over(Window.orderBy('date').rowsBetween(Window.unboundedPreceding, 0))"
        },
        "전기 대비": {
            "용도": "이전 기간과 비교",
            "코드": "lag('value', 1).over(Window.partitionBy('id').orderBy('period'))"
        },
        "백분위 순위": {
            "용도": "상대적 위치 파악",
            "코드": "percent_rank().over(Window.partitionBy('category').orderBy('score'))"
        }
    }
    
    for pattern, info in patterns.items():
        print(f"\n📊 {pattern}:")
        print(f"   용도: {info['용도']}")
        print(f"   코드: {info['코드']}")

if __name__ == "__main__":
    print("✅ 윈도우 함수들이 로드되었습니다.")
    print("   - create_window_sample_data(): 윈도우 함수 예제용 데이터")
    print("   - ranking_functions(): 랭킹 함수 (ROW_NUMBER, RANK, DENSE_RANK)")
    print("   - analytical_functions(): 분석 함수 (LAG, LEAD, FIRST, LAST)")
    print("   - aggregate_window_functions(): 집계 윈도우 함수")
    print("   - advanced_window_techniques(): 고급 윈도우 기법")
    print("   - window_frame_specifications(): 윈도우 프레임 가이드")
    print("   - window_performance_tips(): 성능 최적화 팁")
    print("   - common_window_patterns(): 자주 사용되는 패턴")
