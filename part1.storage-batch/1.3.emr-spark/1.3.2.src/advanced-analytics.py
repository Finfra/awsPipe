# 현재 JSONL 데이터에 맞는 고급 분석 함수들
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

def business_summary_analysis(events_df):
    """비즈니스 요약 분석 - 현재 데이터 구조용"""
    
    # 1. 기본 집계
    summary = events_df \
        .groupBy("category", "event_type") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            expr("percentile_approx(amount, 0.5)").alias("median_amount")
        ) \
        .orderBy(desc("total_amount"))
    
    # 2. 시간대별 분석
    hourly_analysis = events_df \
        .groupBy("hour", "category") \
        .agg(
            count("*").alias("events_count"),
            sum("amount").alias("hourly_revenue")
        ) \
        .orderBy("hour")
    
    return {
        'summary': summary,
        'hourly_analysis': hourly_analysis
    }

def user_behavior_analysis(events_df):
    """사용자 행동 분석"""
    
    # 사용자별 윈도우
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    
    # 사용자 행동 패턴
    user_behavior = events_df \
        .withColumn("event_sequence", row_number().over(user_window)) \
        .withColumn("prev_event", lag("event_type", 1).over(user_window)) \
        .withColumn("prev_amount", lag("amount", 1).over(user_window)) \
        .withColumn("time_diff_minutes", 
                   (unix_timestamp("timestamp") - 
                    unix_timestamp(lag("timestamp", 1).over(user_window))) / 60)
    
    # 사용자별 집계
    user_summary = events_df \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_events"),
            sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction"),
            countDistinct("category").alias("categories_used"),
            countDistinct("event_type").alias("event_types"),
            collect_set("event_type").alias("event_pattern")
        )
    
    return {
        'user_behavior': user_behavior,
        'user_summary': user_summary
    }

def time_series_analysis(events_df):
    """시계열 분석"""
    
    # 시간별 트렌드
    hourly_trend = events_df \
        .groupBy("year", "month", "day", "hour") \
        .agg(
            count("*").alias("events"),
            sum("amount").alias("revenue"),
            countDistinct("user_id").alias("active_users")
        ) \
        .orderBy("year", "month", "day", "hour")
    
    # 이동평균 (3시간 윈도우)
    time_window = Window.orderBy("year", "month", "day", "hour").rowsBetween(-2, 0)
    
    trend_with_ma = hourly_trend \
        .withColumn("events_ma3", avg("events").over(time_window)) \
        .withColumn("revenue_ma3", avg("revenue").over(time_window))
    
    return trend_with_ma

def event_sequence_analysis(events_df):
    """이벤트 시퀀스 분석"""
    
    # 사용자별 이벤트 시퀀스
    user_window = Window.partitionBy("user_id").orderBy("timestamp")
    
    sequence_df = events_df \
        .withColumn("next_event", lead("event_type", 1).over(user_window)) \
        .withColumn("event_pair", 
                   concat(col("event_type"), lit("->"), col("next_event"))) \
        .filter(col("next_event").isNotNull())
    
    # 이벤트 전환 패턴
    transition_patterns = sequence_df \
        .groupBy("event_pair") \
        .count() \
        .orderBy(desc("count"))
    
    return transition_patterns

def category_performance_analysis(events_df):
    """카테고리 성과 분석"""
    
    # 카테고리별 성과 메트릭
    category_performance = events_df \
        .groupBy("category") \
        .agg(
            count("*").alias("total_transactions"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("user_id").alias("unique_customers"),
            # 구매 전환율 (purchase 이벤트 비율)
            (sum(when(col("event_type") == "purchase", 1).otherwise(0)) / count("*") * 100)
            .alias("purchase_conversion_rate")
        ) \
        .withColumn("revenue_per_customer", 
                   col("total_revenue") / col("unique_customers"))
    
    # 카테고리별 랭킹
    category_window = Window.orderBy(desc("total_revenue"))
    
    ranked_categories = category_performance \
        .withColumn("revenue_rank", row_number().over(category_window)) \
        .withColumn("revenue_percentile", percent_rank().over(category_window))
    
    return ranked_categories

def anomaly_detection(events_df):
    """이상치 탐지 - 금액 기준"""
    
    # 통계값 계산
    stats = events_df.agg(
        avg("amount").alias("mean_amount"),
        stddev("amount").alias("std_amount")
    ).collect()[0]
    
    mean_val = stats["mean_amount"]
    std_val = stats["std_amount"]
    
    # Z-score 기반 이상치 탐지
    anomaly_df = events_df \
        .withColumn("z_score", 
                   abs((col("amount") - lit(mean_val)) / lit(std_val))) \
        .withColumn("is_anomaly", col("z_score") > 2.5) \
        .withColumn("anomaly_type",
                   when(col("amount") > (lit(mean_val) + lit(std_val) * 2.5), "high")
                   .when(col("amount") < (lit(mean_val) - lit(std_val) * 2.5), "low")
                   .otherwise("normal"))
    
    return anomaly_df

def create_pivot_analysis(events_df):
    """피벗 분석"""
    
    # 카테고리별 x 이벤트타입별 피벗
    pivot_df = events_df \
        .groupBy("category") \
        .pivot("event_type") \
        .agg(
            count("*").alias("count"),
            sum("amount").alias("total_amount")
        )
    
    return pivot_df

def calculate_conversion_funnel(events_df):
    """전환 퍼널 분석"""
    
    # 이벤트 타입별 사용자 수
    funnel_data = events_df \
        .groupBy("event_type") \
        .agg(countDistinct("user_id").alias("unique_users")) \
        .orderBy(desc("unique_users"))
    
    # 전환율 계산 (가정: login -> view -> purchase 순서)
    event_counts = {row["event_type"]: row["unique_users"] for row in funnel_data.collect()}
    
    print("🔄 전환 퍼널 분석:")
    for event_type, count in event_counts.items():
        if "login" in event_counts:
            conversion_rate = (count / event_counts["login"] * 100) if event_counts["login"] > 0 else 0
            print(f"   {event_type}: {count:,} 명 ({conversion_rate:.1f}%)")
    
    return funnel_data

def daily_cohort_simple(events_df):
    """간단한 일별 코호트 분석"""
    
    # 사용자별 첫 구매일
    first_purchase = events_df \
        .filter(col("event_type") == "purchase") \
        .groupBy("user_id") \
        .agg(min("timestamp").alias("first_purchase_date"))
    
    # 코호트 조인
    cohort_df = events_df \
        .join(first_purchase, "user_id") \
        .withColumn("days_since_first", 
                   datediff(to_date(col("timestamp")), to_date(col("first_purchase_date"))))
    
    # 코호트 집계
    cohort_analysis = cohort_df \
        .groupBy("first_purchase_date", "days_since_first") \
        .agg(
            countDistinct("user_id").alias("active_users"),
            sum("amount").alias("cohort_revenue")
        ) \
        .orderBy("first_purchase_date", "days_since_first")
    
    return cohort_analysis

def quick_insights(events_df):
    """빠른 인사이트 추출"""
    
    print("🔍 빠른 데이터 인사이트")
    print("="*40)
    
    # 1. 기본 통계
    total_records = events_df.count()
    total_users = events_df.select("user_id").distinct().count()
    total_revenue = events_df.agg(sum("amount")).collect()[0][0]
    
    print(f"📊 총 레코드: {total_records:,}")
    print(f"👥 총 사용자: {total_users:,}")
    print(f"💰 총 거래액: ${total_revenue:,.2f}")
    print(f"📈 사용자당 평균 거래액: ${total_revenue/total_users:.2f}")
    
    # 2. 상위 카테고리
    print(f"\n🏆 상위 카테고리 (거래액 기준):")
    events_df.groupBy("category") \
        .agg(sum("amount").alias("revenue")) \
        .orderBy(desc("revenue")) \
        .show(5, truncate=False)
    
    # 3. 피크 시간대
    print(f"\n⏰ 시간대별 활동:")
    events_df.groupBy("hour") \
        .count() \
        .orderBy(desc("count")) \
        .show(5, truncate=False)

# 모든 분석을 한번에 실행
def run_complete_analysis(events_df):
    """전체 분석 실행"""
    
    print("🚀 전체 분석 시작...")
    
    # 1. 빠른 인사이트
    quick_insights(events_df)
    
    # 2. 비즈니스 분석
    business_results = business_summary_analysis(events_df)
    print(f"\n📋 카테고리별 성과:")
    business_results['summary'].show(10, truncate=False)
    
    # 3. 이상치 탐지
    anomalies = anomaly_detection(events_df)
    anomaly_count = anomalies.filter(col("is_anomaly")).count()
    print(f"\n⚠️  탐지된 이상치: {anomaly_count}개")
    
    # 4. 전환 퍼널
    calculate_conversion_funnel(events_df)
    
    # 5. 이벤트 시퀀스
    print(f"\n🔗 주요 이벤트 전환 패턴:")
    event_sequence_analysis(events_df).show(10, truncate=False)
    
    return {
        'business_analysis': business_results,
        'anomalies': anomalies,
        'user_behavior': user_behavior_analysis(events_df)
    }

if __name__ == "__main__":
    print("✅ 현재 데이터 구조에 맞는 고급 분석 함수들이 로드되었습니다.")
    print("   - business_summary_analysis(): 비즈니스 요약")
    print("   - user_behavior_analysis(): 사용자 행동 분석")
    print("   - time_series_analysis(): 시계열 분석")
    print("   - anomaly_detection(): 이상치 탐지")
    print("   - calculate_conversion_funnel(): 전환 퍼널")
    print("   - quick_insights(): 빠른 인사이트")
    print("   - run_complete_analysis(): 전체 분석 실행")