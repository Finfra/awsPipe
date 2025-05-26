# Spark DataFrame 조인 연산
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_join_sample_data(spark):
    """조인 예제용 샘플 데이터 생성"""
    
    # 사용자 데이터
    users_data = [
        (1, "Alice", "alice@gmail.com", 28),
        (2, "Bob", "bob@yahoo.com", 35),
        (3, "Charlie", "charlie@gmail.com", 42),
        (4, "Diana", "diana@hotmail.com", 31)
    ]
    users_df = spark.createDataFrame(users_data, ["user_id", "name", "email", "age"])
    
    # 주문 데이터
    orders_data = [
        (101, 1, 100.0, "2024-01-01"),
        (102, 1, 150.0, "2024-01-02"),
        (103, 2, 200.0, "2024-01-01"),
        (104, 5, 75.0, "2024-01-03"),  # user_id 5는 users에 없음
        (105, 3, 300.0, "2024-01-04")
    ]
    orders_df = spark.createDataFrame(orders_data, ["order_id", "user_id", "amount", "order_date"])
    
    # 상품 데이터
    products_data = [
        (1001, "Laptop", "Electronics", 999.99),
        (1002, "Phone", "Electronics", 599.99),
        (1003, "Book", "Education", 29.99),
        (1004, "Headphones", "Electronics", 199.99)
    ]
    products_df = spark.createDataFrame(products_data, ["product_id", "name", "category", "price"])
    
    # 주문 상세 데이터
    order_details_data = [
        (101, 1001, 1),
        (102, 1002, 1),
        (103, 1001, 1),
        (103, 1004, 1),  # 주문 103에 여러 상품
        (104, 1003, 2),
        (105, 1001, 1)
    ]
    order_details_df = spark.createDataFrame(order_details_data, ["order_id", "product_id", "quantity"])
    
    return users_df, orders_df, products_df, order_details_df

def basic_join_operations(users_df, orders_df):
    """기본 조인 연산 예제"""
    
    print("=== 기본 조인 연산 ===")
    
    join_results = {}
    
    # 1. Inner Join (기본)
    inner_join = users_df.join(orders_df, "user_id")
    print("1. Inner Join (매칭되는 데이터만):")
    inner_join.select("user_id", "name", "order_id", "amount").show()
    join_results['inner'] = inner_join
    
    # 2. Left Join
    left_join = users_df.join(orders_df, "user_id", "left")
    print("2. Left Join (모든 사용자 + 매칭되는 주문):")
    left_join.select("user_id", "name", "order_id", "amount").show()
    join_results['left'] = left_join
    
    # 3. Right Join
    right_join = users_df.join(orders_df, "user_id", "right")
    print("3. Right Join (모든 주문 + 매칭되는 사용자):")
    right_join.select("user_id", "name", "order_id", "amount").show()
    join_results['right'] = right_join
    
    # 4. Full Outer Join
    full_join = users_df.join(orders_df, "user_id", "full")
    print("4. Full Outer Join (모든 데이터):")
    full_join.select("user_id", "name", "order_id", "amount").show()
    join_results['full'] = full_join
    
    # 5. Left Semi Join (EXISTS와 유사)
    semi_join = users_df.join(orders_df, "user_id", "left_semi")
    print("5. Left Semi Join (주문이 있는 사용자만):")
    semi_join.show()
    join_results['semi'] = semi_join
    
    # 6. Left Anti Join (NOT EXISTS와 유사)
    anti_join = users_df.join(orders_df, "user_id", "left_anti")
    print("6. Left Anti Join (주문이 없는 사용자만):")
    anti_join.show()
    join_results['anti'] = anti_join
    
    return join_results

def complex_join_conditions(users_df, orders_df):
    """복잡한 조인 조건 예제"""
    
    print("=== 복잡한 조인 조건 ===")
    
    # 샘플 데이터에 추가 컬럼 생성
    orders_with_date = orders_df.withColumn("order_date", to_date(col("order_date")))
    users_with_signup = users_df.withColumn("signup_date", lit("2023-01-01").cast(DateType()))
    
    # 1. 복합 조건 조인
    complex_join = users_with_signup.join(
        orders_with_date,
        (users_with_signup.user_id == orders_with_date.user_id) & 
        (orders_with_date.order_date >= users_with_signup.signup_date)
    )
    
    print("1. 복합 조건 조인 (사용자 ID + 가입일 이후 주문):")
    complex_join.select("name", "order_id", "amount", "order_date", "signup_date").show()
    
    # 2. 범위 조인 (나이 기반)
    age_ranges_data = [
        ("Youth", 0, 25),
        ("Adult", 26, 40),
        ("Senior", 41, 100)
    ]
    age_ranges_df = spark.createDataFrame(age_ranges_data, ["age_group", "min_age", "max_age"])
    
    age_join = users_df.join(
        age_ranges_df,
        (users_df.age >= age_ranges_df.min_age) & 
        (users_df.age <= age_ranges_df.max_age)
    )
    
    print("2. 범위 조인 (나이 그룹):")
    age_join.select("name", "age", "age_group").show()
    
    # 3. 브로드캐스트 조인 (작은 테이블 최적화)
    broadcast_join = users_df.join(broadcast(age_ranges_df), 
                                  (users_df.age >= age_ranges_df.min_age) & 
                                  (users_df.age <= age_ranges_df.max_age))
    
    print("3. 브로드캐스트 조인:")
    broadcast_join.select("name", "age", "age_group").show()
    
    return complex_join, age_join, broadcast_join

def multi_table_joins(users_df, orders_df, products_df, order_details_df):
    """다중 테이블 조인"""
    
    print("=== 다중 테이블 조인 ===")
    
    # 4개 테이블 조인: 사용자 -> 주문 -> 주문상세 -> 상품
    complete_join = users_df \
        .join(orders_df, "user_id") \
        .join(order_details_df, "order_id") \
        .join(products_df, "product_id")
    
    print("완전한 주문 정보 (4개 테이블 조인):")
    complete_join.select(
        "name", "order_id", "amount", 
        products_df.name.alias("product_name"), 
        "category", "quantity"
    ).show()
    
    # 집계와 함께 조인
    order_summary = complete_join \
        .groupBy("user_id", users_df.name.alias("user_name")) \
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_spent"),
            countDistinct("product_id").alias("unique_products"),
            collect_list(products_df.name).alias("purchased_products")
        )
    
    print("사용자별 주문 요약:")
    order_summary.show(truncate=False)
    
    return complete_join, order_summary

def join_performance_optimization():
    """조인 성능 최적화 기법"""
    
    print("=== 조인 성능 최적화 ===")
    
    optimization_tips = [
        "1. 브로드캐스트 조인 사용 (작은 테이블 < 10MB)",
        "2. 조인 키로 사전 파티셔닝",
        "3. 불필요한 컬럼 제거 후 조인",
        "4. 조인 전 필터링으로 데이터 크기 줄이기",
        "5. 조인 힌트 사용",
        "6. 적절한 파티션 수 설정",
        "7. 조인 순서 최적화 (작은 테이블 먼저)"
    ]
    
    for tip in optimization_tips:
        print(f"  {tip}")
    
    # 최적화 예제 코드
    print("\n=== 최적화 예제 ===")
    
    # 브로드캐스트 조인 예제
    print("# 브로드캐스트 조인")
    print("large_df.join(broadcast(small_df), 'key')")
    
    # 조인 힌트 예제
    print("\n# 조인 힌트")
    print("df1.hint('broadcast').join(df2, 'key')")
    
    # 파티션 최적화 예제
    print("\n# 파티션 최적화")
    print("df1.repartition('join_key').join(df2.repartition('join_key'), 'join_key')")
    
    # 선택적 조인 예제
    print("\n# 선택적 조인 (필요한 컬럼만)")
    print("df1.select('key', 'col1').join(df2.select('key', 'col2'), 'key')")

def join_troubleshooting():
    """조인 문제 해결 가이드"""
    
    print("=== 조인 문제 해결 가이드 ===")
    
    troubleshooting_guide = {
        "데이터 스큐 문제": [
            "- 특정 키에 데이터가 집중되는 현상",
            "- 해결: 솔트 키 추가, 파티션 재분배",
            "- 예방: 조인 전 데이터 분포 확인"
        ],
        "메모리 부족": [
            "- 큰 테이블 조인 시 발생",
            "- 해결: 브로드캐스트 조인, 파티션 수 증가",
            "- 예방: 조인 전 데이터 크기 확인"
        ],
        "카디널리티 폭발": [
            "- 1:N 조인에서 결과 크기 급증",
            "- 해결: 조인 전 중복 제거, 필터링",
            "- 예방: 조인 키 유니크성 확인"
        ],
        "성능 저하": [
            "- 비효율적인 조인 순서",
            "- 해결: 작은 테이블 먼저 조인",
            "- 예방: 실행 계획 분석"
        ]
    }
    
    for problem, solutions in troubleshooting_guide.items():
        print(f"\n📋 {problem}:")
        for solution in solutions:
            print(f"  {solution}")

def join_best_practices():
    """조인 모범 사례"""
    
    print("=== 조인 모범 사례 ===")
    
    best_practices = [
        "1. 조인 전 필요한 컬럼만 선택",
        "2. 조인 전 데이터 필터링",
        "3. 작은 테이블에 브로드캐스트 조인 사용",
        "4. 조인 키 데이터 타입 일치 확인",
        "5. NULL 값 처리 전략 명확히",
        "6. 조인 결과 크기 예상 및 모니터링",
        "7. 실행 계획 분석으로 최적화",
        "8. 적절한 파티션 수 설정"
    ]
    
    for practice in best_practices:
        print(f"  ✅ {practice}")

if __name__ == "__main__":
    print("✅ 조인 연산 함수들이 로드되었습니다.")
    print("   - create_join_sample_data(): 조인 예제용 샘플 데이터")
    print("   - basic_join_operations(): 기본 조인 연산")
    print("   - complex_join_conditions(): 복잡한 조인 조건")
    print("   - multi_table_joins(): 다중 테이블 조인")
    print("   - join_performance_optimization(): 성능 최적화 기법")
    print("   - join_troubleshooting(): 문제 해결 가이드")
    print("   - join_best_practices(): 모범 사례")
