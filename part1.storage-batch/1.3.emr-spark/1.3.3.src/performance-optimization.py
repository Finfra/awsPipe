# Spark DataFrame 성능 최적화
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel

def dataframe_optimization_techniques(df):
    """DataFrame 최적화 기법"""
    
    print("=== DataFrame 최적화 기법 ===")
    
    optimization_results = {}
    
    # 1. 파티셔닝 최적화
    current_partitions = df.rdd.getNumPartitions()
    record_count = df.count()
    
    # 적절한 파티션 수 계산 (파티션당 100K-1M 레코드)
    optimal_partitions = max(min(record_count // 500000, 200), 1)
    
    print(f"📊 파티션 정보:")
    print(f"   현재 파티션: {current_partitions}")
    print(f"   레코드 수: {record_count:,}")
    print(f"   권장 파티션: {optimal_partitions}")
    
    if optimal_partitions < current_partitions:
        optimized_df = df.coalesce(optimal_partitions)
        optimization_results['partitioned'] = optimized_df
        print(f"   → Coalesce로 {optimal_partitions}개 파티션으로 축소")
    elif optimal_partitions > current_partitions:
        optimized_df = df.repartition(optimal_partitions)
        optimization_results['partitioned'] = optimized_df
        print(f"   → Repartition으로 {optimal_partitions}개 파티션으로 확장")
    else:
        optimized_df = df
        optimization_results['partitioned'] = optimized_df
        print(f"   → 현재 파티션 수가 적절함")
    
    return optimization_results

def caching_strategies(df):
    """캐싱 전략"""
    
    print("=== 캐싱 전략 ===")
    
    caching_results = {}
    
    # 1. 기본 메모리 캐시
    memory_cached_df = df.cache()
    memory_cached_df.count()  # 캐시 워밍업
    caching_results['memory'] = memory_cached_df
    print("✅ 메모리 캐시 적용")
    
    # 2. 메모리 + 디스크 캐시
    memory_disk_df = df.persist(StorageLevel.MEMORY_AND_DISK)
    caching_results['memory_disk'] = memory_disk_df
    print("✅ 메모리 + 디스크 캐시 적용")
    
    # 3. 직렬화된 메모리 캐시
    serialized_df = df.persist(StorageLevel.MEMORY_ONLY_SER)
    caching_results['serialized'] = serialized_df
    print("✅ 직렬화된 메모리 캐시 적용")
    
    print("\n📋 캐시 사용 가이드라인:")
    print("   - 여러 번 사용되는 DataFrame만 캐시")
    print("   - 변환 후 캐시 (필터링, 조인 후)")
    print("   - 메모리 부족시 MEMORY_AND_DISK 사용")
    print("   - 큰 DataFrame은 직렬화 캐시 고려")
    
    return caching_results

def schema_optimization(df):
    """스키마 최적화"""
    
    print("=== 스키마 최적화 ===")
    
    optimized_df = df
    changes_made = []
    
    # 1. 데이터 타입 최적화
    for field in df.schema.fields:
        
        # Long을 Int로 변환 (값이 Int 범위 내인 경우)
        if field.dataType == LongType() and field.name not in ["user_id", "event_id"]:
            try:
                max_val = df.agg(max(field.name)).collect()[0][0]
                if max_val and max_val < 2147483647:
                    optimized_df = optimized_df.withColumn(
                        field.name, 
                        col(field.name).cast(IntegerType())
                    )
                    changes_made.append(f"{field.name}: Long → Int")
            except:
                pass
        
        # String을 Boolean으로 변환
        elif field.dataType == StringType() and field.name.startswith("is_"):
            optimized_df = optimized_df.withColumn(
                field.name,
                when(col(field.name).isin(["true", "True", "1", "yes", "Y"]), True)
                .when(col(field.name).isin(["false", "False", "0", "no", "N"]), False)
                .otherwise(None).cast(BooleanType())
            )
            changes_made.append(f"{field.name}: String → Boolean")
    
    # 2. 컬럼 순서 최적화
    priority_columns = []
    other_columns = []
    
    for col_name in optimized_df.columns:
        if col_name in ["user_id", "timestamp", "event_type", "value", "date"]:
            priority_columns.append(col_name)
        else:
            other_columns.append(col_name)
    
    if priority_columns:
        optimized_df = optimized_df.select(*(priority_columns + other_columns))
        changes_made.append("컬럼 순서 최적화 적용")
    
    print("🔧 스키마 최적화 변경사항:")
    for change in changes_made:
        print(f"   - {change}")
    
    return optimized_df

def performance_monitoring_and_tuning():
    """성능 모니터링 및 튜닝"""
    
    print("=== 성능 모니터링 및 튜닝 ===")
    
    # 성능 병목 지점
    bottlenecks = {
        "CPU 집약적": {
            "증상": "CPU 사용률 높음, 긴 처리 시간",
            "해결책": ["executor 수 증가", "복잡한 연산 최적화", "파티션 수 증가"]
        },
        "메모리 부족": {
            "증상": "OOM 에러, 스필링 발생",
            "해결책": ["executor 메모리 증가", "파티션 수 증가", "캐시 최적화"]
        },
        "I/O 병목": {
            "증상": "디스크 읽기/쓰기 지연",
            "해결책": ["파일 포맷 최적화", "압축 사용", "파티셔닝 개선"]
        },
        "네트워크 병목": {
            "증상": "셔플 단계에서 지연",
            "해결책": ["브로드캐스트 조인", "파티션 키 최적화", "데이터 로컬리티 향상"]
        }
    }
    
    print("🔍 성능 병목 지점 및 해결책:")
    for bottleneck, info in bottlenecks.items():
        print(f"\n{bottleneck}:")
        print(f"   증상: {info['증상']}")
        print(f"   해결책: {', '.join(info['해결책'])}")

def spark_ui_analysis_guide():
    """Spark UI 분석 가이드"""
    
    print("=== Spark UI 분석 가이드 ===")
    
    ui_tabs = {
        "Jobs": {
            "확인 사항": "작업 실행 시간, 실패한 작업",
            "최적화 포인트": "긴 실행 시간의 작업 분석"
        },
        "Stages": {
            "확인 사항": "스테이지별 실행 시간, 셔플 데이터 크기",
            "최적화 포인트": "셔플이 많은 스테이지 최적화"
        },
        "Storage": {
            "확인 사항": "캐시된 데이터 크기, 메모리 사용률",
            "최적화 포인트": "불필요한 캐시 제거"
        },
        "Environment": {
            "확인 사항": "Spark 설정값",
            "최적화 포인트": "설정 튜닝"
        },
        "Executors": {
            "확인 사항": "Executor별 메모리, CPU 사용률",
            "최적화 포인트": "리소스 불균형 해결"
        },
        "SQL": {
            "확인 사항": "쿼리 실행 계획, 실행 시간",
            "최적화 포인트": "비효율적인 쿼리 개선"
        }
    }
    
    print("📊 Spark UI 탭별 분석 포인트:")
    for tab, info in ui_tabs.items():
        print(f"\n{tab} 탭:")
        print(f"   확인 사항: {info['확인 사항']}")
        print(f"   최적화 포인트: {info['최적화 포인트']}")

def execution_plan_analysis():
    """실행 계획 분석"""
    
    print("=== 실행 계획 분석 ===")
    
    # 실행 계획 분석 방법
    analysis_methods = {
        "explain()": {
            "용도": "기본 실행 계획 확인",
            "사용법": "df.explain()",
            "정보": "물리적 실행 계획"
        },
        "explain(True)": {
            "용도": "상세 실행 계획 확인",
            "사용법": "df.explain(True)",
            "정보": "논리적 + 물리적 계획"
        },
        "explain('cost')": {
            "용도": "비용 기반 실행 계획",
            "사용법": "df.explain('cost')",
            "정보": "각 단계별 예상 비용"
        }
    }
    
    print("🔍 실행 계획 분석 방법:")
    for method, info in analysis_methods.items():
        print(f"\n{method}:")
        print(f"   용도: {info['용도']}")
        print(f"   사용법: {info['사용법']}")
        print(f"   정보: {info['정보']}")
    
    # 실행 계획에서 확인할 사항
    print("\n📋 실행 계획 확인 사항:")
    check_points = [
        "스캔되는 파일 수와 크기",
        "필터 조건의 푸시다운 여부",
        "조인 타입 (BroadcastHashJoin vs SortMergeJoin)",
        "셔플 파티션 수",
        "집계 연산의 부분 집계 여부",
        "컬럼 프루닝 적용 여부"
    ]
    
    for point in check_points:
        print(f"   - {point}")

def common_performance_patterns():
    """자주 사용되는 성능 패턴"""
    
    print("=== 자주 사용되는 성능 패턴 ===")
    
    patterns = {
        "필터 우선 적용": {
            "설명": "조인이나 집계 전에 필터 적용",
            "예제": "df.filter(condition).join(other_df)"
        },
        "컬럼 선택 최적화": {
            "설명": "필요한 컬럼만 선택 후 연산",
            "예제": "df.select('col1', 'col2').groupBy('col1').sum('col2')"
        },
        "브로드캐스트 조인": {
            "설명": "작은 테이블을 브로드캐스트하여 조인",
            "예제": "large_df.join(broadcast(small_df), 'key')"
        },
        "파티션 키 조인": {
            "설명": "조인 키로 사전 파티셔닝",
            "예제": "df1.repartition('key').join(df2.repartition('key'), 'key')"
        },
        "캐시 활용": {
            "설명": "반복 사용되는 데이터 캐시",
            "예제": "df.filter(condition).cache()"
        }
    }
    
    print("🚀 성능 최적화 패턴:")
    for pattern, info in patterns.items():
        print(f"\n{pattern}:")
        print(f"   설명: {info['설명']}")
        print(f"   예제: {info['예제']}")

def troubleshooting_guide():
    """성능 문제 해결 가이드"""
    
    print("=== 성능 문제 해결 가이드 ===")
    
    common_issues = {
        "OutOfMemoryError": {
            "원인": ["Executor 메모리 부족", "파티션 크기 불균형", "과도한 캐시 사용"],
            "해결책": ["executor-memory 증가", "파티션 수 증가", "캐시 정리"]
        },
        "느린 조인 성능": {
            "원인": ["큰 테이블간 조인", "데이터 스큐", "부적절한 조인 타입"],
            "해결책": ["브로드캐스트 조인", "솔트 키 추가", "조인 힌트 사용"]
        },
        "긴 GC 시간": {
            "원인": ["힙 메모리 부족", "큰 객체 생성", "메모리 누수"],
            "해결책": ["G1GC 사용", "메모리 설정 조정", "객체 재사용"]
        },
        "스테이지 실패": {
            "원인": ["네트워크 오류", "노드 장애", "태스크 타임아웃"],
            "해결책": ["재시도 설정 증가", "클러스터 상태 확인", "타임아웃 증가"]
        }
    }
    
    print("🔧 일반적인 성능 문제 및 해결책:")
    for issue, info in common_issues.items():
        print(f"\n{issue}:")
        print(f"   원인: {', '.join(info['원인'])}")
        print(f"   해결책: {', '.join(info['해결책'])}")

if __name__ == "__main__":
    print("✅ 성능 최적화 함수들이 로드되었습니다.")
    print("   - dataframe_optimization_techniques(): DataFrame 최적화")
    print("   - caching_strategies(): 캐싱 전략")
    print("   - schema_optimization(): 스키마 최적화")
    print("   - performance_monitoring_and_tuning(): 성능 모니터링")
    print("   - spark_ui_analysis_guide(): Spark UI 분석 가이드")
    print("   - execution_plan_analysis(): 실행 계획 분석")
    print("   - common_performance_patterns(): 성능 패턴")
    print("   - troubleshooting_guide(): 문제 해결 가이드")
