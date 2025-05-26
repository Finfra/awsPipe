import pandas as pd
import time
import os
import json
from pathlib import Path

def benchmark_file_formats():
    """파일 포맷별 성능 벤치마크"""
    
    # 테스트 데이터 로드
    print("테스트 데이터 로딩...")
    df = pd.read_pickle('/home/ec2-user/test_dataset.pkl')
    
    # 포맷별 설정
    formats = {
        'csv': {
            'save': lambda df, path: df.to_csv(path, index=False),
            'load': lambda path: pd.read_csv(path, parse_dates=['timestamp']),
            'ext': '.csv'
        },
        'json': {
            'save': lambda df, path: df.to_json(path, orient='records', date_format='iso'),
            'load': lambda path: pd.read_json(path, orient='records'),
            'ext': '.json'
        },
        'parquet_snappy': {
            'save': lambda df, path: df.to_parquet(path, compression='snappy', index=False),
            'load': lambda path: pd.read_parquet(path),
            'ext': '.parquet'
        },
        'parquet_gzip': {
            'save': lambda df, path: df.to_parquet(path, compression='gzip', index=False),
            'load': lambda path: pd.read_parquet(path),
            'ext': '.parquet'
        }
    }
    
    # ORC 추가 (가능한 경우)
    try:
        formats['orc'] = {
            'save': lambda df, path: df.to_orc(path, compression='snappy'),
            'load': lambda path: pd.read_orc(path),
            'ext': '.orc'
        }
    except:
        print("⚠️ ORC 포맷 스킵 (pyarrow 버전 확인 필요)")
    
    print(f"\n{'포맷':<20} {'저장(초)':<10} {'로드(초)':<10} {'크기(MB)':<12} {'압축률':<12}")
    print("=" * 75)
    
    results = {}
    baseline_size = None
    
    for fmt_name, fmt_config in formats.items():
        file_path = f"test_data_{fmt_name}{fmt_config['ext']}"
        
        try:
            # 1. 저장 성능 측정
            start_time = time.time()
            fmt_config['save'](df, file_path)
            save_time = time.time() - start_time
            
            # 2. 파일 크기 측정
            file_size_mb = os.path.getsize(file_path) / 1024 / 1024
            
            # 3. 로딩 성능 측정
            start_time = time.time()
            loaded_df = fmt_config['load'](file_path)
            load_time = time.time() - start_time
            
            # 4. 압축률 계산
            if fmt_name == 'csv':
                baseline_size = file_size_mb
                compression = "기준"
            else:
                reduction = ((baseline_size - file_size_mb) / baseline_size) * 100
                compression = f"{reduction:+.1f}%"
            
            # 5. 데이터 무결성 확인
            assert len(loaded_df) == len(df), f"{fmt_name}: 레코드 수 불일치"
            
            # 결과 저장
            results[fmt_name] = {
                'save_time': save_time,
                'load_time': load_time,
                'file_size_mb': file_size_mb,
                'compression': compression
            }
            
            print(f"{fmt_name:<20} {save_time:<10.2f} {load_time:<10.2f} {file_size_mb:<12.2f} {compression:<12}")
            
            # 임시 파일 정리
            os.remove(file_path)
            
        except Exception as e:
            print(f"{fmt_name:<20} ❌ 오류: {str(e)[:40]}")
            results[fmt_name] = {'error': str(e)}
    
    return results

def query_performance_test():
    """쿼리 성능 비교"""
    
    print("\n=== 쿼리 성능 테스트 ===")
    
    df = pd.read_pickle('/home/ec2-user/test_dataset.pkl')
    
    # 테스트용 파일 생성
    df.to_csv('query_test.csv', index=False)
    df.to_parquet('query_test.parquet', compression='snappy', index=False)
    
    print(f"{'쿼리 유형':<30} {'CSV(초)':<10} {'Parquet(초)':<12} {'성능향상':<10}")
    print("-" * 70)
    
    # 쿼리 테스트
    queries = [
        {
            'name': '전체 레코드 수 조회',
            'csv_func': lambda: len(pd.read_csv('query_test.csv')),
            'parquet_func': lambda: len(pd.read_parquet('query_test.parquet'))
        },
        {
            'name': '카테고리별 필터링',
            'csv_func': lambda: len(pd.read_csv('query_test.csv').query("category == 'electronics'")),
            'parquet_func': lambda: len(pd.read_parquet('query_test.parquet').query("category == 'electronics'"))
        },
        {
            'name': '금액 평균 계산',
            'csv_func': lambda: pd.read_csv('query_test.csv')['amount'].mean(),
            'parquet_func': lambda: pd.read_parquet('query_test.parquet')['amount'].mean()
        },
        {
            'name': '카테고리별 집계',
            'csv_func': lambda: pd.read_csv('query_test.csv').groupby('category')['amount'].sum().to_dict(),
            'parquet_func': lambda: pd.read_parquet('query_test.parquet').groupby('category')['amount'].sum().to_dict()
        }
    ]
    
    for query in queries:
        # CSV 성능
        start = time.time()
        csv_result = query['csv_func']()
        csv_time = time.time() - start
        
        # Parquet 성능
        start = time.time()
        parquet_result = query['parquet_func']()
        parquet_time = time.time() - start
        
        # 성능 향상 계산
        speedup = f"{csv_time/parquet_time:.1f}배" if parquet_time > 0 else "N/A"
        
        print(f"{query['name']:<30} {csv_time:<10.3f} {parquet_time:<12.3f} {speedup:<10}")
    
    # 정리
    os.remove('query_test.csv')
    os.remove('query_test.parquet')

def analyze_results_and_recommendations(results):
    """결과 분석 및 권장사항"""
    
    print("\n=== 성능 분석 ===")
    
    valid_results = {k: v for k, v in results.items() if 'error' not in v}
    
    if not valid_results:
        print("유효한 결과가 없습니다.")
        return
    
    # 최고 성능 찾기
    fastest_save = min(valid_results.items(), key=lambda x: x[1]['save_time'])
    fastest_load = min(valid_results.items(), key=lambda x: x[1]['load_time'])
    smallest_file = min(valid_results.items(), key=lambda x: x[1]['file_size_mb'])
    
    print(f"🚀 가장 빠른 저장: {fastest_save[0]} ({fastest_save[1]['save_time']:.2f}초)")
    print(f"⚡ 가장 빠른 로드: {fastest_load[0]} ({fastest_load[1]['load_time']:.2f}초)")
    print(f"💾 가장 작은 파일: {smallest_file[0]} ({smallest_file[1]['file_size_mb']:.2f}MB)")
    
    print("\n=== 사용 시나리오별 권장사항 ===")
    
    scenarios = {
        "🔄 데이터 교환/호환성": {
            "권장": "CSV",
            "이유": "모든 도구에서 지원, 사람이 읽기 가능",
            "용도": "소규모 데이터, 수동 검토 필요한 경우"
        },
        "🌐 웹 API/NoSQL": {
            "권장": "JSON",
            "이유": "웹 표준, 중첩 구조 지원",
            "용도": "API 응답, 설정 파일, 반구조화 데이터"
        },
        "📊 실시간 분석": {
            "권장": "Parquet (Snappy)",
            "이유": "빠른 압축/해제, 컬럼형 최적화",
            "용도": "BI 도구, 빈번한 쿼리, 대화형 분석"
        },
        "💰 장기 보관/아카이빙": {
            "권장": "Parquet (GZIP)",
            "이유": "최고 압축률, 스토리지 비용 절약",
            "용도": "과거 데이터, 규정 준수, 백업"
        },
        "🏭 Hadoop 생태계": {
            "권장": "ORC",
            "이유": "Hive 최적화, 트랜잭션 지원",
            "용도": "Hive 쿼리, 대용량 배치 처리"
        }
    }
    
    for scenario, info in scenarios.items():
        print(f"\n{scenario}")
        print(f"  권장 포맷: {info['권장']}")
        print(f"  선택 이유: {info['이유']}")
        print(f"  적합 용도: {info['용도']}")

if __name__ == "__main__":
    # 1. 테스트 데이터 확인
    if not os.path.exists('/home/ec2-user/test_dataset.pkl'):
        print("❌ 테스트 데이터가 없습니다. 먼저 load_test_data.py를 실행하세요.")
        exit(1)
    
    # 2. 포맷 성능 비교
    results = benchmark_file_formats()
    
    # 3. 쿼리 성능 테스트
    query_performance_test()
    
    # 4. 결과 분석 및 권장사항
    analyze_results_and_recommendations(results)
    
    # 5. 결과를 JSON으로 저장
    with open('~/format_benchmark_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n📈 상세 결과 저장: ~/format_benchmark_results.json")