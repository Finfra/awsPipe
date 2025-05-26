#!/usr/bin/env python3
"""
파일 포맷별 성능 비교 스크립트
CSV, JSON, Parquet, ORC 포맷의 저장/로딩 성능 및 파일 크기 비교
"""

import pandas as pd
import time
import os
import json
from pathlib import Path

def compare_file_formats():
    """다양한 파일 포맷의 성능 비교"""
    
    # 테스트 데이터 생성
    print("테스트 데이터 생성 중...")
    test_size = 50000
    
    df = pd.DataFrame({
        'id': range(test_size),
        'name': [f'user_{i}' for i in range(test_size)],
        'email': [f'user{i}@example.com' for i in range(test_size)],
        'age': [20 + (i % 50) for i in range(test_size)],
        'salary': [30000 + (i * 100 % 70000) for i in range(test_size)],
        'department': ['engineering', 'sales', 'marketing', 'hr'][i % 4] for i in range(test_size)],
        'join_date': pd.date_range('2020-01-01', periods=test_size, freq='H'),
        'score': [round(50 + (i * 0.1 % 50), 2) for i in range(test_size)],
        'active': [i % 3 == 0 for i in range(test_size)]
    })
    
    print(f"테스트 데이터: {len(df):,} 레코드, {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    # 테스트 결과 저장
    results = {}
    
    # 포맷별 테스트 함수 정의
    formats = {
        'csv': {
            'save': lambda df, path: df.to_csv(path, index=False),
            'load': lambda path: pd.read_csv(path),
            'extension': '.csv'
        },
        'json': {
            'save': lambda df, path: df.to_json(path, orient='records', date_format='iso'),
            'load': lambda path: pd.read_json(path, orient='records'),
            'extension': '.json'
        },
        'parquet': {
            'save': lambda df, path: df.to_parquet(path, index=False, compression='snappy'),
            'load': lambda path: pd.read_parquet(path),
            'extension': '.parquet'
        },
        'parquet_gzip': {
            'save': lambda df, path: df.to_parquet(path, index=False, compression='gzip'),
            'load': lambda path: pd.read_parquet(path),
            'extension': '.parquet'
        }
    }
    
    # ORC 포맷 추가 (pyarrow 설치된 경우)
    try:
        formats['orc'] = {
            'save': lambda df, path: df.to_orc(path, index=False),
            'load': lambda path: pd.read_orc(path),
            'extension': '.orc'
        }
    except AttributeError:
        print("ORC 포맷 테스트 건너뜀 (pyarrow 업그레이드 필요)")
    
    print("\n=== 파일 포맷 성능 비교 ===")
    print(f"{'Format':<15} {'Save(s)':<10} {'Load(s)':<10} {'Size(MB)':<12} {'Compression':<12}")
    print("-" * 70)
    
    # 각 포맷별 성능 테스트
    for fmt_name, fmt_config in formats.items():
        try:
            file_path = f"test_data{fmt_config['extension']}"
            
            # 저장 성능 측정
            start_time = time.time()
            fmt_config['save'](df, file_path)
            save_time = time.time() - start_time
            
            # 파일 크기 측정
            file_size = os.path.getsize(file_path) / 1024 / 1024
            
            # 로딩 성능 측정
            start_time = time.time()
            loaded_df = fmt_config['load'](file_path)
            load_time = time.time() - start_time
            
            # 압축률 계산 (CSV 대비)
            if fmt_name == 'csv':
                csv_size = file_size
                compression_ratio = "baseline"
            else:
                compression_ratio = f"{(1 - file_size/csv_size)*100:.1f}%"
            
            # 결과 저장
            results[fmt_name] = {
                'save_time': save_time,
                'load_time': load_time,
                'file_size_mb': file_size,
                'compression_ratio': compression_ratio
            }
            
            # 결과 출력
            print(f"{fmt_name:<15} {save_time:<10.2f} {load_time:<10.2f} {file_size:<12.2f} {compression_ratio:<12}")
            
            # 데이터 무결성 확인
            assert len(loaded_df) == len(df), f"{fmt_name}: 레코드 수 불일치"
            
            # 임시 파일 정리
            os.remove(file_path)
            
        except Exception as e:
            print(f"{fmt_name:<15} ERROR: {str(e)}")
            results[fmt_name] = {'error': str(e)}
    
    print("\n=== 성능 분석 ===")
    
    # 가장 빠른 저장/로딩 포맷 찾기
    valid_results = {k: v for k, v in results.items() if 'error' not in v}
    
    if valid_results:
        fastest_save = min(valid_results.items(), key=lambda x: x[1]['save_time'])
        fastest_load = min(valid_results.items(), key=lambda x: x[1]['load_time'])
        smallest_file = min(valid_results.items(), key=lambda x: x[1]['file_size_mb'])
        
        print(f"가장 빠른 저장: {fastest_save[0]} ({fastest_save[1]['save_time']:.2f}초)")
        print(f"가장 빠른 로딩: {fastest_load[0]} ({fastest_load[1]['load_time']:.2f}초)")
        print(f"가장 작은 파일: {smallest_file[0]} ({smallest_file[1]['file_size_mb']:.2f}MB)")
    
    return results

def benchmark_query_performance():
    """쿼리 성능 비교 (Parquet vs CSV)"""
    
    print("\n=== 쿼리 성능 비교 ===")
    
    # 대용량 테스트 데이터 생성
    test_size = 100000
    df = pd.DataFrame({
        'user_id': range(test_size),
        'category': ['A', 'B', 'C', 'D'][i % 4] for i in range(test_size)],
        'amount': [100 + (i % 1000) for i in range(test_size)],
        'date': pd.date_range('2024-01-01', periods=test_size, freq='H'),
        'region': ['north', 'south', 'east', 'west'][i % 4] for i in range(test_size)]
    })
    
    # 파일 저장
    df.to_csv('query_test.csv', index=False)
    df.to_parquet('query_test.parquet', index=False)
    
    # 쿼리 테스트 함수
    def test_query(file_path, query_func, description):
        start_time = time.time()
        result = query_func(file_path)
        end_time = time.time()
        print(f"{description:<30} {end_time - start_time:<10.3f}초")
        return result
    
    # 테스트 쿼리들
    queries = [
        {
            'desc': 'Full scan (count)',
            'csv_func': lambda path: len(pd.read_csv(path)),
            'parquet_func': lambda path: len(pd.read_parquet(path))
        },
        {
            'desc': 'Filter by category',
            'csv_func': lambda path: len(pd.read_csv(path).query("category == 'A'")),
            'parquet_func': lambda path: len(pd.read_parquet(path).query("category == 'A'"))
        },
        {
            'desc': 'Aggregation (sum)',
            'csv_func': lambda path: pd.read_csv(path).groupby('category')['amount'].sum().to_dict(),
            'parquet_func': lambda path: pd.read_parquet(path).groupby('category')['amount'].sum().to_dict()
        }
    ]
    
    print(f"{'Query Type':<30} {'Time':<10}")
    print("-" * 45)
    
    for query in queries:
        print(f"\n{query['desc']}:")
        test_query('query_test.csv', query['csv_func'], '  CSV')
        test_query('query_test.parquet', query['parquet_func'], '  Parquet')
    
    # 정리
    os.remove('query_test.csv')
    os.remove('query_test.parquet')

def generate_recommendations():
    """사용 사례별 포맷 권장사항"""
    
    print("\n=== 포맷 선택 가이드 ===")
    
    recommendations = {
        "CSV": {
            "장점": ["가독성 좋음", "범용적 지원", "간단한 구조"],
            "단점": ["큰 파일 크기", "느린 처리 속도", "타입 정보 손실"],
            "적합한 용도": ["작은 데이터셋", "사람이 읽어야 하는 데이터", "호환성이 중요한 경우"]
        },
        "JSON": {
            "장점": ["구조화된 데이터 표현", "웹 표준", "중첩 구조 지원"],
            "단점": ["큰 파일 크기", "파싱 오버헤드", "압축률 낮음"],
            "적합한 용도": ["API 응답", "설정 파일", "반구조화 데이터"]
        },
        "Parquet": {
            "장점": ["높은 압축률", "빠른 쿼리 성능", "열 지향 저장"],
            "단점": ["바이너리 포맷", "스키마 의존적"],
            "적합한 용도": ["빅데이터 분석", "ETL 파이프라인", "장기 저장"]
        },
        "ORC": {
            "장점": ["최고 압축률", "Hive 최적화", "트랜잭션 지원"],
            "단점": ["Hadoop 생태계 종속", "제한적 지원"],
            "적합한 용도": ["Hadoop 환경", "대용량 배치 처리", "Hive 쿼리"]
        }
    }
    
    for fmt, info in recommendations.items():
        print(f"\n{fmt}:")
        print(f"  장점: {', '.join(info['장점'])}")
        print(f"  단점: {', '.join(info['단점'])}")
        print(f"  권장 용도: {', '.join(info['적합한 용도'])}")

if __name__ == "__main__":
    # 포맷 성능 비교 실행
    results = compare_file_formats()
    
    # 쿼리 성능 비교 실행
    benchmark_query_performance()
    
    # 권장사항 출력
    generate_recommendations()
    
    # 결과를 JSON으로 저장
    with open('format_comparison_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n상세 결과가 'format_comparison_results.json'에 저장됨")
