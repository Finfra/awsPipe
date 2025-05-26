import boto3
import time
import json

def test_partition_query(bucket_name):
    """파티션 쿼리 성능 테스트"""
    s3 = boto3.client('s3')
    
    # 1. 전체 스캔 (비효율적)
    print("=== 전체 스캔 테스트 ===")
    start_time = time.time()
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix='raw-data/'
    )
    
    total_objects = response.get('KeyCount', 0)
    scan_time = time.time() - start_time
    
    print(f"전체 객체 수: {total_objects}")
    print(f"스캔 시간: {scan_time:.2f}초")
    
    # 2. 파티션 기반 쿼리 (효율적)
    print("\n=== 파티션 쿼리 테스트 ===")
    start_time = time.time()
    
    # 특정 날짜의 데이터만 조회
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix='raw-data/year=2024/month=01/day=15/'
    )
    
    partition_objects = response.get('KeyCount', 0)
    partition_time = time.time() - start_time
    
    print(f"파티션 객체 수: {partition_objects}")
    print(f"파티션 쿼리 시간: {partition_time:.2f}초")
    print(f"성능 개선: {(scan_time/partition_time):.1f}배 빠름")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("사용법: python partition_performance_test.py <bucket-name>")
        sys.exit(1)
    
    test_partition_query(sys.argv[1])