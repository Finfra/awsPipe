import boto3
import time

def test_partition_query(bucket_name):
    """간단한 파티션 쿼리 성능 테스트"""
    s3 = boto3.client('s3')
    
    # 전체 스캔
    print("=== 전체 스캔 ===")
    start_time = time.time()
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw-data/')
    total_objects = response.get('KeyCount', 0)
    scan_time = time.time() - start_time
    
    print(f"전체 객체: {total_objects}, 시간: {scan_time:.2f}초")
    
    # 파티션 쿼리
    print("=== 파티션 쿼리 ===")
    start_time = time.time()
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix='raw-data/year=2024/month=01/day=15/'
    )
    
    partition_objects = response.get('KeyCount', 0)
    partition_time = time.time() - start_time
    
    print(f"파티션 객체: {partition_objects}, 시간: {partition_time:.2f}초")
    
    if partition_time > 0:
        print(f"성능 개선: {(scan_time/partition_time):.1f}배")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("사용법: python test_performance.py <bucket-name>")
        sys.exit(1)
    
    test_partition_query(sys.argv[1])
