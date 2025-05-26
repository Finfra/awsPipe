import json
import boto3
from datetime import datetime, timedelta
import random
import os

def generate_sample_data(count=1000):
    """샘플 데이터 생성"""
    data = []
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    for i in range(count):
        record = {
            'id': i,
            'timestamp': (base_time + timedelta(minutes=i)).isoformat(),
            'user_id': random.randint(1000, 9999),
            'event_type': random.choice(['login', 'logout', 'purchase', 'view']),
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'category': random.choice(['electronics', 'clothing', 'books', 'food'])
        }
        data.append(record)
    
    return data

def upload_partitioned_data(bucket_name, data):
    """파티션된 구조로 데이터 업로드"""
    s3 = boto3.client('s3')
    
    # 시간별로 데이터 그룹화
    partitioned_data = {}
    
    for record in data:
        timestamp = datetime.fromisoformat(record['timestamp'])
        partition_key = f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={timestamp.hour:02d}"
        
        if partition_key not in partitioned_data:
            partitioned_data[partition_key] = []
        
        partitioned_data[partition_key].append(record)
    
    # 각 파티션별로 파일 업로드
    for partition_key, records in partitioned_data.items():
        # JSON Lines 형식으로 저장
        file_content = '\n'.join(json.dumps(record) for record in records)
        
        # S3 키 생성
        s3_key = f"raw-data/{partition_key}/data.jsonl"
        
        # S3에 업로드
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType='application/jsonl'
        )
        
        print(f"업로드 완료: s3://{bucket_name}/{s3_key} ({len(records)} records)")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("사용법: python generate_partitioned_data.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    
    # 데이터 생성 및 업로드
    print("샘플 데이터 생성 중...")
    sample_data = generate_sample_data(2000)
    
    print("파티션된 데이터 업로드 중...")
    upload_partitioned_data(bucket_name, sample_data)
    
    print("완료!")
