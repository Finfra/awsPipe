import json
import boto3
from datetime import datetime, timedelta
import random

def generate_sample_data(count=500):
    """간단한 샘플 데이터 생성"""
    data = []
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    
    for i in range(count):
        record = {
            'id': i,
            'timestamp': (base_time + timedelta(minutes=i)).isoformat(),
            'user_id': random.randint(1000, 9999),
            'event_type': random.choice(['login', 'logout', 'purchase', 'view']),
            'amount': round(random.uniform(10.0, 1000.0), 2)
        }
        data.append(record)
    return data

def upload_partitioned_data(bucket_name, data):
    """파티션된 구조로 데이터 업로드"""
    s3 = boto3.client('s3')
    partitioned_data = {}
    
    for record in data:
        timestamp = datetime.fromisoformat(record['timestamp'])
        partition_key = f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
        
        if partition_key not in partitioned_data:
            partitioned_data[partition_key] = []
        partitioned_data[partition_key].append(record)
    
    for partition_key, records in partitioned_data.items():
        file_content = '\n'.join(json.dumps(record) for record in records)
        s3_key = f"raw-data/{partition_key}/data.jsonl"
        
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType='application/jsonl'
        )
        print(f"업로드: s3://{bucket_name}/{s3_key}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("사용법: python generate_data.py <bucket-name>")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    sample_data = generate_sample_data(500)
    upload_partitioned_data(bucket_name, sample_data)
    print("완료!")
