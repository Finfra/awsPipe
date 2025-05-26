import boto3
import json
import time
from datetime import datetime
import random

def generate_test_data():
    """테스트 데이터 생성"""
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'event_type': random.choice(['login', 'logout', 'purchase', 'view']),
        'user_id': random.randint(1000, 9999),
        'ip_address': f"192.168.{random.randint(1,10)}.{random.randint(1,254)}",
        'status_code': random.choice([200, 201, 404, 500]),
        'response_size': random.randint(100, 5000),
        'path': random.choice(['/', '/api/users', '/api/orders'])
    }

def send_test_data(stream_name, count=100):
    """테스트 데이터 전송"""
    firehose = boto3.client('firehose')
    
    records = []
    for i in range(count):
        data = generate_test_data()
        records.append({'Data': json.dumps(data)})
        
        # 25개씩 배치 전송
        if len(records) == 25 or i == count - 1:
            response = firehose.put_record_batch(
                DeliveryStreamName=stream_name,
                Records=records
            )
            
            success = len(records) - response['FailedPutCount']
            print(f"전송: {success}/{len(records)} 성공")
            
            records = []
            time.sleep(1)
    
    print(f"총 {count}개 레코드 전송 완료")

if __name__ == "__main__":
    send_test_data('etl-demo-stream', 200)
