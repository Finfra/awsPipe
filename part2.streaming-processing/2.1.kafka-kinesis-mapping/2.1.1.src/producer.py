import boto3
import json
import time
import random
from datetime import datetime

# Kinesis 클라이언트 생성
kinesis = boto3.client('kinesis', region_name='ap-northeast-2')

def produce_data():
    """데이터를 지속적으로 Kinesis 스트림에 전송"""
    
    while True:
        # 샘플 데이터 생성
        data = {
            'timestamp': datetime.now().isoformat(),
            'user_id': f'user_{random.randint(1, 1000)}',
            'event_type': random.choice(['login', 'purchase', 'view', 'logout']),
            'value': random.randint(1, 100),
            'session_id': f'session_{random.randint(1000, 9999)}'
        }
        
        try:
            # Kinesis 스트림에 데이터 전송
            response = kinesis.put_record(
                StreamName='test-stream',
                Data=json.dumps(data),
                PartitionKey=data['user_id']  # 파티션 키로 user_id 사용
            )
            
            print(f"Sent: {data}")
            print(f"Response: {response['ShardId']}, SequenceNumber: {response['SequenceNumber']}")
            
        except Exception as e:
            print(f"Error sending data: {e}")
        
        # 1초 간격으로 데이터 전송
        time.sleep(1)

if __name__ == "__main__":
    produce_data()
