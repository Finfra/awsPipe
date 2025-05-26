#!/usr/bin/env python3
import boto3
import json
import time
from datetime import datetime

def send_test_data(stream_name='lab-data-stream', record_count=10):
    """테스트 데이터를 Kinesis 스트림으로 전송"""
    kinesis = boto3.client('kinesis')
    
    print(f"📤 {record_count}개의 테스트 레코드를 {stream_name}으로 전송 시작...")
    
    for i in range(record_count):
        data = {
            'timestamp': datetime.utcnow().isoformat(),
            'user_id': 1000 + i,
            'event_type': 'test_event',
            'value': 100 + i * 10,
            'metadata': {
                'source': 'test_producer',
                'version': '1.0'
            }
        }
        
        try:
            response = kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey=f'user_{1000 + i}'
            )
            
            print(f"✅ 레코드 {i+1}/{record_count} 전송 완료: {response['SequenceNumber']}")
            time.sleep(1)  # 1초 대기
            
        except Exception as e:
            print(f"❌ 레코드 {i+1} 전송 실패: {e}")
    
    print(f"🎉 총 {record_count}개 레코드 전송 완료!")

if __name__ == "__main__":
    import sys
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'lab-data-stream'
    record_count = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    
    send_test_data(stream_name, record_count)
