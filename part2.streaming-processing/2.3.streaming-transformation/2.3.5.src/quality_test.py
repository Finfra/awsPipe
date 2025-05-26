import boto3
import json
import random
import time
from datetime import datetime

def generate_test_data():
    """테스트 데이터 생성 (품질 이슈 포함)"""
    # 80% 정상 데이터
    if random.random() < 0.8:
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_type': random.choice(['login', 'logout', 'purchase', 'view']),
            'user_id': random.randint(1000, 9999),
            'session_id': f"sess_{random.randint(100000, 999999)}",
            'ip_address': f"192.168.{random.randint(1,10)}.{random.randint(1,254)}"
        }
    
    # 20% 품질 이슈 데이터
    issue_type = random.choice([
        'missing_timestamp', 
        'invalid_timestamp_format', 
        'missing_event_type',
        'invalid_user_id_type'
    ])
    
    if issue_type == 'missing_timestamp':
        return {
            'event_type': 'test', 
            'user_id': 1234
        }
    elif issue_type == 'invalid_timestamp_format':
        return {
            'timestamp': 'invalid-time-format',
            'event_type': 'test',
            'user_id': 1234
        }
    elif issue_type == 'missing_event_type':
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'user_id': 1234
        }
    else:  # invalid_user_id_type
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_type': 'test',
            'user_id': 'invalid_user_id'  # 문자열로 잘못된 타입
        }

def send_quality_test_data(stream_name, count=200, batch_size=10):
    """품질 테스트 데이터 전송"""
    kinesis = boto3.client('kinesis')
    
    print(f"품질 테스트 데이터 {count}개를 {stream_name}로 전송 시작...")
    
    for i in range(count):
        data = generate_test_data()
        
        try:
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),
                PartitionKey=f"test-{i % 10}"  # 파티션 분산
            )
            
            if (i + 1) % batch_size == 0:
                print(f"전송 진행: {i + 1}/{count}")
                time.sleep(0.1)  # 부하 조절
                
        except Exception as e:
            print(f"레코드 {i} 전송 실패: {e}")
    
    print(f"테스트 데이터 {count}개 전송 완료")
    print("5-10분 후 CloudWatch에서 품질 메트릭을 확인하세요.")

def generate_burst_test_data(stream_name, burst_count=500):
    """품질 이슈 집중 발생 테스트"""
    kinesis = boto3.client('kinesis')
    
    print(f"품질 이슈 집중 테스트: {burst_count}개 무효 데이터 전송")
    
    for i in range(burst_count):
        # 100% 무효 데이터
        invalid_data = {'invalid': 'data', 'missing_required_fields': True}
        
        try:
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(invalid_data),
                PartitionKey=f"burst-{i % 5}"
            )
            
            if (i + 1) % 50 == 0:
                print(f"무효 데이터 전송: {i + 1}/{burst_count}")
                
        except Exception as e:
            print(f"레코드 {i} 전송 실패: {e}")
    
    print("품질 이슈 집중 테스트 완료 - 알람이 트리거될 예정")

if __name__ == "__main__":
    import sys
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'log-streaming-demo'
    test_type = sys.argv[2] if len(sys.argv) > 2 else 'normal'
    
    if test_type == 'burst':
        generate_burst_test_data(stream_name)
    else:
        send_quality_test_data(stream_name)
