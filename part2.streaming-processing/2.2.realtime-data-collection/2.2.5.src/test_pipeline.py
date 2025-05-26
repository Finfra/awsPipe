#!/usr/bin/env python3
"""
파이프라인 테스트 스크립트
Kinesis → Firehose → S3 → EMR 파이프라인 전체 테스트
"""

import boto3
import json
import time
import random
from datetime import datetime, timedelta
import uuid

class PipelineTestRunner:
    """파이프라인 테스트 실행기"""
    
    def __init__(self, stream_name='log-streaming-demo'):
        self.kinesis = boto3.client('kinesis')
        self.stream_name = stream_name
        self.test_data_count = 0
        
    def generate_test_log_event(self, event_type='web'):
        """테스트 로그 이벤트 생성"""
        
        base_timestamp = datetime.utcnow()
        
        if event_type == 'web':
            # 웹 로그 이벤트
            return {
                'timestamp': base_timestamp.isoformat() + 'Z',
                'event_id': str(uuid.uuid4()),
                'event_type': 'page_view',
                'log_type': 'apache_access',
                'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
                'path': random.choice(['/', '/api/users', '/api/orders', '/login', '/products']),
                'status_code': random.choice([200, 200, 200, 404, 500, 301]),
                'response_size': random.randint(100, 50000),
                'user_agent': 'Mozilla/5.0 (Test Browser)',
                'referer': random.choice([None, 'https://google.com', 'https://facebook.com']),
                'user_id': random.randint(1000, 9999),
                'session_id': f"session_{random.randint(100000, 999999)}"
            }
        
        elif event_type == 'application':
            # 애플리케이션 로그 이벤트
            return {
                'timestamp': base_timestamp.isoformat() + 'Z',
                'event_id': str(uuid.uuid4()),
                'event_type': 'application_log',
                'log_type': 'application',
                'level': random.choice(['INFO', 'WARN', 'ERROR', 'DEBUG']),
                'logger': random.choice([
                    'com.example.UserService',
                    'com.example.OrderService', 
                    'com.example.PaymentService',
                    'com.example.NotificationService'
                ]),
                'message': random.choice([
                    'User login successful',
                    'Order processed successfully',
                    'Payment transaction completed',
                    'Cache miss for key',
                    'Database connection timeout',
                    'Invalid API request'
                ]),
                'user_id': random.randint(1000, 9999),
                'request_id': f"req-{random.randint(100000, 999999)}",
                'thread': f"thread-{random.randint(1, 10)}"
            }
        
        else:
            # IoT 센서 이벤트
            return {
                'timestamp': base_timestamp.isoformat() + 'Z',
                'event_id': str(uuid.uuid4()),
                'event_type': 'sensor_reading',
                'log_type': 'iot_sensor',
                'device_id': f"sensor_{random.randint(1, 100):03d}",
                'device_type': random.choice(['temperature', 'humidity', 'pressure']),
                'value': round(random.uniform(10.0, 40.0), 2),
                'unit': random.choice(['celsius', 'percent', 'hpa']),
                'location': random.choice(['building_a', 'building_b', 'warehouse']),
                'battery_level': random.randint(20, 100)
            }
    
    def send_test_batch(self, batch_size=25, event_types=['web', 'application', 'iot']):
        """테스트 데이터 배치 전송"""
        
        records = []
        
        for i in range(batch_size):
            event_type = random.choice(event_types)
            event_data = self.generate_test_log_event(event_type)
            
            records.append({
                'Data': json.dumps(event_data),
                'PartitionKey': event_data.get('session_id', event_data.get('device_id', f'partition_{i}'))
            })
        
        try:
            response = self.kinesis.put_records(
                Records=records,
                StreamName=self.stream_name
            )
            
            success_count = len(records) - response['FailedRecordCount']
            self.test_data_count += success_count
            
            print(f"배치 전송 완료: {success_count}/{len(records)} 성공")
            
            if response['FailedRecordCount'] > 0:
                print(f"실패한 레코드: {response['FailedRecordCount']}개")
            
            return success_count
            
        except Exception as e:
            print(f"배치 전송 실패: {e}")
            return 0
    
    def run_continuous_test(self, duration_minutes=10, interval_seconds=30):
        """지속적인 테스트 데이터 전송"""
        
        print(f"파이프라인 테스트 시작: {duration_minutes}분간, {interval_seconds}초 간격")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        batch_count = 0
        
        while time.time() < end_time:
            batch_count += 1
            
            print(f"\n=== 배치 {batch_count} ===")
            
            # 다양한 이벤트 타입으로 배치 전송
            success_count = self.send_test_batch(
                batch_size=random.randint(20, 50),
                event_types=['web', 'application', 'iot']
            )
            
            print(f"누적 전송: {self.test_data_count:,}개 레코드")
            
            # 간격 대기
            if time.time() < end_time:
                time.sleep(interval_seconds)
        
        print(f"\n테스트 완료! 총 {self.test_data_count:,}개 레코드 전송")
        return self.test_data_count
    
    def send_burst_test(self, total_records=1000):
        """대량 데이터 버스트 테스트"""
        
        print(f"버스트 테스트 시작: {total_records:,}개 레코드")
        
        batch_size = 500  # Kinesis 최대 배치 크기
        batches = (total_records + batch_size - 1) // batch_size
        
        for i in range(batches):
            current_batch_size = min(batch_size, total_records - (i * batch_size))
            
            success_count = self.send_test_batch(
                batch_size=current_batch_size,
                event_types=['web', 'application', 'iot']
            )
            
            print(f"배치 {i+1}/{batches}: {success_count}/{current_batch_size} 성공")
            
            # 처리율 제한을 위해 잠시 대기
            time.sleep(1)
        
        print(f"버스트 테스트 완료! 총 {self.test_data_count:,}개 레코드 전송")

def check_kinesis_stream_status(stream_name):
    """Kinesis 스트림 상태 확인"""
    
    kinesis = boto3.client('kinesis')
    
    try:
        response = kinesis.describe_stream(StreamName=stream_name)
        stream_status = response['StreamDescription']['StreamStatus']
        shard_count = len(response['StreamDescription']['Shards'])
        
        print(f"Kinesis 스트림 상태:")
        print(f"  스트림명: {stream_name}")
        print(f"  상태: {stream_status}")
        print(f"  샤드 수: {shard_count}")
        
        return stream_status == 'ACTIVE'
        
    except Exception as e:
        print(f"스트림 상태 확인 실패: {e}")
        return False

def check_firehose_stream_status(delivery_stream_name):
    """Firehose 전송 스트림 상태 확인"""
    
    firehose = boto3.client('firehose')
    
    try:
        response = firehose.describe_delivery_stream(
            DeliveryStreamName=delivery_stream_name
        )
        
        stream_status = response['DeliveryStreamDescription']['DeliveryStreamStatus']
        destination = response['DeliveryStreamDescription']['Destinations'][0]
        
        print(f"Firehose 전송 스트림 상태:")
        print(f"  스트림명: {delivery_stream_name}")
        print(f"  상태: {stream_status}")
        print(f"  대상: {destination.get('ExtendedS3DestinationDescription', {}).get('BucketARN', 'Unknown')}")
        
        return stream_status == 'ACTIVE'
        
    except Exception as e:
        print(f"Firehose 스트림 상태 확인 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='파이프라인 테스트')
    parser.add_argument('--stream-name', default='log-streaming-demo', help='Kinesis 스트림명')
    parser.add_argument('--test-type', choices=['continuous', 'burst', 'single'], 
                       default='continuous', help='테스트 유형')
    parser.add_argument('--duration', type=int, default=10, help='테스트 시간(분)')
    parser.add_argument('--records', type=int, default=1000, help='버스트 테스트 레코드 수')
    parser.add_argument('--check-status', action='store_true', help='스트림 상태만 확인')
    
    args = parser.parse_args()
    
    if args.check_status:
        # 스트림 상태 확인만 수행
        print("=== 스트림 상태 확인 ===")
        kinesis_ok = check_kinesis_stream_status(args.stream_name)
        firehose_ok = check_firehose_stream_status('streaming-to-s3-pipeline')
        
        if kinesis_ok and firehose_ok:
            print("✅ 모든 스트림이 정상 상태입니다.")
        else:
            print("❌ 일부 스트림에 문제가 있습니다.")
        return
    
    # 테스트 실행
    test_runner = PipelineTestRunner(args.stream_name)
    
    try:
        if args.test_type == 'single':
            # 단일 배치 테스트
            print("=== 단일 배치 테스트 ===")
            test_runner.send_test_batch(batch_size=50)
            
        elif args.test_type == 'burst':
            # 버스트 테스트
            print("=== 버스트 테스트 ===")
            test_runner.send_burst_test(total_records=args.records)
            
        else:
            # 지속적 테스트
            print("=== 지속적 테스트 ===")
            test_runner.run_continuous_test(
                duration_minutes=args.duration,
                interval_seconds=30
            )
    
    except KeyboardInterrupt:
        print("\n테스트 중단됨")
    
    except Exception as e:
        print(f"테스트 실행 중 오류: {e}")

if __name__ == "__main__":
    main()
