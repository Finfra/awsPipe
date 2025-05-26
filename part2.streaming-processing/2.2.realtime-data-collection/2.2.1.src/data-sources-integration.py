#!/usr/bin/env python3
"""
다양한 데이터 소스에서 Kinesis로 데이터 전송
웹 로그, IoT 센서, 데이터베이스 변경 사항 등을 실시간으로 스트리밍
"""

import boto3
import json
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KinesisDataProducer:
    def __init__(self, stream_name, region='us-east-1'):
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.stream_name = stream_name
        self.stats = {
            'total_sent': 0,
            'total_failed': 0,
            'start_time': time.time()
        }
    
    def send_record(self, data, partition_key):
        """단일 레코드 전송"""
        try:
            response = self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data) if isinstance(data, dict) else data,
                PartitionKey=partition_key
            )
            
            self.stats['total_sent'] += 1
            return response
            
        except Exception as e:
            self.stats['total_failed'] += 1
            logger.error(f"레코드 전송 실패: {e}")
            return None
    
    def send_batch_records(self, records):
        """배치 레코드 전송 (최대 500개)"""
        try:
            # Kinesis put_records 형식으로 변환
            kinesis_records = []
            for record in records:
                kinesis_records.append({
                    'Data': json.dumps(record['data']) if isinstance(record['data'], dict) else record['data'],
                    'PartitionKey': record['partition_key']
                })
            
            response = self.kinesis.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            
            # 성공/실패 통계 업데이트
            success_count = len(kinesis_records) - response['FailedRecordCount']
            self.stats['total_sent'] += success_count
            self.stats['total_failed'] += response['FailedRecordCount']
            
            # 실패한 레코드 로그
            if response['FailedRecordCount'] > 0:
                logger.warning(f"배치 전송 중 {response['FailedRecordCount']}개 레코드 실패")
            
            return response
            
        except Exception as e:
            self.stats['total_failed'] += len(records)
            logger.error(f"배치 전송 실패: {e}")
            return None
    
    def get_stats(self):
        """전송 통계 반환"""
        elapsed_time = time.time() - self.stats['start_time']
        return {
            'total_sent': self.stats['total_sent'],
            'total_failed': self.stats['total_failed'],
            'success_rate': self.stats['total_sent'] / max(self.stats['total_sent'] + self.stats['total_failed'], 1) * 100,
            'throughput_per_sec': self.stats['total_sent'] / max(elapsed_time, 1),
            'elapsed_time': elapsed_time
        }

class WebLogSimulator:
    """웹 로그 시뮬레이터"""
    
    def __init__(self, producer):
        self.producer = producer
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        self.paths = [
            '/', '/products', '/about', '/contact', '/login', 
            '/search', '/cart', '/checkout', '/profile'
        ]
        self.status_codes = [200, 200, 200, 200, 404, 500, 301]  # 200이 대부분
    
    def generate_log_entry(self):
        """웹 로그 엔트리 생성"""
        timestamp = datetime.utcnow()
        
        log_entry = {
            'timestamp': timestamp.isoformat(),
            'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
            'path': random.choice(self.paths),
            'status_code': random.choice(self.status_codes),
            'response_size': random.randint(100, 50000),
            'user_agent': random.choice(self.user_agents),
            'referer': 'https://google.com' if random.random() > 0.7 else '-',
            'session_id': f"sess_{random.randint(100000, 999999)}",
            'user_id': random.randint(1, 10000) if random.random() > 0.3 else None
        }
        
        return log_entry
    
    def run_continuous(self, duration_seconds=300, rate_per_second=10):
        """지속적인 웹 로그 생성"""
        logger.info(f"웹 로그 시뮬레이션 시작: {rate_per_second}개/초, {duration_seconds}초간")
        
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            batch_records = []
            
            # 1초간 생성할 레코드들을 배치로 준비
            for _ in range(rate_per_second):
                log_entry = self.generate_log_entry()
                batch_records.append({
                    'data': log_entry,
                    'partition_key': log_entry['session_id']
                })
            
            # 배치 전송
            if batch_records:
                self.producer.send_batch_records(batch_records)
            
            # 1초 대기
            time.sleep(1)
        
        logger.info("웹 로그 시뮬레이션 완료")

class IoTSensorSimulator:
    """IoT 센서 데이터 시뮬레이터"""
    
    def __init__(self, producer):
        self.producer = producer
        self.device_types = ['temperature', 'humidity', 'pressure', 'motion', 'light']
        self.locations = ['building_a', 'building_b', 'warehouse', 'parking_lot']
    
    def generate_sensor_data(self, device_id, device_type, location):
        """센서 데이터 생성"""
        timestamp = datetime.utcnow()
        
        # 디바이스 타입별 데이터 생성
        if device_type == 'temperature':
            value = round(random.uniform(18.0, 35.0), 2)
            unit = 'celsius'
        elif device_type == 'humidity':
            value = round(random.uniform(30.0, 80.0), 2)
            unit = 'percent'
        elif device_type == 'pressure':
            value = round(random.uniform(980.0, 1050.0), 2)
            unit = 'hpa'
        elif device_type == 'motion':
            value = random.choice([0, 1])  # 0: no motion, 1: motion detected
            unit = 'boolean'
        else:  # light
            value = random.randint(0, 100000)
            unit = 'lux'
        
        sensor_data = {
            'timestamp': timestamp.isoformat(),
            'device_id': device_id,
            'device_type': device_type,
            'location': location,
            'value': value,
            'unit': unit,
            'battery_level': random.randint(20, 100),
            'signal_strength': random.randint(-90, -20)
        }
        
        return sensor_data
    
    def run_continuous(self, duration_seconds=300, devices_count=50):
        """지속적인 IoT 센서 데이터 생성"""
        logger.info(f"IoT 센서 시뮬레이션 시작: {devices_count}개 디바이스, {duration_seconds}초간")
        
        # 디바이스 목록 생성
        devices = []
        for i in range(devices_count):
            devices.append({
                'device_id': f"sensor_{i:03d}",
                'device_type': random.choice(self.device_types),
                'location': random.choice(self.locations)
            })
        
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            batch_records = []
            
            # 각 디바이스에서 데이터 생성 (전체 디바이스의 10%가 매번 데이터 전송)
            active_devices = random.sample(devices, max(1, devices_count // 10))
            
            for device in active_devices:
                sensor_data = self.generate_sensor_data(
                    device['device_id'],
                    device['device_type'], 
                    device['location']
                )
                
                batch_records.append({
                    'data': sensor_data,
                    'partition_key': device['device_id']
                })
            
            # 배치 전송
            if batch_records:
                self.producer.send_batch_records(batch_records)
            
            # 5초 대기 (IoT는 웹 로그보다 느린 주기)
            time.sleep(5)
        
        logger.info("IoT 센서 시뮬레이션 완료")

class DatabaseChangeSimulator:
    """데이터베이스 변경 사항 시뮬레이터 (CDC)"""
    
    def __init__(self, producer):
        self.producer = producer
        self.tables = ['users', 'orders', 'products', 'inventory']
        self.operations = ['INSERT', 'UPDATE', 'DELETE']
    
    def generate_change_event(self):
        """데이터베이스 변경 이벤트 생성"""
        timestamp = datetime.utcnow()
        table = random.choice(self.tables)
        operation = random.choice(self.operations)
        
        # 테이블별 데이터 구조 시뮬레이션
        if table == 'users':
            record_data = {
                'user_id': random.randint(1, 100000),
                'username': f"user_{random.randint(1000, 9999)}",
                'email': f"user{random.randint(1000, 9999)}@example.com",
                'created_at': timestamp.isoformat(),
                'status': random.choice(['active', 'inactive', 'pending'])
            }
        elif table == 'orders':
            record_data = {
                'order_id': random.randint(1, 1000000),
                'user_id': random.randint(1, 100000),
                'total_amount': round(random.uniform(10.0, 500.0), 2),
                'status': random.choice(['pending', 'processing', 'shipped', 'delivered']),
                'created_at': timestamp.isoformat()
            }
        elif table == 'products':
            record_data = {
                'product_id': random.randint(1, 10000),
                'name': f"Product_{random.randint(100, 999)}",
                'price': round(random.uniform(5.0, 200.0), 2),
                'category': random.choice(['electronics', 'clothing', 'books', 'sports']),
                'stock_quantity': random.randint(0, 1000)
            }
        else:  # inventory
            record_data = {
                'product_id': random.randint(1, 10000),
                'location': random.choice(['warehouse_a', 'warehouse_b', 'store_1']),
                'quantity': random.randint(0, 500),
                'last_updated': timestamp.isoformat()
            }
        
        change_event = {
            'timestamp': timestamp.isoformat(),
            'database': 'ecommerce',
            'table': table,
            'operation': operation,
            'transaction_id': random.randint(1000000, 9999999),
            'before': record_data if operation in ['UPDATE', 'DELETE'] else None,
            'after': record_data if operation in ['INSERT', 'UPDATE'] else None
        }
        
        return change_event
    
    def run_continuous(self, duration_seconds=300, rate_per_minute=30):
        """지속적인 데이터베이스 변경 이벤트 생성"""
        logger.info(f"DB 변경 시뮬레이션 시작: {rate_per_minute}개/분, {duration_seconds}초간")
        
        start_time = time.time()
        interval = 60.0 / rate_per_minute  # 이벤트 간 간격(초)
        
        while time.time() - start_time < duration_seconds:
            change_event = self.generate_change_event()
            
            # 단일 레코드 전송 (DB 변경은 개별적으로 발생)
            self.producer.send_record(
                change_event,
                f"{change_event['table']}_{change_event['transaction_id']}"
            )
            
            # 대기
            time.sleep(interval)
        
        logger.info("DB 변경 시뮬레이션 완료")

class MultiSourceDataGenerator:
    """다중 소스 데이터 생성 관리자"""
    
    def __init__(self, stream_name, region='us-east-1'):
        self.producer = KinesisDataProducer(stream_name, region)
        self.web_simulator = WebLogSimulator(self.producer)
        self.iot_simulator = IoTSensorSimulator(self.producer)
        self.db_simulator = DatabaseChangeSimulator(self.producer)
    
    def run_all_sources(self, duration_seconds=300):
        """모든 데이터 소스를 병렬로 실행"""
        logger.info(f"다중 소스 데이터 생성 시작: {duration_seconds}초간")
        
        # 각 시뮬레이터를 별도 스레드에서 실행
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(self.web_simulator.run_continuous, duration_seconds, 15),  # 15개/초
                executor.submit(self.iot_simulator.run_continuous, duration_seconds, 30),   # 30개 디바이스
                executor.submit(self.db_simulator.run_continuous, duration_seconds, 20)    # 20개/분
            ]
            
            # 모든 시뮬레이터 완료 대기
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"시뮬레이터 실행 오류: {e}")
        
        # 최종 통계 출력
        stats = self.producer.get_stats()
        logger.info("=== 최종 전송 통계 ===")
        logger.info(f"총 전송: {stats['total_sent']:,}개")
        logger.info(f"실패: {stats['total_failed']:,}개")
        logger.info(f"성공률: {stats['success_rate']:.1f}%")
        logger.info(f"처리량: {stats['throughput_per_sec']:.1f}개/초")
        logger.info(f"실행 시간: {stats['elapsed_time']:.1f}초")

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='다양한 소스에서 Kinesis로 데이터 전송')
    parser.add_argument('--stream-name', required=True, help='Kinesis 스트림 이름')
    parser.add_argument('--region', default='us-east-1', help='AWS 리전')
    parser.add_argument('--duration', type=int, default=300, help='실행 시간(초)')
    parser.add_argument('--source', choices=['web', 'iot', 'db', 'all'], default='all', 
                       help='실행할 데이터 소스')
    
    args = parser.parse_args()
    
    try:
        if args.source == 'all':
            # 모든 소스 실행
            generator = MultiSourceDataGenerator(args.stream_name, args.region)
            generator.run_all_sources(args.duration)
        else:
            # 개별 소스 실행
            producer = KinesisDataProducer(args.stream_name, args.region)
            
            if args.source == 'web':
                simulator = WebLogSimulator(producer)
                simulator.run_continuous(args.duration, 20)  # 20개/초
            elif args.source == 'iot':
                simulator = IoTSensorSimulator(producer)
                simulator.run_continuous(args.duration, 50)  # 50개 디바이스
            elif args.source == 'db':
                simulator = DatabaseChangeSimulator(producer)
                simulator.run_continuous(args.duration, 30)  # 30개/분
            
            # 통계 출력
            stats = producer.get_stats()
            logger.info("=== 전송 통계 ===")
            logger.info(f"총 전송: {stats['total_sent']:,}개")
            logger.info(f"실패: {stats['total_failed']:,}개")
            logger.info(f"성공률: {stats['success_rate']:.1f}%")
            logger.info(f"처리량: {stats['throughput_per_sec']:.1f}개/초")
    
    except KeyboardInterrupt:
        logger.info("사용자 중단")
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        raise

if __name__ == "__main__":
    main()
