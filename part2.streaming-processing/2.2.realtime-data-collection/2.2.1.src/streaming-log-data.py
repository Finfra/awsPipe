#!/usr/bin/env python3
"""
실시간 로그 데이터 스트리밍
웹 서버 로그, 애플리케이션 로그를 실시간으로 Kinesis로 전송
"""

import boto3
import json
import time
import threading
import queue
import re
from datetime import datetime
from pathlib import Path
import logging
import os
import signal
import sys

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LogStreamer:
    """실시간 로그 스트리밍 클래스"""
    
    def __init__(self, stream_name, region='us-east-1'):
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.stream_name = stream_name
        self.log_queue = queue.Queue(maxsize=1000)
        self.stats = {
            'lines_processed': 0,
            'records_sent': 0,
            'errors': 0,
            'start_time': time.time()
        }
        self.running = False
        self.batch_size = 500
        self.batch_timeout = 5  # 5초마다 배치 전송
    
    def parse_apache_log(self, log_line):
        """Apache 액세스 로그 파싱"""
        
        # Combined 로그 형식 정규식
        pattern = r'(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"'
        
        match = re.match(pattern, log_line.strip())
        if match:
            ip, timestamp, method, path, protocol, status, size, referer, user_agent = match.groups()
            
            # 타임스탬프 변환
            try:
                log_time = datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z')
            except:
                log_time = datetime.utcnow()
            
            return {
                'timestamp': log_time.isoformat(),
                'ip_address': ip,
                'method': method,
                'path': path,
                'protocol': protocol,
                'status_code': int(status),
                'response_size': int(size) if size.isdigit() else 0,
                'referer': referer if referer != '-' else None,
                'user_agent': user_agent,
                'log_type': 'apache_access'
            }
        
        return None
    
    def parse_nginx_log(self, log_line):
        """Nginx 액세스 로그 파싱"""
        
        # Nginx 기본 로그 형식
        pattern = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)"'
        
        match = re.match(pattern, log_line.strip())
        if match:
            ip, timestamp, method, path, protocol, status, size, referer, user_agent = match.groups()
            
            try:
                log_time = datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z')
            except:
                log_time = datetime.utcnow()
            
            return {
                'timestamp': log_time.isoformat(),
                'ip_address': ip,
                'method': method,
                'path': path,
                'protocol': protocol,
                'status_code': int(status),
                'response_size': int(size),
                'referer': referer if referer != '-' else None,
                'user_agent': user_agent,
                'log_type': 'nginx_access'
            }
        
        return None
    
    def parse_application_log(self, log_line):
        """애플리케이션 로그 파싱 (JSON 형식 가정)"""
        
        try:
            # JSON 로그인 경우
            log_data = json.loads(log_line.strip())
            
            # 표준 필드로 변환
            return {
                'timestamp': log_data.get('timestamp', datetime.utcnow().isoformat()),
                'level': log_data.get('level', 'INFO'),
                'message': log_data.get('message', ''),
                'logger': log_data.get('logger', 'unknown'),
                'thread': log_data.get('thread'),
                'exception': log_data.get('exception'),
                'user_id': log_data.get('user_id'),
                'request_id': log_data.get('request_id'),
                'log_type': 'application'
            }
        
        except json.JSONDecodeError:
            # 일반 텍스트 로그 처리
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'level': 'INFO',
                'message': log_line.strip(),
                'log_type': 'application_text'
            }
    
    def send_to_kinesis_batch(self, records):
        """배치로 Kinesis에 전송"""
        
        if not records:
            return
        
        try:
            # Kinesis 레코드 형식으로 변환
            kinesis_records = []
            for record in records:
                kinesis_records.append({
                    'Data': json.dumps(record),
                    'PartitionKey': record.get('ip_address', record.get('request_id', 'default'))
                })
            
            # 배치 전송 (최대 500개)
            response = self.kinesis.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            
            # 통계 업데이트
            success_count = len(kinesis_records) - response['FailedRecordCount']
            self.stats['records_sent'] += success_count
            self.stats['errors'] += response['FailedRecordCount']
            
            if response['FailedRecordCount'] > 0:
                logger.warning(f"배치 전송 중 {response['FailedRecordCount']}개 레코드 실패")
            
            logger.debug(f"배치 전송 완료: {success_count}개 성공")
            
        except Exception as e:
            logger.error(f"Kinesis 전송 오류: {e}")
            self.stats['errors'] += len(records)
    
    def batch_sender_thread(self):
        """배치 전송 스레드"""
        
        batch = []
        last_send_time = time.time()
        
        while self.running:
            try:
                # 큐에서 레코드 가져오기 (타임아웃 1초)
                try:
                    record = self.log_queue.get(timeout=1)
                    batch.append(record)
                except queue.Empty:
                    pass
                
                current_time = time.time()
                
                # 배치 크기 또는 시간 기준으로 전송
                if (len(batch) >= self.batch_size or 
                    (batch and current_time - last_send_time >= self.batch_timeout)):
                    
                    self.send_to_kinesis_batch(batch)
                    batch = []
                    last_send_time = current_time
                
            except Exception as e:
                logger.error(f"배치 전송 스레드 오류: {e}")
        
        # 종료 시 남은 배치 전송
        if batch:
            self.send_to_kinesis_batch(batch)
    
    def tail_file(self, file_path, log_type='apache'):
        """파일을 tail로 읽으면서 로그 스트리밍"""
        
        logger.info(f"로그 파일 tail 시작: {file_path} ({log_type})")
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                # 파일 끝으로 이동
                file.seek(0, 2)
                
                while self.running:
                    line = file.readline()
                    
                    if line:
                        # 로그 파싱
                        parsed_log = None
                        
                        if log_type == 'apache':
                            parsed_log = self.parse_apache_log(line)
                        elif log_type == 'nginx':
                            parsed_log = self.parse_nginx_log(line)
                        elif log_type == 'application':
                            parsed_log = self.parse_application_log(line)
                        
                        if parsed_log:
                            try:
                                self.log_queue.put(parsed_log, timeout=1)
                                self.stats['lines_processed'] += 1
                            except queue.Full:
                                logger.warning("로그 큐 가득참 - 레코드 드롭")
                                self.stats['errors'] += 1
                        else:
                            logger.debug(f"파싱 실패한 로그: {line.strip()}")
                    else:
                        # 새 라인이 없으면 잠시 대기
                        time.sleep(0.1)
                        
        except FileNotFoundError:
            logger.error(f"로그 파일을 찾을 수 없음: {file_path}")
        except Exception as e:
            logger.error(f"로그 파일 읽기 오류: {e}")
    
    def start_streaming(self, log_files):
        """로그 스트리밍 시작"""
        
        logger.info("로그 스트리밍 시작")
        self.running = True
        
        # 배치 전송 스레드 시작
        batch_thread = threading.Thread(target=self.batch_sender_thread)
        batch_thread.daemon = True
        batch_thread.start()
        
        # 각 로그 파일에 대해 tail 스레드 시작
        tail_threads = []
        
        for log_file_config in log_files:
            file_path = log_file_config['path']
            log_type = log_file_config.get('type', 'apache')
            
            if os.path.exists(file_path):
                tail_thread = threading.Thread(
                    target=self.tail_file,
                    args=(file_path, log_type)
                )
                tail_thread.daemon = True
                tail_thread.start()
                tail_threads.append(tail_thread)
            else:
                logger.warning(f"로그 파일이 존재하지 않음: {file_path}")
        
        # 통계 출력 스레드
        stats_thread = threading.Thread(target=self.print_stats_periodically)
        stats_thread.daemon = True
        stats_thread.start()
        
        # 메인 스레드 대기
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("사용자 중단 요청")
            self.stop_streaming()
        
        # 모든 스레드 종료 대기
        for thread in tail_threads:
            thread.join(timeout=5)
        
        batch_thread.join(timeout=5)
        stats_thread.join(timeout=5)
        
        logger.info("로그 스트리밍 종료")
    
    def stop_streaming(self):
        """로그 스트리밍 중지"""
        self.running = False
    
    def print_stats_periodically(self):
        """주기적으로 통계 출력"""
        
        while self.running:
            time.sleep(30)  # 30초마다 출력
            
            elapsed = time.time() - self.stats['start_time']
            lines_per_sec = self.stats['lines_processed'] / max(elapsed, 1)
            records_per_sec = self.stats['records_sent'] / max(elapsed, 1)
            
            logger.info(f"=== 스트리밍 통계 ===")
            logger.info(f"처리된 라인: {self.stats['lines_processed']:,}")
            logger.info(f"전송된 레코드: {self.stats['records_sent']:,}")
            logger.info(f"오류: {self.stats['errors']:,}")
            logger.info(f"처리율: {lines_per_sec:.1f} 라인/초")
            logger.info(f"전송율: {records_per_sec:.1f} 레코드/초")
            logger.info(f"큐 크기: {self.log_queue.qsize()}")

class LogGenerator:
    """테스트용 로그 생성기"""
    
    @staticmethod
    def generate_apache_logs(file_path, count=1000, interval=0.1):
        """Apache 로그 생성"""
        
        logger.info(f"Apache 로그 생성 시작: {file_path}")
        
        ips = ['192.168.1.100', '10.0.0.50', '203.0.113.10', '198.51.100.5']
        paths = ['/', '/index.html', '/api/users', '/api/orders', '/login', '/logout']
        status_codes = [200, 200, 200, 404, 500, 301]
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'curl/7.68.0'
        ]
        
        with open(file_path, 'a') as f:
            for i in range(count):
                timestamp = datetime.now().strftime('%d/%b/%Y:%H:%M:%S %z')
                ip = ips[i % len(ips)]
                path = paths[i % len(paths)]
                status = status_codes[i % len(status_codes)]
                size = 1000 + (i * 100) % 50000
                user_agent = user_agents[i % len(user_agents)]
                
                log_line = f'{ip} - - [{timestamp}] "GET {path} HTTP/1.1" {status} {size} "-" "{user_agent}"\n'
                f.write(log_line)
                f.flush()
                
                if interval > 0:
                    time.sleep(interval)
        
        logger.info(f"Apache 로그 생성 완료: {count}개 라인")
    
    @staticmethod
    def generate_application_logs(file_path, count=1000, interval=0.1):
        """애플리케이션 로그 생성 (JSON 형식)"""
        
        logger.info(f"애플리케이션 로그 생성 시작: {file_path}")
        
        levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
        messages = [
            'User login successful',
            'Order processed successfully',
            'Database connection timeout',
            'Invalid API request',
            'Cache miss for key'
        ]
        
        with open(file_path, 'a') as f:
            for i in range(count):
                log_entry = {
                    'timestamp': datetime.utcnow().isoformat(),
                    'level': levels[i % len(levels)],
                    'logger': 'com.example.app',
                    'message': messages[i % len(messages)],
                    'thread': f'thread-{i % 10}',
                    'user_id': 1000 + (i % 100),
                    'request_id': f'req-{i:06d}'
                }
                
                f.write(json.dumps(log_entry) + '\n')
                f.flush()
                
                if interval > 0:
                    time.sleep(interval)
        
        logger.info(f"애플리케이션 로그 생성 완료: {count}개 라인")

def signal_handler(signum, frame):
    """시그널 핸들러"""
    logger.info("종료 시그널 수신")
    sys.exit(0)

def main():
    """메인 실행 함수"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='실시간 로그 스트리밍')
    parser.add_argument('--stream-name', required=True, help='Kinesis 스트림 이름')
    parser.add_argument('--region', default='us-east-1', help='AWS 리전')
    parser.add_argument('--log-files', required=True, help='로그 파일 설정 (JSON)')
    parser.add_argument('--generate-test-logs', action='store_true', 
                       help='테스트 로그 생성')
    
    args = parser.parse_args()
    
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 로그 파일 설정 파싱
        log_files = json.loads(args.log_files)
        
        # 테스트 로그 생성 (옵션)
        if args.generate_test_logs:
            logger.info("테스트 로그 생성")
            
            # 로그 파일 디렉토리 생성
            for log_file in log_files:
                log_path = Path(log_file['path'])
                log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 백그라운드에서 로그 생성
            def generate_logs():
                while True:
                    for log_file in log_files:
                        if log_file.get('type') == 'apache':
                            LogGenerator.generate_apache_logs(
                                log_file['path'], count=10, interval=1
                            )
                        elif log_file.get('type') == 'application':
                            LogGenerator.generate_application_logs(
                                log_file['path'], count=10, interval=1
                            )
                    time.sleep(5)
            
            log_gen_thread = threading.Thread(target=generate_logs)
            log_gen_thread.daemon = True
            log_gen_thread.start()
        
        # 로그 스트리머 시작
        streamer = LogStreamer(args.stream_name, args.region)
        streamer.start_streaming(log_files)
        
    except json.JSONDecodeError:
        logger.error("로그 파일 설정 JSON 파싱 오류")
    except Exception as e:
        logger.error(f"실행 오류: {e}")
        raise

if __name__ == "__main__":
    # 사용 예시
    example_config = """
    [
        {
            "path": "/var/log/apache2/access.log",
            "type": "apache"
        },
        {
            "path": "/var/log/nginx/access.log", 
            "type": "nginx"
        },
        {
            "path": "/var/log/myapp/app.log",
            "type": "application"
        }
    ]
    """
    
    print("사용 예시:")
    print(f"python {sys.argv[0]} --stream-name my-log-stream --log-files '{example_config.strip()}'")
    print(f"python {sys.argv[0]} --stream-name my-log-stream --log-files '{example_config.strip()}' --generate-test-logs")
    
    main()
