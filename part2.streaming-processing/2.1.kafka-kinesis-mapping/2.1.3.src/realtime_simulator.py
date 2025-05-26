#!/usr/bin/env python3
import boto3
import json
import time
import random
from datetime import datetime
import threading

class KinesisSimulator:
    def __init__(self, stream_name):
        self.kinesis = boto3.client('kinesis')
        self.stream_name = stream_name
        self.running = False
        
    def start_simulation(self, duration_seconds=60):
        """시뮬레이션 시작"""
        self.running = True
        print(f"🚀 실시간 데이터 시뮬레이션 시작 ({duration_seconds}초간)")
        
        # 여러 스레드로 데이터 생성
        threads = []
        for i in range(3):  # 3개 스레드
            thread = threading.Thread(
                target=self.simulate_user_activity,
                args=(f'simulator_{i}', duration_seconds)
            )
            threads.append(thread)
            thread.start()
        
        # 모든 스레드 완료 대기
        for thread in threads:
            thread.join()
            
        print("🎉 시뮬레이션 완료")
    
    def simulate_user_activity(self, simulator_id, duration):
        """사용자 활동 시뮬레이션"""
        start_time = time.time()
        count = 0
        
        while time.time() - start_time < duration and self.running:
            # 랜덤 사용자 이벤트 생성
            event = {
                'timestamp': datetime.utcnow().isoformat(),
                'simulator_id': simulator_id,
                'user_id': random.randint(1000, 9999),
                'event_type': random.choice(['click', 'view', 'purchase', 'login', 'logout']),
                'value': random.randint(1, 1000),
                'session_id': f'session_{random.randint(100, 999)}',
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'location': random.choice(['US', 'EU', 'ASIA'])
            }
            
            try:
                self.kinesis.put_record(
                    StreamName=self.stream_name,
                    Data=json.dumps(event),
                    PartitionKey=event['session_id']
                )
                count += 1
                
                if count % 10 == 0:
                    print(f"📊 {simulator_id}: {count}개 이벤트 전송")
                    
            except Exception as e:
                print(f"❌ 전송 오류 ({simulator_id}): {e}")
            
            # 100ms ~ 2초 랜덤 대기
            time.sleep(random.uniform(0.1, 2.0))
    
    def stop_simulation(self):
        """시뮬레이션 중지"""
        self.running = False
        print("⏹️ 시뮬레이션 중지 요청")

def run_load_test(stream_name, duration=60, thread_count=3):
    """부하 테스트 실행"""
    simulator = KinesisSimulator(stream_name)
    
    try:
        simulator.start_simulation(duration)
    except KeyboardInterrupt:
        print("\n⏹️ 사용자에 의해 중단됨")
        simulator.stop_simulation()

if __name__ == "__main__":
    import sys
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'lab-data-stream'
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    run_load_test(stream_name, duration)
