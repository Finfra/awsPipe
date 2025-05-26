import boto3
import time
from datetime import datetime, timedelta

class KinesisShardManager:
    def __init__(self, stream_name):
        self.kinesis = boto3.client('kinesis')
        self.cloudwatch = boto3.client('cloudwatch')
        self.stream_name = stream_name
    
    def get_stream_metrics(self, minutes=5):
        """스트림 메트릭 조회"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=minutes)
        
        # 처리량 메트릭 조회
        response = self.cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='IncomingBytes',
            Dimensions=[
                {'Name': 'StreamName', 'Value': self.stream_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=60,  # 1분 단위
            Statistics=['Average', 'Maximum']
        )
        
        if not response['Datapoints']:
            return {'avg_bytes_per_sec': 0, 'max_bytes_per_sec': 0}
        
        # 바이트/초로 변환
        avg_bytes = sum(dp['Average'] for dp in response['Datapoints']) / len(response['Datapoints'])
        max_bytes = max(dp['Maximum'] for dp in response['Datapoints'])
        
        return {
            'avg_bytes_per_sec': avg_bytes,
            'max_bytes_per_sec': max_bytes
        }
    
    def get_current_shard_count(self):
        """현재 활성 샤드 수 조회"""
        try:
            response = self.kinesis.describe_stream(StreamName=self.stream_name)
            shards = response['StreamDescription']['Shards']
            
            # 활성 샤드만 카운트 (EndingSequenceNumber가 없는 샤드)
            active_shards = [
                shard for shard in shards 
                if 'EndingSequenceNumber' not in shard.get('SequenceNumberRange', {})
            ]
            
            return len(active_shards)
        except Exception as e:
            print(f"샤드 수 조회 오류: {e}")
            return 0
    
    def should_scale_out(self, metrics):
        """스케일 아웃 필요 여부 판단"""
        current_shards = self.get_current_shard_count()
        if current_shards == 0:
            return False
        
        # MB/초로 변환
        avg_mb_per_sec = metrics['avg_bytes_per_sec'] / 1024 / 1024
        per_shard_throughput = avg_mb_per_sec / current_shards
        
        # 샤드당 처리량이 0.8MB/초 초과 시 스케일 아웃
        return per_shard_throughput > 0.8
    
    def should_scale_in(self, metrics):
        """스케일 인 필요 여부 판단"""
        current_shards = self.get_current_shard_count()
        if current_shards <= 1:  # 최소 1개 샤드 유지
            return False
        
        avg_mb_per_sec = metrics['avg_bytes_per_sec'] / 1024 / 1024
        per_shard_throughput = avg_mb_per_sec / current_shards
        
        # 샤드당 처리량이 0.2MB/초 미만 시 스케일 인
        return per_shard_throughput < 0.2
    
    def scale_out(self):
        """스케일 아웃 실행"""
        current_shards = self.get_current_shard_count()
        new_shard_count = min(current_shards * 2, 100)  # 최대 100개 제한
        
        try:
            response = self.kinesis.update_shard_count(
                StreamName=self.stream_name,
                TargetShardCount=new_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            print(f"✅ 스케일 아웃 시작: {current_shards} → {new_shard_count} 샤드")
            print(f"현재 샤드 수: {response['CurrentShardCount']}")
            print(f"목표 샤드 수: {response['TargetShardCount']}")
            
            return True
            
        except Exception as e:
            print(f"❌ 스케일 아웃 실패: {e}")
            return False
    
    def scale_in(self):
        """스케일 인 실행"""
        current_shards = self.get_current_shard_count()
        new_shard_count = max(current_shards // 2, 1)
        
        try:
            response = self.kinesis.update_shard_count(
                StreamName=self.stream_name,
                TargetShardCount=new_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            print(f"✅ 스케일 인 시작: {current_shards} → {new_shard_count} 샤드")
            print(f"현재 샤드 수: {response['CurrentShardCount']}")
            print(f"목표 샤드 수: {response['TargetShardCount']}")
            
            return True
            
        except Exception as e:
            print(f"❌ 스케일 인 실패: {e}")
            return False
    
    def auto_scale(self):
        """자동 스케일링 실행"""
        print(f"=== 자동 스케일링 확인: {self.stream_name} ===")
        
        metrics = self.get_stream_metrics()
        current_shards = self.get_current_shard_count()
        
        print(f"현재 샤드 수: {current_shards}")
        print(f"평균 처리량: {metrics['avg_bytes_per_sec']/1024/1024:.2f} MB/초")
        print(f"최대 처리량: {metrics['max_bytes_per_sec']/1024/1024:.2f} MB/초")
        
        if current_shards > 0:
            per_shard_avg = (metrics['avg_bytes_per_sec']/1024/1024) / current_shards
            print(f"샤드당 평균 처리량: {per_shard_avg:.2f} MB/초")
        
        if self.should_scale_out(metrics):
            return self.scale_out()
        elif self.should_scale_in(metrics):
            return self.scale_in()
        else:
            print("🔵 스케일링 불필요")
            return True
    
    def get_scaling_status(self):
        """스케일링 상태 확인"""
        try:
            response = self.kinesis.describe_stream(StreamName=self.stream_name)
            stream_status = response['StreamDescription']['StreamStatus']
            
            print(f"스트림 상태: {stream_status}")
            
            if stream_status == 'UPDATING':
                print("⏳ 스케일링 진행 중...")
                return False
            elif stream_status == 'ACTIVE':
                print("✅ 스트림 활성 상태")
                return True
            else:
                print(f"⚠️ 알 수 없는 상태: {stream_status}")
                return False
                
        except Exception as e:
            print(f"상태 확인 오류: {e}")
            return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python shard_manager.py <stream_name> [action]")
        print("Actions: auto_scale, status, scale_out, scale_in")
        sys.exit(1)
    
    stream_name = sys.argv[1]
    action = sys.argv[2] if len(sys.argv) > 2 else 'auto_scale'
    
    manager = KinesisShardManager(stream_name)
    
    if action == 'auto_scale':
        manager.auto_scale()
    elif action == 'status':
        manager.get_scaling_status()
        metrics = manager.get_stream_metrics()
        print(f"\n현재 메트릭:")
        print(f"  평균 처리량: {metrics['avg_bytes_per_sec']/1024/1024:.2f} MB/초")
        print(f"  최대 처리량: {metrics['max_bytes_per_sec']/1024/1024:.2f} MB/초")
    elif action == 'scale_out':
        manager.scale_out()
    elif action == 'scale_in':
        manager.scale_in()
    else:
        print(f"알 수 없는 액션: {action}")
