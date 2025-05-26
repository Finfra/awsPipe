#!/usr/bin/env python3
import boto3
import json
import time

def consume_test_data(stream_name='lab-data-stream'):
    """Kinesis 스트림에서 데이터 수신"""
    kinesis = boto3.client('kinesis')
    
    print(f"📥 {stream_name}에서 데이터 수신 시작...")
    
    try:
        # 스트림 정보 조회
        response = kinesis.describe_stream(StreamName=stream_name)
        shards = response['StreamDescription']['Shards']
        
        print(f"📊 스트림 정보:")
        print(f"  - 스트림명: {stream_name}")
        print(f"  - 샤드 수: {len(shards)}")
        print(f"  - 상태: {response['StreamDescription']['StreamStatus']}")
        print()
        
        total_records = 0
        
        for shard in shards:
            shard_id = shard['ShardId']
            print(f"🔍 샤드 처리 중: {shard_id}")
            
            # 샤드 이터레이터 생성
            iterator_response = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'  # 가장 오래된 레코드부터
            )
            
            shard_iterator = iterator_response['ShardIterator']
            
            # 레코드 읽기 (여러 번 시도)
            for attempt in range(3):  # 최대 3번 시도
                records_response = kinesis.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                records = records_response['Records']
                
                if records:
                    print(f"  📋 {len(records)}개 레코드 발견:")
                    
                    for i, record in enumerate(records):
                        try:
                            data = json.loads(record['Data'])
                            print(f"    {i+1}. 사용자 {data['user_id']}: {data['event_type']} (값: {data['value']})")
                            print(f"       시간: {data['timestamp']}")
                            print(f"       시퀀스: {record['SequenceNumber']}")
                            print()
                        except json.JSONDecodeError:
                            print(f"    {i+1}. 디코딩 불가: {record['Data']}")
                    
                    total_records += len(records)
                else:
                    print(f"  📭 레코드가 없음 (시도 {attempt + 1}/3)")
                
                # 다음 이터레이터 확인
                shard_iterator = records_response.get('NextShardIterator')
                if not shard_iterator:
                    break
                
                time.sleep(1)  # 1초 대기
        
        print(f"🎯 총 {total_records}개 레코드 처리 완료")
        
    except Exception as e:
        print(f"❌ 데이터 수신 오류: {e}")

if __name__ == "__main__":
    import sys
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'lab-data-stream'
    consume_test_data(stream_name)
