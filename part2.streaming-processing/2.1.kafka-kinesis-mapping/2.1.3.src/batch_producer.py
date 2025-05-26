#!/usr/bin/env python3
import boto3
import json
from datetime import datetime

def send_batch_data(stream_name='lab-data-stream', batch_size=100):
    """배치 단위로 데이터 전송"""
    kinesis = boto3.client('kinesis')
    
    print(f"📦 {batch_size}개 레코드를 배치로 {stream_name}에 전송...")
    
    # 배치 레코드 준비 (최대 500개)
    records = []
    for i in range(min(batch_size, 500)):
        record = {
            'Data': json.dumps({
                'timestamp': datetime.utcnow().isoformat(),
                'batch_id': 'batch_001',
                'record_id': i,
                'value': i * 5,
                'category': f'category_{i % 5}',
                'priority': 'high' if i % 10 == 0 else 'normal'
            }),
            'PartitionKey': f'batch_key_{i % 10}'
        }
        records.append(record)
    
    try:
        # 배치 전송
        response = kinesis.put_records(
            Records=records,
            StreamName=stream_name
        )
        
        success_count = len(records) - response['FailedRecordCount']
        
        print(f"✅ 전송 완료: {success_count}/{len(records)}개 성공")
        print(f"❌ 실패 수: {response['FailedRecordCount']}")
        
        # 실패한 레코드 분석
        if response['FailedRecordCount'] > 0:
            failed_records = []
            error_codes = {}
            
            for i, record_result in enumerate(response['Records']):
                if 'ErrorCode' in record_result:
                    failed_records.append(records[i])
                    error_code = record_result['ErrorCode']
                    error_codes[error_code] = error_codes.get(error_code, 0) + 1
            
            print(f"📊 오류 분석:")
            for error_code, count in error_codes.items():
                print(f"  - {error_code}: {count}개")
            
            # 실패한 레코드 재시도
            if failed_records:
                print(f"🔄 {len(failed_records)}개 레코드 재시도...")
                retry_response = kinesis.put_records(
                    Records=failed_records,
                    StreamName=stream_name
                )
                
                retry_success = len(failed_records) - retry_response['FailedRecordCount']
                print(f"✅ 재시도 성공: {retry_success}/{len(failed_records)}개")
        
        return success_count
        
    except Exception as e:
        print(f"❌ 배치 전송 오류: {e}")
        return 0

if __name__ == "__main__":
    import sys
    
    stream_name = sys.argv[1] if len(sys.argv) > 1 else 'lab-data-stream'
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    send_batch_data(stream_name, batch_size)
