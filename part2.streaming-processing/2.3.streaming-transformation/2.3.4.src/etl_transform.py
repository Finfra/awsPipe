import json
import base64
from datetime import datetime

def lambda_handler(event, context):
    output_records = []
    
    for record in event['records']:
        try:
            # 데이터 디코딩
            raw_data = base64.b64decode(record['data']).decode('utf-8')
            payload = json.loads(raw_data)
            
            # ETL 변환
            transformed = transform_data(payload)
            
            # 출력 인코딩
            output_data = json.dumps(transformed) + '\n'
            encoded_data = base64.b64encode(output_data.encode('utf-8')).decode('utf-8')
            
            output_records.append({
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': encoded_data
            })
            
        except Exception as e:
            output_records.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed'
            })
    
    return {'records': output_records}

def transform_data(payload):
    """데이터 변환 로직"""
    
    transformed = {
        'timestamp': payload.get('timestamp', datetime.utcnow().isoformat()),
        'event_type': payload.get('event_type', 'unknown'),
        'user_id': payload.get('user_id'),
        'ip_address': anonymize_ip(payload.get('ip_address')),
        'status_code': payload.get('status_code'),
        'response_size': payload.get('response_size', 0),
        'processed_at': datetime.utcnow().isoformat(),
        'etl_version': '1.0'
    }
    
    # HTTP 상태 분류
    if transformed['status_code']:
        code = int(transformed['status_code'])
        if 200 <= code < 300:
            transformed['status_category'] = 'Success'
        elif 400 <= code < 500:
            transformed['status_category'] = 'Client Error'
        elif 500 <= code < 600:
            transformed['status_category'] = 'Server Error'
        else:
            transformed['status_category'] = 'Other'
    
    return transformed

def anonymize_ip(ip_address):
    """IP 주소 익명화"""
    if not ip_address:
        return None
    
    parts = ip_address.split('.')
    if len(parts) == 4:
        return '.'.join(parts[:3] + ['xxx'])
    return 'anonymized'
