import pandas as pd
import json

def load_jsonl_data(file_path):
    """JSON Lines 데이터를 DataFrame으로 로드"""
    
    records = []
    with open(file_path, 'r') as f:
        for line in f:
            records.append(json.loads(line.strip()))
    
    df = pd.DataFrame(records)
    print(f"데이터 로드 완료: {len(df):,} 레코드")
    print(f"컬럼: {list(df.columns)}")
    print(f"메모리 사용량: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    return df

if __name__ == "__main__":
    df = load_jsonl_data('/home/ec2-user/test-data/data.jsonl')
    
    # 타입 최적화
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['user_id'] = df['user_id'].astype('int32')
    df['amount'] = df['amount'].astype('float32')
    
    # 테스트용으로 저장
    df.to_pickle('~/test_dataset.pkl')
    print("테스트 데이터셋 저장: ~/test_dataset.pkl")