import boto3
import json
import time  # ← 추가됨

kinesis = boto3.client('kinesis', region_name='ap-northeast-2')

def consume_data():
    stream_description = kinesis.describe_stream(StreamName='test-stream')
    shard_id = stream_description['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator = kinesis.get_shard_iterator(
        StreamName='test-stream',
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )['ShardIterator']

    while True:
        response = kinesis.get_records(ShardIterator=shard_iterator)

        for record in response['Records']:
            data = json.loads(record['Data'])
            print(f"Received: {data}")

        shard_iterator = response['NextShardIterator']

        if not response['Records']:
            time.sleep(1)

consume_data()
