import boto3
import sys

def setup_intelligent_tiering(bucket_name):
    s3 = boto3.client('s3')
    
    # Intelligent Tiering 설정
    s3.put_bucket_intelligent_tiering_configuration(
        Bucket=bucket_name,
        Id='EntireDataLake',
        IntelligentTieringConfiguration={
            'Id': 'EntireDataLake',
            'Status': 'Enabled',
            'OptionalFields': ['BucketKeyStatus']
        }
    )
    
    # Lifecycle 정책
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'DataLakeLifecycle',
                'Status': 'Enabled',
                'Filter': {'Prefix': 'raw-data/'},
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': 90,
                        'StorageClass': 'GLACIER'
                    },
                    {
                        'Days': 365,
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ]
            }
        ]
    }
    
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_config
    )

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 lifecycle_policy.py <bucket-name>")
        sys.exit(1)
    bucket_name = sys.argv[1]
    setup_intelligent_tiering(bucket_name)
