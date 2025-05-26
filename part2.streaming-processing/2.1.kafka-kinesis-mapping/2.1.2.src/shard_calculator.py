import math

def calculate_required_shards(requirements):
    """필요한 샤드 수 계산"""
    
    # 처리량 기준 계산
    throughput_shards = math.ceil(requirements['mb_per_second'] / 1.0)  # 1MB/초 제한
    
    # 레코드 수 기준 계산
    records_shards = math.ceil(requirements['records_per_second'] / 1000)  # 1000 레코드/초 제한
    
    # 파티션 키 기준 계산 (순서 보장)
    partition_shards = requirements.get('partition_keys', 1)
    
    # 최대값 선택
    required_shards = max(throughput_shards, records_shards, partition_shards)
    
    # 향후 확장성을 위한 버퍼 (20% 추가)
    buffered_shards = math.ceil(required_shards * 1.2)
    
    return {
        'minimum_shards': required_shards,
        'recommended_shards': buffered_shards,
        'throughput_based': throughput_shards,
        'records_based': records_shards,
        'partition_based': partition_shards,
        'estimated_cost_per_hour': buffered_shards * 0.015
    }

if __name__ == "__main__":
    # 사용 예시
    requirements = {
        'mb_per_second': 3.5,
        'records_per_second': 2500,
        'partition_keys': 4  # 4개의 서로 다른 파티션 키
    }

    shard_plan = calculate_required_shards(requirements)
    print(f"권장 샤드 수: {shard_plan['recommended_shards']}")
    print(f"시간당 예상 비용: ${shard_plan['estimated_cost_per_hour']:.2f}")
    
    print("\n=== 상세 분석 ===")
    print(f"처리량 기준 필요 샤드: {shard_plan['throughput_based']}")
    print(f"레코드 수 기준 필요 샤드: {shard_plan['records_based']}")
    print(f"파티션 기준 필요 샤드: {shard_plan['partition_based']}")
    print(f"최소 필요 샤드: {shard_plan['minimum_shards']}")
    print(f"권장 샤드 (20% 버퍼): {shard_plan['recommended_shards']}")
