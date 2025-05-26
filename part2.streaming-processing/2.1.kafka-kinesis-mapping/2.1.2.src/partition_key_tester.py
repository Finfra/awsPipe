import hashlib
import uuid
from datetime import datetime

class PartitionKeyStrategy:
    @staticmethod
    def user_based_key(user_id):
        """사용자 기반 파티션 키 - 사용자별 순서 보장"""
        return str(user_id)
    
    @staticmethod
    def hash_based_key(identifier):
        """해시 기반 파티션 키 - 균등 분산"""
        return hashlib.md5(str(identifier).encode()).hexdigest()
    
    @staticmethod
    def time_based_key(timestamp, buckets=10):
        """시간 기반 파티션 키 - 시간대별 분산"""
        minute_bucket = (timestamp.minute // (60 // buckets))
        return f"{timestamp.hour:02d}_{minute_bucket:02d}"
    
    @staticmethod
    def composite_key(user_id, category):
        """복합 파티션 키 - 사용자+카테고리별 순서 보장"""
        return f"{user_id}_{category}"
    
    @staticmethod
    def random_key():
        """완전 랜덤 키 - 최대 분산"""
        return str(uuid.uuid4())

def test_partition_distribution(key_generator, sample_size=10000, shard_count=4):
    """파티션 키 분산 테스트"""
    keys = {}
    
    for i in range(sample_size):
        if key_generator == PartitionKeyStrategy.user_based_key:
            key = key_generator(i % 100)  # 100명의 사용자
        elif key_generator == PartitionKeyStrategy.time_based_key:
            key = key_generator(datetime.now())
        elif key_generator == PartitionKeyStrategy.composite_key:
            user_id = i % 100
            category = ['A', 'B', 'C'][i % 3]
            key = key_generator(user_id, category)
        else:
            key = key_generator(i)
        
        # 해시값을 샤드 수로 나눈 나머지로 분산 계산
        shard_id = int(hashlib.md5(key.encode()).hexdigest(), 16) % shard_count
        keys[shard_id] = keys.get(shard_id, 0) + 1
    
    print(f"\n=== 파티션 키 분산 결과 ({key_generator.__name__}) ===")
    for shard_id in range(shard_count):
        count = keys.get(shard_id, 0)
        percentage = (count / sample_size) * 100
        bar = '█' * int(percentage // 2)  # 시각적 표현
        print(f"샤드 {shard_id}: {count:,} ({percentage:5.1f}%) {bar}")
    
    # 분산 균등성 계산 (표준편차)
    values = [keys.get(i, 0) for i in range(shard_count)]
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    std_dev = variance ** 0.5
    cv = (std_dev / mean) * 100 if mean > 0 else 0  # 변동계수
    
    print(f"분산 균등성: {cv:.1f}% (낮을수록 균등)")
    return cv

def compare_all_strategies(sample_size=10000, shard_count=4):
    """모든 파티션 키 전략 비교"""
    print(f"🔍 파티션 키 전략 비교 분석")
    print(f"샘플 크기: {sample_size:,}, 샤드 수: {shard_count}")
    print("=" * 60)
    
    strategies = [
        PartitionKeyStrategy.hash_based_key,
        PartitionKeyStrategy.user_based_key,
        PartitionKeyStrategy.time_based_key,
        PartitionKeyStrategy.composite_key,
        PartitionKeyStrategy.random_key
    ]
    
    results = {}
    for strategy in strategies:
        cv = test_partition_distribution(strategy, sample_size, shard_count)
        results[strategy.__name__] = cv
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("📊 전략별 분산 균등성 순위 (낮을수록 좋음)")
    print("=" * 60)
    
    sorted_results = sorted(results.items(), key=lambda x: x[1])
    for i, (strategy, cv) in enumerate(sorted_results, 1):
        rating = "⭐" * (6 - min(5, int(cv // 5)))
        print(f"{i}. {strategy:20} {cv:6.1f}% {rating}")
    
    return results

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'compare':
            compare_all_strategies()
        else:
            strategy_name = sys.argv[1]
            if hasattr(PartitionKeyStrategy, strategy_name):
                strategy = getattr(PartitionKeyStrategy, strategy_name)
                test_partition_distribution(strategy)
            else:
                print(f"Unknown strategy: {strategy_name}")
                print("Available strategies:", [m for m in dir(PartitionKeyStrategy) if not m.startswith('_')])
    else:
        print("=== 파티션 키 전략별 분산 테스트 ===")
        test_partition_distribution(PartitionKeyStrategy.hash_based_key)
        test_partition_distribution(PartitionKeyStrategy.user_based_key)
        test_partition_distribution(PartitionKeyStrategy.random_key)
