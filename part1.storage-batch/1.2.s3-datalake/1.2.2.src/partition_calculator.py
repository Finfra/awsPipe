def calculate_optimal_partitions(total_size_gb, target_partition_size_mb=128):
    """최적 파티션 수 계산"""
    total_size_mb = total_size_gb * 1024
    optimal_partitions = int(total_size_mb / target_partition_size_mb)
    return max(optimal_partitions, 1)

# 사용 예시
if __name__ == "__main__":
    data_size_gb = 10
    optimal_partitions = calculate_optimal_partitions(data_size_gb)
    print(f"권장 파티션 수: {optimal_partitions}")
