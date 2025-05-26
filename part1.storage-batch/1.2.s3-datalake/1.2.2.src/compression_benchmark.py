import time
import os

def benchmark_compression(df, codec):
    start_time = time.time()
    
    # 압축 저장
    output_path = f"test-{codec}.parquet"
    df.write.option("compression", codec).parquet(output_path)
    
    write_time = time.time() - start_time
    file_size = get_directory_size(output_path)
    
    # 압축 해제 (읽기)
    start_time = time.time()
    df_read = spark.read.parquet(output_path)
    df_read.count()  # 실제 읽기 실행
    read_time = time.time() - start_time
    
    return {
        'codec': codec,
        'write_time': write_time,
        'read_time': read_time,
        'file_size_mb': file_size / 1024 / 1024,
        'total_time': write_time + read_time
    }
