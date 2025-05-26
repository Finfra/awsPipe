#!/usr/bin/env python3
"""
Spark 성능 테스트 및 벤치마킹 스크립트
다양한 설정값과 최적화 기법의 성능을 비교 측정
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import json
import argparse
from datetime import datetime

class SparkPerformanceTester:
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.results = {}
    
    def create_spark_session(self, config_name, configs):
        """특정 설정으로 Spark 세션 생성"""
        builder = SparkSession.builder.appName(f"PerformanceTest-{config_name}")
        
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def run_benchmark_query(self, spark, query_name, query_func):
        """벤치마크 쿼리 실행 및 성능 측정"""
        print(f"실행 중: {query_name}")
        
        start_time = time.time()
        
        try:
            result = query_func(spark)
            
            # 실제 실행을 위해 액션 호출
            if hasattr(result, 'count'):
                count = result.count()
            elif hasattr(result, 'collect'):
                count = len(result.collect())
            else:
                count = 0
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            return {
                'success': True,
                'execution_time': execution_time,
                'record_count': count,
                'records_per_second': count / execution_time if execution_time > 0 else 0
            }
            
        except Exception as e:
            end_time = time.time()
            return {
                'success': False,
                'execution_time': end_time - start_time,
                'error': str(e)
            }
    
    def test_configuration(self, config_name, configs):
        """특정 설정으로 성능 테스트 실행"""
        print(f"\n=== {config_name} 테스트 ===")
        
        spark = self.create_spark_session(config_name, configs)
        config_results = {}
        
        try:
            # 데이터 로드 테스트
            def load_test(spark):
                return spark.read.parquet(self.input_path)
            
            config_results['data_load'] = self.run_benchmark_query(
                spark, "데이터 로드", load_test
            )
            
            # 기본 집계 테스트
            def basic_aggregation(spark):
                df = spark.read.parquet(self.input_path)
                return df.groupBy("category").agg(
                    count("*").alias("count"),
                    sum("value").alias("total_value")
                )
            
            config_results['basic_aggregation'] = self.run_benchmark_query(
                spark, "기본 집계", basic_aggregation
            )
            
            # 복잡한 조인 테스트 (셀프 조인)
            def complex_join(spark):
                df = spark.read.parquet(self.input_path)
                df1 = df.select("user_id", "category").distinct()
                df2 = df.groupBy("user_id").agg(count("*").alias("event_count"))
                return df1.join(df2, "user_id")
            
            config_results['complex_join'] = self.run_benchmark_query(
                spark, "복잡한 조인", complex_join
            )
            
            # 윈도우 함수 테스트
            def window_function(spark):
                from pyspark.sql.window import Window
                df = spark.read.parquet(self.input_path)
                window = Window.partitionBy("category").orderBy("timestamp")
                return df.withColumn("row_number", row_number().over(window))
            
            config_results['window_function'] = self.run_benchmark_query(
                spark, "윈도우 함수", window_function
            )
            
            # 쓰기 성능 테스트
            def write_test(spark):
                df = spark.read.parquet(self.input_path).limit(1000)
                test_output = f"{self.output_path}/test-{config_name}-{int(time.time())}"
                df.coalesce(1).write.mode("overwrite").parquet(test_output)
                return df
            
            config_results['write_performance'] = self.run_benchmark_query(
                spark, "쓰기 성능", write_test
            )
            
        finally:
            spark.stop()
        
        self.results[config_name] = config_results
        return config_results
    
    def run_all_tests(self):
        """모든 설정 조합으로 테스트 실행"""
        
        # 테스트 설정들
        test_configs = {
            'default': {
                'spark.sql.adaptive.enabled': 'false',
                'spark.sql.shuffle.partitions': '200'
            },
            'adaptive_enabled': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.shuffle.partitions': '200'
            },
            'optimized_partitions': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',  
                'spark.sql.shuffle.partitions': '100',
                'spark.sql.files.maxPartitionBytes': '134217728'
            },
            's3_optimized': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.hadoop.fs.s3a.multipart.size': '134217728',
                'spark.hadoop.fs.s3a.multipart.threshold': '134217728',
                'spark.sql.files.maxPartitionBytes': '134217728'
            },
            'memory_optimized': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.execution.arrow.pyspark.enabled': 'true',
                'spark.executor.memoryFraction': '0.8'
            },
            'full_optimized': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                'spark.sql.adaptive.localShuffleReader.enabled': 'true',
                'spark.hadoop.fs.s3a.multipart.size': '134217728',
                'spark.hadoop.fs.s3a.multipart.threshold': '134217728',
                'spark.sql.files.maxPartitionBytes': '134217728',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.parquet.compression.codec': 'snappy',
                'spark.sql.execution.arrow.pyspark.enabled': 'true'
            }
        }
        
        # 각 설정으로 테스트 실행
        for config_name, configs in test_configs.items():
            self.test_configuration(config_name, configs)
    
    def generate_report(self):
        """성능 테스트 결과 리포트 생성"""
        
        print("\n" + "="*80)
        print("성능 테스트 결과 리포트")
        print("="*80)
        
        # 테스트 항목별 성능 비교
        test_types = ['data_load', 'basic_aggregation', 'complex_join', 
                     'window_function', 'write_performance']
        
        for test_type in test_types:
            print(f"\n{test_type.replace('_', ' ').title()} 성능 비교:")
            print("-" * 60)
            
            # 성공한 테스트만 비교
            successful_results = []
            for config_name, config_result in self.results.items():
                if test_type in config_result and config_result[test_type]['success']:
                    successful_results.append({
                        'config': config_name,
                        'time': config_result[test_type]['execution_time'],
                        'records_per_sec': config_result[test_type].get('records_per_second', 0)
                    })
            
            # 실행 시간 기준 정렬
            successful_results.sort(key=lambda x: x['time'])
            
            if successful_results:
                baseline_time = successful_results[0]['time']
                
                for i, result in enumerate(successful_results):
                    speedup = baseline_time / result['time'] if result['time'] > 0 else 0
                    print(f"{i+1:2d}. {result['config']:<20} "
                          f"{result['time']:>8.2f}초 "
                          f"(x{speedup:.2f} 성능)")
        
        # 종합 성능 점수 계산
        print(f"\n종합 성능 점수:")
        print("-" * 40)
        
        config_scores = {}
        for config_name, config_result in self.results.items():
            total_time = 0
            success_count = 0
            
            for test_type in test_types:
                if test_type in config_result and config_result[test_type]['success']:
                    total_time += config_result[test_type]['execution_time']
                    success_count += 1
            
            if success_count > 0:
                avg_time = total_time / success_count
                config_scores[config_name] = avg_time
        
        # 점수 기준 정렬
        sorted_configs = sorted(config_scores.items(), key=lambda x: x[1])
        
        for i, (config_name, avg_time) in enumerate(sorted_configs):
            print(f"{i+1:2d}. {config_name:<20} {avg_time:>8.2f}초 (평균)")
        
        return self.results
    
    def save_results(self, filename=None):
        """결과를 JSON 파일로 저장"""
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"spark_performance_results_{timestamp}.json"
        
        # 결과에 메타데이터 추가
        output_data = {
            'metadata': {
                'test_date': datetime.now().isoformat(),
                'input_path': self.input_path,
                'output_path': self.output_path
            },
            'results': self.results
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n결과 저장 완료: {filename}")

def main():
    parser = argparse.ArgumentParser(description='Spark 성능 테스트')
    parser.add_argument('--input', required=True, help='입력 데이터 S3 경로')
    parser.add_argument('--output', required=True, help='출력 S3 경로')
    parser.add_argument('--output-file', help='결과 저장 파일명')
    
    args = parser.parse_args()
    
    # 성능 테스터 생성
    tester = SparkPerformanceTester(args.input, args.output)
    
    print("Spark 성능 테스트 시작...")
    print(f"입력 경로: {args.input}")
    print(f"출력 경로: {args.output}")
    
    # 모든 테스트 실행
    tester.run_all_tests()
    
    # 리포트 생성
    results = tester.generate_report()
    
    # 결과 저장
    tester.save_results(args.output_file)
    
    print("\n성능 테스트 완료!")

if __name__ == "__main__":
    main()
