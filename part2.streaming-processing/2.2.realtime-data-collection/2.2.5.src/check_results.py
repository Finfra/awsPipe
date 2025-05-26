#!/usr/bin/env python3
"""
파이프라인 처리 결과 확인 스크립트
S3에 저장된 처리 결과 데이터를 분석하고 출력
"""

import boto3
import pandas as pd
import os
import json
from datetime import datetime
import argparse

def download_processed_data(bucket_name, local_dir='./pipeline-results'):
    """S3에서 처리된 데이터 다운로드"""
    
    s3 = boto3.client('s3')
    
    # 로컬 디렉토리 생성
    os.makedirs(local_dir, exist_ok=True)
    
    try:
        # processed-data 폴더의 모든 파일 나열
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='processed-data/'
        )
        
        if 'Contents' not in response:
            print("처리된 데이터가 없습니다.")
            return []
        
        downloaded_files = []
        
        for obj in response['Contents']:
            key = obj['Key']
            local_path = os.path.join(local_dir, key.replace('processed-data/', ''))
            
            # 로컬 디렉토리 생성
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # 파일 다운로드
            s3.download_file(bucket_name, key, local_path)
            downloaded_files.append(local_path)
            
            print(f"다운로드: {key} → {local_path}")
        
        return downloaded_files
        
    except Exception as e:
        print(f"데이터 다운로드 실패: {e}")
        return []

def analyze_log_statistics(data_dir):
    """로그 통계 분석"""
    
    stats_pattern = os.path.join(data_dir, '**/log_statistics/**/*.parquet')
    
    try:
        # 로그 통계 파일 찾기
        import glob
        stats_files = glob.glob(stats_pattern, recursive=True)
        
        if not stats_files:
            print("로그 통계 파일을 찾을 수 없습니다.")
            return
        
        # Parquet 파일 읽기
        for file_path in stats_files:
            print(f"\n=== 로그 통계 분석: {file_path} ===")
            
            df = pd.read_parquet(file_path)
            
            print("로그 타입별 통계:")
            print(df.to_string(index=False))
            
            # 총계 계산
            total_events = df['total_events'].sum()
            total_unique_ips = df['unique_ips'].sum()
            
            print(f"\n총 이벤트 수: {total_events:,}")
            print(f"총 고유 IP 수: {total_unique_ips:,}")
            
    except Exception as e:
        print(f"로그 통계 분석 실패: {e}")

def analyze_hourly_statistics(data_dir):
    """시간대별 통계 분석"""
    
    hourly_pattern = os.path.join(data_dir, '**/hourly_statistics/**/*.parquet')
    
    try:
        import glob
        hourly_files = glob.glob(hourly_pattern, recursive=True)
        
        if not hourly_files:
            print("시간대별 통계 파일을 찾을 수 없습니다.")
            return
        
        for file_path in hourly_files:
            print(f"\n=== 시간대별 통계 분석: {file_path} ===")
            
            df = pd.read_parquet(file_path)
            
            # 상위 10개 시간대
            top_hours = df.nlargest(10, 'events_per_hour')
            
            print("상위 10개 시간대별 이벤트:")
            print(top_hours.to_string(index=False))
            
            # 로그 타입별 시간대 평균
            avg_by_type = df.groupby('log_type')['events_per_hour'].agg(['mean', 'max', 'sum'])
            
            print(f"\n로그 타입별 시간대 통계:")
            print(avg_by_type.to_string())
            
    except Exception as e:
        print(f"시간대별 통계 분석 실패: {e}")

def analyze_status_statistics(data_dir):
    """HTTP 상태 코드 통계 분석"""
    
    status_pattern = os.path.join(data_dir, '**/status_statistics/**/*.parquet')
    
    try:
        import glob
        status_files = glob.glob(status_pattern, recursive=True)
        
        if not status_files:
            print("상태 코드 통계 파일을 찾을 수 없습니다.")
            return
        
        for file_path in status_files:
            print(f"\n=== HTTP 상태 코드 분석: {file_path} ===")
            
            df = pd.read_parquet(file_path)
            
            print("상태 코드별 분포:")
            print(df.to_string(index=False))
            
            # 에러율 계산
            total_requests = df['status_count'].sum()
            error_requests = df[df['status_code'] >= 400]['status_count'].sum()
            error_rate = (error_requests / total_requests * 100) if total_requests > 0 else 0
            
            print(f"\n총 요청 수: {total_requests:,}")
            print(f"에러 요청 수: {error_requests:,}")
            print(f"에러율: {error_rate:.2f}%")
            
    except Exception as e:
        print(f"상태 코드 분석 실패: {e}")

def analyze_top_ips(data_dir):
    """상위 IP 분석"""
    
    ips_pattern = os.path.join(data_dir, '**/top_ips/**/*.parquet')
    
    try:
        import glob
        ips_files = glob.glob(ips_pattern, recursive=True)
        
        if not ips_files:
            print("상위 IP 파일을 찾을 수 없습니다.")
            return
        
        for file_path in ips_files:
            print(f"\n=== 상위 IP 주소 분석: {file_path} ===")
            
            df = pd.read_parquet(file_path)
            
            # 상위 20개 IP
            top_20_ips = df.head(20)
            
            print("상위 20개 IP 주소:")
            print(top_20_ips.to_string(index=False))
            
            # 통계 요약
            total_requests = df['request_count'].sum()
            top_10_requests = df.head(10)['request_count'].sum()
            concentration = (top_10_requests / total_requests * 100) if total_requests > 0 else 0
            
            print(f"\n총 요청 수: {total_requests:,}")
            print(f"상위 10개 IP 요청 수: {top_10_requests:,}")
            print(f"상위 10개 IP 집중도: {concentration:.2f}%")
            
    except Exception as e:
        print(f"상위 IP 분석 실패: {e}")

def analyze_application_logs(data_dir):
    """애플리케이션 로그 분석"""
    
    app_pattern = os.path.join(data_dir, '**/application_analysis/**/*.parquet')
    
    try:
        import glob
        app_files = glob.glob(app_pattern, recursive=True)
        
        for file_path in app_files:
            print(f"\n=== 애플리케이션 로그 분석: {file_path} ===")
            
            if 'log_level_statistics' in file_path:
                df = pd.read_parquet(file_path)
                print("로그 레벨별 통계:")
                print(df.to_string(index=False))
                
            elif 'error_analysis' in file_path:
                df = pd.read_parquet(file_path)
                print("에러 분석 (상위 10개):")
                print(df.head(10).to_string(index=False))
                
            elif 'user_activity' in file_path:
                df = pd.read_parquet(file_path)
                print("사용자 활동 통계 (상위 10명):")
                print(df.head(10).to_string(index=False))
                
    except Exception as e:
        print(f"애플리케이션 로그 분석 실패: {e}")

def check_processing_metadata(data_dir):
    """처리 메타데이터 확인"""
    
    metadata_pattern = os.path.join(data_dir, '**/processing_metadata/**/*.parquet')
    
    try:
        import glob
        metadata_files = glob.glob(metadata_pattern, recursive=True)
        
        for file_path in metadata_files:
            print(f"\n=== 처리 메타데이터: {file_path} ===")
            
            df = pd.read_parquet(file_path)
            
            for _, row in df.iterrows():
                print(f"총 처리 레코드: {row['total_records']:,}")
                print(f"처리 시각: {row['processing_timestamp']}")
                print(f"출력 경로: {row['output_path']}")
                
    except Exception as e:
        print(f"메타데이터 확인 실패: {e}")

def generate_summary_report(data_dir):
    """종합 리포트 생성"""
    
    print("\n" + "="*60)
    print("파이프라인 처리 결과 종합 리포트")
    print("="*60)
    
    try:
        # 모든 분석 실행
        analyze_log_statistics(data_dir)
        analyze_hourly_statistics(data_dir)
        analyze_status_statistics(data_dir)
        analyze_top_ips(data_dir)
        analyze_application_logs(data_dir)
        check_processing_metadata(data_dir)
        
        print(f"\n리포트 생성 완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"리포트 생성 실패: {e}")

def list_s3_processed_data(bucket_name):
    """S3의 처리된 데이터 목록 조회"""
    
    s3 = boto3.client('s3')
    
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='processed-data/'
        )
        
        if 'Contents' not in response:
            print("S3에서 처리된 데이터를 찾을 수 없습니다.")
            return
        
        print(f"\n=== S3 처리된 데이터 목록 ({bucket_name}) ===")
        
        total_size = 0
        file_count = 0
        
        for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']
            modified = obj['LastModified']
            
            print(f"{key:<60} {size:>10} bytes {modified}")
            
            total_size += size
            file_count += 1
        
        print(f"\n총 {file_count}개 파일, 총 크기: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
        
    except Exception as e:
        print(f"S3 데이터 목록 조회 실패: {e}")

def main():
    """메인 실행 함수"""
    
    parser = argparse.ArgumentParser(description='파이프라인 결과 확인')
    parser.add_argument('--bucket-name', required=True, help='S3 버킷명')
    parser.add_argument('--download', action='store_true', help='S3에서 데이터 다운로드')
    parser.add_argument('--local-dir', default='./pipeline-results', help='로컬 저장 디렉토리')
    parser.add_argument('--list-only', action='store_true', help='S3 파일 목록만 조회')
    
    args = parser.parse_args()
    
    if args.list_only:
        # S3 파일 목록만 조회
        list_s3_processed_data(args.bucket_name)
        return
    
    if args.download:
        # S3에서 데이터 다운로드
        print("S3에서 처리된 데이터 다운로드 중...")
        downloaded_files = download_processed_data(args.bucket_name, args.local_dir)
        
        if not downloaded_files:
            print("다운로드할 데이터가 없습니다.")
            return
        
        print(f"\n{len(downloaded_files)}개 파일 다운로드 완료")
    
    # 로컬 데이터 분석
    if os.path.exists(args.local_dir):
        generate_summary_report(args.local_dir)
    else:
        print(f"로컬 데이터 디렉토리가 없습니다: {args.local_dir}")
        print("--download 옵션으로 먼저 데이터를 다운로드하세요.")

if __name__ == "__main__":
    main()
