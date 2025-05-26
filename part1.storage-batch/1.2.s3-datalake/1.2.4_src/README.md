# 1.2.4 실습: S3 버킷 실습 가이드

## 실습 순서

### 1. S3 버킷 생성
```bash
chmod +x 1.2.4_src/setup_s3_bucket.sh
./1.2.4_src/setup_s3_bucket.sh
```

### 2. 샘플 데이터 생성 및 업로드
```bash
BUCKET_NAME=$(cat ~/BUCKET_NAME)
python 1.2.4_src/generate_data.py $BUCKET_NAME
```

### 3. 라이프사이클 정책 적용
```bash
chmod +x 1.2.4_src/apply_lifecycle.sh
./1.2.4_src/apply_lifecycle.sh
```

### 4. 성능 테스트
```bash
python 1.2.4_src/test_performance.py $BUCKET_NAME
```

### 5. 정리
```bash
chmod +x 1.2.4_src/cleanup.sh
./1.2.4_src/cleanup.sh
```

## 포함된 파일
* `setup_s3_bucket.sh`: S3 버킷 생성 및 기본 설정
* `generate_data.py`: 파티션된 샘플 데이터 생성
* `lifecycle-policy.json`: S3 라이프사이클 정책
* `apply_lifecycle.sh`: 정책 적용 및 데이터 확인
* `test_performance.py`: 파티션 성능 테스트
* `cleanup.sh`: 리소스 정리

## 학습 목표
* S3 버킷 생성 및 파티션 구조 설계
* 라이프사이클 정책을 통한 비용 최적화
* 파티션 프루닝 성능 개선 확인
