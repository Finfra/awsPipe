# AWS BigData Pipeline
## Part1. AWS 빅데이터 저장 및 배치처리
### 1.1. 오픈소스 → AWS 핵심 서비스 매핑
#### 1.1.1. Hadoop HDFS → S3
#### 1.1.2. Apache Spark → EMR
#### 1.1.3. 실습: AWS IAM 설정 및 기본 환경 구성

### 1.2. S3 데이터 레이크 구축
#### 1.2.1. S3 버킷 및 스토리지 클래스
#### 1.2.2. 파티셔닝 및 압축 전략
#### 1.2.3. 데이터 레이크 아키텍처 설계
#### 1.2.4. 실습: S3 버킷 생성 및 파티션된 데이터 저장
#### 1.2.5. 실습: 다양한 파일 포맷 성능 비교

### 1.3. EMR 클러스터 및 Spark 처리
#### 1.3.1. EMR 클러스터 구성 및 관리
#### 1.3.2. Jupyter Notebook 연동
#### 1.3.3. Spark DataFrame API 활용
#### 1.3.4. 대용량 데이터 배치 처리
#### 1.3.5. 실습: EMR 클러스터 생성 및 Spark Job 실행
#### 1.3.6. 실습: S3 데이터 읽기/쓰기 최적화


## Part2. AWS 실시간 스트리밍 처리 
### 2.1. Apache Kafka → Kinesis 매핑
#### 2.1.1. Kinesis Data Streams vs Data Firehose
#### 2.1.2. 파티션 및 샤드 관리
#### 2.1.3. 실습: Kinesis Data Streams 생성

### 2.2. 실시간 데이터 수집 및 처리
#### 2.2.1. 다양한 소스에서 Kinesis로 데이터 전송
#### 2.2.2. Kinesis Analytics를 통한 실시간 분석
#### 2.2.3. S3/EMR 연동 파이프라인
#### 2.2.4. 실습: 실시간 로그 데이터 스트리밍
#### 2.2.5. 실습: Kinesis → S3 → EMR 파이프라인 구축

### 2.3. 스트리밍 데이터 변환 및 저장
#### 2.3.1. Kinesis Data Firehose 변환 기능
#### 2.3.2. 압축 및 포맷 변환
#### 2.3.3. 에러 처리 전략
#### 2.3.4. 실습: 실시간 ETL 파이프라인 구현
#### 2.3.5. 실습: 스트리밍 데이터 품질 모니터링

## Part3. AWS 데이터 시각화 및 모니터링
### 3.1. Grafana → QuickSight 매핑
#### 3.1.1. QuickSight SPICE 엔진
#### 3.1.2. 다중 데이터 소스 연동 (S3, EMR 결과)
#### 3.1.3. 대시보드 설계 및 구축
#### 3.1.4. 실습: S3/EMR 데이터 기반 BI 대시보드 생성

### 3.2. Prometheus → CloudWatch 매핑  
#### 3.2.1. CloudWatch 메트릭 및 로그
#### 3.2.2. 커스텀 메트릭 생성
#### 3.2.3. 알람 및 알림 설정
#### 3.2.4. EMR/Kinesis 서비스 모니터링
#### 3.2.5. 실습: 빅데이터 파이프라인 통합 모니터링
#### 3.2.6. 실습: 장애 감지 및 자동 알림 설정


**참고 링크**
- [Amazon S3 공식 문서](https://docs.aws.amazon.com/ko_kr/s3/)
- [Amazon EMR 공식 문서](https://docs.aws.amazon.com/ko_kr/emr/)
- [AWS Data Lake 구축 가이드](https://docs.aws.amazon.com/ko_kr/analytics/latest/big-data-datalakes/welcome.html)
- [Amazon Kinesis 공식 문서](https://docs.aws.amazon.com/ko_kr/kinesis/)
- [Kinesis Data Streams vs Firehose](https://docs.aws.amazon.com/ko_kr/streams/latest/dev/introduction.html)
- [AWS 실시간 데이터 분석](https://aws.amazon.com/ko/real-time-analytics/)
- [Amazon QuickSight 공식 문서](https://docs.aws.amazon.com/ko_kr/quicksight/)
- [Amazon CloudWatch 공식 문서](https://docs.aws.amazon.com/ko_kr/cloudwatch/)
- [AWS Monitoring & Observability](https://aws.amazon.com/ko/monitoring-and-observability/)


