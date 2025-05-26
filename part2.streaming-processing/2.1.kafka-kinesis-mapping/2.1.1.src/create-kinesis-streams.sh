#!/bin/bash

echo "=== Kinesis Data Streams 생성 및 설정 ==="
echo

# 스트림 생성 함수
create_kinesis_stream() {
    local stream_name=$1
    local shard_count=$2
    local stream_mode=${3:-PROVISIONED}
    
    echo "1. Kinesis 스트림 생성: $stream_name"
    
    if [ "$stream_mode" = "ON_DEMAND" ]; then
        aws kinesis create-stream \
            --stream-name $stream_name \
            --stream-mode-details StreamMode=ON_DEMAND
    else
        aws kinesis create-stream \
            --stream-name $stream_name \
            --shard-count $shard_count \
            --stream-mode-details StreamMode=PROVISIONED
    fi
    
    if [ $? -eq 0 ]; then
        echo "✓ 스트림 생성 요청 완료"
    else
        echo "✗ 스트림 생성 실패"
        return 1
    fi
}

# 스트림 상태 확인 함수
wait_for_stream_active() {
    local stream_name=$1
    local max_wait=300  # 5분
    local wait_time=0
    
    echo "2. 스트림 활성화 대기: $stream_name"
    
    while [ $wait_time -lt $max_wait ]; do
        local status=$(aws kinesis describe-stream \
            --stream-name $stream_name \
            --query 'StreamDescription.StreamStatus' \
            --output text 2>/dev/null)
        
        if [ "$status" = "ACTIVE" ]; then
            echo "✓ 스트림 활성화 완료"
            return 0
        elif [ "$status" = "CREATING" ]; then
            echo "⏳ 스트림 생성 중... (${wait_time}초 경과)"
        else
            echo "⚠ 스트림 상태: $status"
        fi
        
        sleep 10
        wait_time=$((wait_time + 10))
    done
    
    echo "✗ 스트림 활성화 시간 초과"
    return 1
}

# 스트림 정보 출력 함수
show_stream_info() {
    local stream_name=$1
    
    echo "3. 스트림 정보 확인: $stream_name"
    
    local stream_info=$(aws kinesis describe-stream --stream-name $stream_name)
    
    echo "스트림 상세 정보:"
    echo "$stream_info" | jq -r '.StreamDescription | {
        StreamName: .StreamName,
        StreamStatus: .StreamStatus,
        StreamModeDetails: .StreamModeDetails,
        ShardCount: (.Shards | length),
        RetentionPeriodHours: .RetentionPeriodHours,
        StreamCreationTimestamp: .StreamCreationTimestamp
    }'
    
    echo
    echo "샤드 목록:"
    echo "$stream_info" | jq -r '.StreamDescription.Shards[] | {
        ShardId: .ShardId,
        HashKeyRange: .HashKeyRange,
        SequenceNumberRange: .SequenceNumberRange
    }'
}

# 스트림 태그 설정 함수
set_stream_tags() {
    local stream_name=$1
    
    echo "4. 스트림 태그 설정"
    
    aws kinesis add-tags-to-stream \
        --stream-name $stream_name \
        --tags Environment=development,Project=bigdata-pipeline,Owner=data-team
    
    if [ $? -eq 0 ]; then
        echo "✓ 태그 설정 완료"
    else
        echo "✗ 태그 설정 실패"
    fi
}

# 보존 기간 설정 함수
set_retention_period() {
    local stream_name=$1
    local retention_hours=${2:-24}
    
    echo "5. 데이터 보존 기간 설정: ${retention_hours}시간"
    
    aws kinesis increase-stream-retention-period \
        --stream-name $stream_name \
        --retention-period-hours $retention_hours
    
    if [ $? -eq 0 ]; then
        echo "✓ 보존 기간 설정 완료"
    else
        echo "✗ 보존 기간 설정 실패"
    fi
}

# 스트림 테스트 함수
test_stream() {
    local stream_name=$1
    
    echo "6. 스트림 테스트"
    
    # 테스트 레코드 전송
    local test_data='{"user_id": 123, "event": "test", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'
    
    echo "테스트 데이터 전송..."
    local put_result=$(aws kinesis put-record \
        --stream-name $stream_name \
        --partition-key "test-key" \
        --data "$test_data")
    
    if [ $? -eq 0 ]; then
        local sequence_number=$(echo "$put_result" | jq -r '.SequenceNumber')
        local shard_id=$(echo "$put_result" | jq -r '.ShardId')
        echo "✓ 테스트 레코드 전송 성공"
        echo "  샤드 ID: $shard_id"
        echo "  시퀀스 번호: $sequence_number"
        
        # 데이터 읽기 테스트
        echo "테스트 데이터 읽기..."
        test_read_stream "$stream_name" "$shard_id"
    else
        echo "✗ 테스트 레코드 전송 실패"
    fi
}

# 스트림 읽기 테스트 함수
test_read_stream() {
    local stream_name=$1
    local shard_id=$2
    
    # 샤드 이터레이터 생성
    local iterator_result=$(aws kinesis get-shard-iterator \
        --stream-name $stream_name \
        --shard-id $shard_id \
        --shard-iterator-type TRIM_HORIZON)
    
    if [ $? -ne 0 ]; then
        echo "✗ 샤드 이터레이터 생성 실패"
        return 1
    fi
    
    local shard_iterator=$(echo "$iterator_result" | jq -r '.ShardIterator')
    
    # 레코드 읽기
    local records_result=$(aws kinesis get-records \
        --shard-iterator "$shard_iterator")
    
    if [ $? -eq 0 ]; then
        local record_count=$(echo "$records_result" | jq -r '.Records | length')
        echo "✓ 테스트 데이터 읽기 성공: ${record_count}개 레코드"
        
        if [ $record_count -gt 0 ]; then
            echo "첫 번째 레코드 내용:"
            echo "$records_result" | jq -r '.Records[0].Data' | base64 -d
        fi
    else
        echo "✗ 테스트 데이터 읽기 실패"
    fi
}

# 스트림 성능 모니터링 설정
setup_monitoring() {
    local stream_name=$1
    
    echo "7. 모니터링 설정"
    
    # CloudWatch 메트릭 확인
    echo "현재 스트림 메트릭:"
    aws cloudwatch get-metric-statistics \
        --namespace AWS/Kinesis \
        --metric-name IncomingRecords \
        --dimensions Name=StreamName,Value=$stream_name \
        --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
        --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
        --period 3600 \
        --statistics Sum \
        --query 'Datapoints[0].Sum' \
        --output text
}

# 메인 실행 함수
main() {
    local stream_name=${1:-"test-stream"}
    local shard_count=${2:-2}
    local stream_mode=${3:-"PROVISIONED"}
    local retention_hours=${4:-24}
    
    echo "Kinesis Data Streams 설정 시작"
    echo "스트림명: $stream_name"
    echo "샤드 수: $shard_count"
    echo "스트림 모드: $stream_mode"
    echo "보존 기간: ${retention_hours}시간"
    echo
    
    # 기존 스트림 존재 확인
    if aws kinesis describe-stream --stream-name $stream_name >/dev/null 2>&1; then
        echo "⚠ 스트림이 이미 존재함: $stream_name"
        echo "기존 스트림 정보를 표시합니다."
        show_stream_info "$stream_name"
        return 0
    fi
    
    # 스트림 생성 및 설정
    create_kinesis_stream "$stream_name" "$shard_count" "$stream_mode" && \
    wait_for_stream_active "$stream_name" && \
    show_stream_info "$stream_name" && \
    set_stream_tags "$stream_name" && \
    set_retention_period "$stream_name" "$retention_hours" && \
    test_stream "$stream_name" && \
    setup_monitoring "$stream_name"
    
    if [ $? -eq 0 ]; then
        echo
        echo "🎉 Kinesis Data Streams 설정 완료!"
        echo "스트림명: $stream_name"
        echo "AWS 콘솔에서 확인: https://console.aws.amazon.com/kinesis"
    else
        echo
        echo "❌ 설정 중 오류 발생"
        exit 1
    fi
}

# 사용법 출력
usage() {
    echo "사용법: $0 [스트림명] [샤드수] [모드] [보존시간]"
    echo
    echo "예시:"
    echo "  $0                                    # 기본 설정으로 생성"
    echo "  $0 my-stream 4 PROVISIONED 48        # 4샤드, 48시간 보존"
    echo "  $0 auto-stream 0 ON_DEMAND 168       # 온디맨드 모드, 7일 보존"
    echo
    echo "매개변수:"
    echo "  스트림명    : Kinesis 스트림 이름 (기본: test-stream)"
    echo "  샤드수      : 프로비저닝 모드 샤드 수 (기본: 2)"
    echo "  모드        : PROVISIONED 또는 ON_DEMAND (기본: PROVISIONED)"
    echo "  보존시간    : 데이터 보존 시간(시간 단위) (기본: 24)"
}

# 도움말 옵션 처리
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
    exit 0
fi

# 필수 도구 확인
check_prerequisites() {
    echo "사전 요구사항 확인..."
    
    # AWS CLI 확인
    if ! command -v aws &> /dev/null; then
        echo "❌ AWS CLI가 설치되지 않음"
        echo "설치 방법: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        exit 1
    fi
    
    # jq 확인
    if ! command -v jq &> /dev/null; then
        echo "❌ jq가 설치되지 않음"
        echo "설치: sudo apt-get install jq 또는 brew install jq"
        exit 1
    fi
    
    # AWS 자격 증명 확인
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        echo "❌ AWS 자격 증명이 구성되지 않음"
        echo "실행: aws configure"
        exit 1
    fi
    
    echo "✓ 모든 사전 요구사항 충족"
    echo
}

# 정리 함수
cleanup() {
    local stream_name=$1
    
    if [ -z "$stream_name" ]; then
        echo "스트림명이 필요합니다."
        return 1
    fi
    
    echo "스트림 삭제: $stream_name"
    read -p "정말 삭제하시겠습니까? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        aws kinesis delete-stream --stream-name "$stream_name"
        echo "✓ 스트림 삭제 요청 완료"
    else
        echo "삭제 취소됨"
    fi
}

# 정리 모드 처리
if [ "$1" = "cleanup" ]; then
    cleanup "$2"
    exit 0
fi

# 메인 실행
check_prerequisites
main "$@"
