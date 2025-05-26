#!/bin/bash

# Spark 작업 제출 스크립트
# 사용법: ./submit-job.sh <python-script> [추가-인자들...]

set -e

# 기본 설정
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_JOBS_DIR="$SCRIPT_DIR/spark-jobs"

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 도움말 함수
show_help() {
    echo "Spark 작업 제출 스크립트"
    echo "사용법: $0 <python-script> [추가-인자들...]"
    echo
    echo "사용 가능한 스크립트:"
    echo "  data-processing.py      - 기본 데이터 처리"
    echo "  optimized-processing.py - 최적화된 데이터 처리"
    echo "  performance-test.py     - 성능 테스트"
    echo
    echo "예시:"
    echo "  $0 data-processing.py --input s3://bucket/input/ --output s3://bucket/output/"
    echo "  $0 optimized-processing.py --input s3://bucket/input/ --output s3://bucket/output/"
    echo "  $0 performance-test.py --input s3://bucket/input/ --output s3://bucket/output/"
    echo
}

# 인자 확인
if [ $# -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

PYTHON_SCRIPT="$1"
shift  # 첫 번째 인자 제거

# 스크립트 파일 경로 확인
if [[ ! "$PYTHON_SCRIPT" =~ \.py$ ]]; then
    PYTHON_SCRIPT="${PYTHON_SCRIPT}.py"
fi

SCRIPT_PATH="$SPARK_JOBS_DIR/$PYTHON_SCRIPT"

if [ ! -f "$SCRIPT_PATH" ]; then
    echo -e "${RED}오류: 스크립트 파일을 찾을 수 없습니다: $SCRIPT_PATH${NC}"
    echo "사용 가능한 스크립트:"
    ls -1 "$SPARK_JOBS_DIR"/*.py 2>/dev/null | sed 's|.*/||' || echo "  (스크립트 없음)"
    exit 1
fi

# Terraform output에서 S3 버킷 정보 가져오기
echo -e "${YELLOW}Terraform 출력에서 설정 정보 가져오는 중...${NC}"

get_terraform_output() {
    local key="$1"
    terraform output -json 2>/dev/null | jq -r ".${key}.value" 2>/dev/null || echo ""
}

# S3 버킷 정보
S3_BUCKET=$(get_terraform_output "s3_bucket_name")
EMR_CLUSTER_ID=$(get_terraform_output "emr_cluster_id")

if [ -z "$S3_BUCKET" ]; then
    echo -e "${YELLOW}경고: Terraform output에서 S3 버킷명을 가져올 수 없습니다.${NC}"
    echo "수동으로 --input과 --output 인자를 지정하세요."
    S3_BUCKET="your-bucket-name"
fi

if [ -z "$EMR_CLUSTER_ID" ]; then
    echo -e "${YELLOW}경고: EMR 클러스터 ID를 가져올 수 없습니다.${NC}"
    echo "로컬에서 spark-submit을 사용합니다."
fi

# 기본 입력/출력 경로 설정 (인자로 지정되지 않은 경우)
DEFAULT_INPUT="s3://$S3_BUCKET/raw-data/"
DEFAULT_OUTPUT="s3://$S3_BUCKET/processed-data/$(date +%Y%m%d_%H%M%S)"

# 인자에 --input이나 --output이 없으면 기본값 추가
ARGS="$@"
if [[ ! "$ARGS" =~ --input ]]; then
    ARGS="--input $DEFAULT_INPUT $ARGS"
fi
if [[ ! "$ARGS" =~ --output ]]; then
    ARGS="--output $DEFAULT_OUTPUT $ARGS"
fi

echo -e "${GREEN}작업 정보:${NC}"
echo "  스크립트: $PYTHON_SCRIPT"
echo "  S3 버킷: $S3_BUCKET"
echo "  EMR 클러스터: ${EMR_CLUSTER_ID:-"로컬 실행"}"
echo "  인자: $ARGS"
echo

# 스크립트를 S3에 업로드 (EMR에서 접근 가능하도록)
SCRIPT_S3_PATH="s3://$S3_BUCKET/scripts/$PYTHON_SCRIPT"
echo -e "${YELLOW}스크립트를 S3에 업로드 중...${NC}"
aws s3 cp "$SCRIPT_PATH" "$SCRIPT_S3_PATH"

# Spark 작업 제출 함수
submit_spark_job() {
    local script_path="$1"
    local script_args="$2"
    
    # spark-submit 설정
    SPARK_SUBMIT_ARGS=(
        --master yarn
        --deploy-mode cluster
        --name "$(basename $PYTHON_SCRIPT .py)-$(date +%H%M%S)"
        --num-executors 4
        --executor-cores 2
        --executor-memory 4g
        --driver-memory 2g
        --conf "spark.sql.adaptive.enabled=true"
        --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
        --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
        --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider"
        --py-files "$script_path"
        "$script_path"
    )
    
    # 스크립트별 특화 설정
    case "$PYTHON_SCRIPT" in
        "performance-test.py")
            SPARK_SUBMIT_ARGS+=(
                --conf "spark.dynamicAllocation.enabled=true"
                --conf "spark.dynamicAllocation.maxExecutors=8"
            )
            ;;
        "optimized-processing.py")
            SPARK_SUBMIT_ARGS+=(
                --conf "spark.sql.adaptive.skewJoin.enabled=true"
                --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"
            )
            ;;
    esac
    
    # 인자 추가
    IFS=' ' read -ra ARG_ARRAY <<< "$script_args"
    SPARK_SUBMIT_ARGS+=("${ARG_ARRAY[@]}")
    
    echo -e "${GREEN}spark-submit 명령어:${NC}"
    echo "spark-submit \\"
    for arg in "${SPARK_SUBMIT_ARGS[@]}"; do
        echo "  $arg \\"
    done | sed '$ s/ \\$//'
    echo
    
    # 실행
    echo -e "${YELLOW}Spark 작업 실행 중...${NC}"
    spark-submit "${SPARK_SUBMIT_ARGS[@]}"
}

# EMR 단계로 작업 제출 (EMR 클러스터가 있는 경우)
submit_emr_step() {
    local script_path="$1"
    local script_args="$2"
    
    # EMR 단계 설정
    STEP_ARGS=(
        "spark-submit"
        "--deploy-mode" "cluster"
        "--master" "yarn"
        "--conf" "spark.sql.adaptive.enabled=true"
        "--conf" "spark.sql.adaptive.coalescePartitions.enabled=true"
        "$script_path"
    )
    
    # 스크립트 인자 추가
    IFS=' ' read -ra ARG_ARRAY <<< "$script_args"
    STEP_ARGS+=("${ARG_ARRAY[@]}")
    
    # JSON 형식으로 단계 정의
    STEP_JSON=$(cat <<EOF
{
    "Name": "$(basename $PYTHON_SCRIPT .py)-$(date +%Y%m%d-%H%M%S)",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": $(printf '%s\n' "${STEP_ARGS[@]}" | jq -R . | jq -s .)
    }
}
EOF
)
    
    echo -e "${GREEN}EMR 단계 제출:${NC}"
    echo "$STEP_JSON" | jq .
    echo
    
    # EMR 단계 추가
    STEP_ID=$(aws emr add-steps \
        --cluster-id "$EMR_CLUSTER_ID" \
        --steps "$STEP_JSON" \
        --query 'StepIds[0]' \
        --output text)
    
    echo -e "${GREEN}EMR 단계 ID: $STEP_ID${NC}"
    
    # 단계 상태 모니터링
    echo "단계 실행 상태 확인 중..."
    while true; do
        STEP_STATE=$(aws emr describe-step \
            --cluster-id "$EMR_CLUSTER_ID" \
            --step-id "$STEP_ID" \
            --query 'Step.Status.State' \
            --output text)
        
        case "$STEP_STATE" in
            "COMPLETED")
                echo -e "${GREEN}단계 완료!${NC}"
                break
                ;;
            "FAILED"|"CANCELLED"|"INTERRUPTED")
                echo -e "${RED}단계 실패: $STEP_STATE${NC}"
                # 실패 상세 정보 출력
                aws emr describe-step \
                    --cluster-id "$EMR_CLUSTER_ID" \
                    --step-id "$STEP_ID" \
                    --query 'Step.Status.FailureDetails'
                exit 1
                ;;
            "PENDING"|"RUNNING")
                echo "상태: $STEP_STATE (대기 중...)"
                sleep 30
                ;;
            *)
                echo "알 수 없는 상태: $STEP_STATE"
                sleep 30
                ;;
        esac
    done
}

# 로그 확인 함수
check_logs() {
    if [ -n "$EMR_CLUSTER_ID" ]; then
        LOG_URI=$(terraform output -raw emr_log_uri 2>/dev/null || echo "")
        if [ -n "$LOG_URI" ]; then
            echo -e "${YELLOW}로그 확인:${NC}"
            echo "  EMR 로그: $LOG_URI"
            echo "  AWS Console: https://console.aws.amazon.com/elasticmapreduce/home#cluster-details:$EMR_CLUSTER_ID"
        fi
    fi
}

# 작업 실행
echo -e "${GREEN}=== Spark 작업 제출 ====${NC}"

if [ -n "$EMR_CLUSTER_ID" ]; then
    # EMR 클러스터가 있으면 EMR 단계로 제출
    submit_emr_step "$SCRIPT_S3_PATH" "$ARGS"
else
    # 로컬에서 spark-submit 실행
    submit_spark_job "$SCRIPT_S3_PATH" "$ARGS"
fi

# 로그 정보 출력
check_logs

echo -e "${GREEN}작업 제출 완료!${NC}"
echo
echo "결과 확인:"
echo "  S3 출력 경로 확인: aws s3 ls $DEFAULT_OUTPUT/"
echo "  처리 결과 다운로드: aws s3 sync $DEFAULT_OUTPUT/ ./results/"
