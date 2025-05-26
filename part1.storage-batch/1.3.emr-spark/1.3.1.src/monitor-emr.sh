#!/bin/bash

# EMR 클러스터 모니터링 스크립트
# Usage: ./monitor-emr.sh [cluster-id]

set -e

CLUSTER_ID=${1}

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: $0 <cluster-id>"
    echo "Available clusters:"
    aws emr list-clusters --active --query 'Clusters[*].[Id,Name,Status.State]' --output table
    exit 1
fi

echo "Monitoring EMR Cluster: $CLUSTER_ID"
echo "================================="

# 클러스터 상태 확인
echo "1. Cluster Status:"
aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.[Id,Name,Status.State,Status.StateChangeReason.Message]' --output table

# 인스턴스 그룹 상태
echo -e "\n2. Instance Groups:"
aws emr list-instance-groups --cluster-id $CLUSTER_ID --query 'InstanceGroups[*].[Name,InstanceGroupType,InstanceType,RunningInstanceCount,Market]' --output table

# 단계(Steps) 상태
echo -e "\n3. Steps Status:"
aws emr list-steps --cluster-id $CLUSTER_ID --query 'Steps[*].[Name,Status.State,Status.StateChangeReason.Message]' --output table

# 활성 애플리케이션
echo -e "\n4. Active Applications:"
aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PublicDnsName' --output text > /tmp/master_dns

if [ -s /tmp/master_dns ]; then
    MASTER_DNS=$(cat /tmp/master_dns)
    echo "Master DNS: $MASTER_DNS"
    echo "Spark UI: http://$MASTER_DNS:4040"
    echo "Yarn UI: http://$MASTER_DNS:8088"
    echo "Zeppelin: http://$MASTER_DNS:8890"
fi

# 리소스 사용률 (CloudWatch 메트릭)
echo -e "\n5. Resource Utilization (Last 1 hour):"
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%S)
START_TIME=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S)

# CPU 사용률
CPU_UTILIZATION=$(aws cloudwatch get-metric-statistics \
    --namespace AWS/ElasticMapReduce \
    --metric-name CPUUtilization \
    --dimensions Name=JobFlowId,Value=$CLUSTER_ID \
    --start-time $START_TIME \
    --end-time $END_TIME \
    --period 3600 \
    --statistics Average \
    --query 'Datapoints[0].Average' \
    --output text 2>/dev/null || echo "N/A")

echo "Average CPU Utilization: $CPU_UTILIZATION%"

# 메모리 사용률
MEMORY_UTILIZATION=$(aws cloudwatch get-metric-statistics \
    --namespace AWS/ElasticMapReduce \
    --metric-name YARNMemoryAvailablePercentage \
    --dimensions Name=JobFlowId,Value=$CLUSTER_ID \
    --start-time $START_TIME \
    --end-time $END_TIME \
    --period 3600 \
    --statistics Average \
    --query 'Datapoints[0].Average' \
    --output text 2>/dev/null || echo "N/A")

echo "YARN Memory Available: $MEMORY_UTILIZATION%"

# 비용 추정
echo -e "\n6. Cost Estimation:"
RUNNING_INSTANCES=$(aws emr list-instance-groups --cluster-id $CLUSTER_ID --query 'InstanceGroups[?State==`RUNNING`].RunningInstanceCount' --output text | tr '\n' '+' | sed 's/+$//')
TOTAL_INSTANCES=$(echo "$RUNNING_INSTANCES" | bc 2>/dev/null || echo "N/A")
echo "Total Running Instances: $TOTAL_INSTANCES"

if [ "$TOTAL_INSTANCES" != "N/A" ]; then
    HOURLY_COST=$(echo "$TOTAL_INSTANCES * 0.5" | bc)
    echo "Estimated Hourly Cost: \$${HOURLY_COST}"
fi

echo -e "\nMonitoring completed at $(date)"
