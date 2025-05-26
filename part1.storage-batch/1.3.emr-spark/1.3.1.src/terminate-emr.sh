#!/bin/bash

# EMR 클러스터 종료 스크립트
# Usage: ./terminate-emr.sh [cluster-id] [--force]

set -e

CLUSTER_ID=${1}
FORCE=${2}

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: $0 <cluster-id> [--force]"
    echo "Available clusters:"
    aws emr list-clusters --active --query 'Clusters[*].[Id,Name,Status.State]' --output table
    exit 1
fi

echo "Terminating EMR Cluster: $CLUSTER_ID"

# 클러스터 정보 확인
CLUSTER_INFO=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.[Name,Status.State]' --output text)
CLUSTER_NAME=$(echo $CLUSTER_INFO | cut -d' ' -f1)
CLUSTER_STATE=$(echo $CLUSTER_INFO | cut -d' ' -f2)

echo "Cluster Name: $CLUSTER_NAME"
echo "Current State: $CLUSTER_STATE"

# 종료 확인
if [ "$FORCE" != "--force" ]; then
    read -p "Are you sure you want to terminate this cluster? (y/N): " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "Termination cancelled."
        exit 0
    fi
fi

# 실행 중인 Steps 확인
RUNNING_STEPS=$(aws emr list-steps --cluster-id $CLUSTER_ID --step-states RUNNING --query 'Steps[*].Name' --output text)

if [ -n "$RUNNING_STEPS" ]; then
    echo "Warning: The following steps are currently running:"
    echo "$RUNNING_STEPS"
    
    if [ "$FORCE" != "--force" ]; then
        read -p "Do you want to terminate anyway? (y/N): " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            echo "Termination cancelled."
            exit 0
        fi
    fi
fi

# 클러스터 종료
echo "Terminating cluster..."
aws emr terminate-clusters --cluster-ids $CLUSTER_ID

echo "Termination request sent. Cluster will be terminated shortly."
echo "You can monitor the status with: aws emr describe-cluster --cluster-id $CLUSTER_ID"
