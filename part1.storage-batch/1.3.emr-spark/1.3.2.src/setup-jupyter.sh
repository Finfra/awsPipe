#!/bin/bash

# Jupyter Notebook 환경 설정 스크립트
# Usage: ./setup-jupyter.sh [cluster-id]

set -e

CLUSTER_ID=${1}

if [ -z "$CLUSTER_ID" ]; then
    echo "Usage: $0 <cluster-id>"
    echo "Available clusters:"
    aws emr list-clusters --active --query 'Clusters[*].[Id,Name,Status.State]' --output table
    exit 1
fi

echo "Setting up Jupyter environment for EMR cluster: $CLUSTER_ID"

# 클러스터 마스터 노드 DNS 가져오기
MASTER_DNS=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query 'Instances[0].PrivateDnsName' --output text)

if [ -z "$MASTER_DNS" ] || [ "$MASTER_DNS" = "None" ]; then
    echo "❌ Error: Cannot get master node DNS. Check cluster status."
    exit 1
fi

echo "Master node: $MASTER_DNS"

# SSH 키 확인
KEY_NAME=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.Ec2InstanceAttributes.Ec2KeyName' --output text)
KEY_PATH="$HOME/.ssh/${KEY_NAME}.pem"

if [ ! -f "$KEY_PATH" ]; then
    echo "❌ Error: SSH key not found at $KEY_PATH"
    echo "Please ensure your SSH key is available."
    exit 1
fi

echo "Using SSH key: $KEY_PATH"

# Python 패키지 설치 스크립트 업로드
echo "📦 Installing Python packages..."

cat > /tmp/install_packages.sh << 'EOF'
#!/bin/bash
sudo pip3 install --upgrade pip
sudo pip3 install jupyter jupyterlab
sudo pip3 install matplotlib seaborn plotly
sudo pip3 install ipywidgets
sudo pip3 install pandas numpy scipy scikit-learn

# Jupyter 확장 설치
sudo pip3 install jupyter_contrib_nbextensions
sudo jupyter contrib nbextension install --system

# 위젯 확장 활성화
sudo jupyter nbextension enable --py widgetsnbextension --system

echo "✅ Package installation completed"
EOF

# 스크립트 업로드 및 실행
scp -i "$KEY_PATH" -o StrictHostKeyChecking=no /tmp/install_packages.sh hadoop@$MASTER_DNS:/tmp/
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS 'chmod +x /tmp/install_packages.sh && /tmp/install_packages.sh'

# Jupyter 설정 파일 업로드
echo "⚙️  Configuring Jupyter..."

cat > /tmp/jupyter_notebook_config.py << 'EOF'
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.token = ''
c.NotebookApp.password = ''
c.NotebookApp.allow_root = True
c.NotebookApp.notebook_dir = '/home/hadoop/notebooks'

# 보안 설정 (프로덕션에서는 더 강화 필요)
c.NotebookApp.allow_origin = '*'
c.NotebookApp.disable_check_xsrf = True
EOF

# 설정 파일 업로드
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS 'mkdir -p ~/.jupyter ~/notebooks'
scp -i "$KEY_PATH" -o StrictHostKeyChecking=no /tmp/jupyter_notebook_config.py hadoop@$MASTER_DNS:~/.jupyter/

# 샘플 노트북 파일들 업로드
echo "📓 Uploading sample notebooks..."

# 현재 디렉토리의 Python 파일들을 노트북으로 변환하여 업로드
for py_file in $(dirname "$0")/*.py; do
    if [ -f "$py_file" ]; then
        filename=$(basename "$py_file")
        echo "Uploading $filename..."
        scp -i "$KEY_PATH" -o StrictHostKeyChecking=no "$py_file" hadoop@$MASTER_DNS:~/notebooks/
    fi
done

# Jupyter 시작 스크립트 생성
cat > /tmp/start_jupyter.sh << 'EOF'
#!/bin/bash

# 환경 변수 설정
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --config=/home/hadoop/.jupyter/jupyter_notebook_config.py"

# PySpark와 함께 Jupyter 시작
cd /home/hadoop/notebooks

nohup pyspark \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    > jupyter.log 2>&1 &

echo "Jupyter notebook started. Check jupyter.log for details."
echo "Access URL: http://$(hostname -I | awk '{print $1}'):8888"
EOF

# 시작 스크립트 업로드
scp -i "$KEY_PATH" -o StrictHostKeyChecking=no /tmp/start_jupyter.sh hadoop@$MASTER_DNS:~/
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS 'chmod +x ~/start_jupyter.sh'

# Jupyter 시작
echo "🚀 Starting Jupyter notebook..."
ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no hadoop@$MASTER_DNS '~/start_jupyter.sh'

# 잠시 대기 후 상태 확인
sleep 10

echo "✨ Jupyter setup completed!"
echo ""
echo "📝 Access Information:"
echo "   Master Node: $MASTER_DNS"
echo "   Jupyter URL: http://$MASTER_DNS:8888"
echo "   SSH Command: ssh -i $KEY_PATH hadoop@$MASTER_DNS"
echo ""
echo "📋 Available Notebooks:"
echo "   - notebook-setup.py: 환경 설정 및 기본 함수"
echo "   - visualization.py: 시각화 및 대시보드"
echo "   - advanced-analytics.py: 고급 분석 함수"
echo ""
echo "🔗 Next Steps:"
echo "   1. 브라우저에서 http://$MASTER_DNS:8888 접속"
echo "   2. 포트 포워딩: ssh -i $KEY_PATH -L 8888:localhost:8888 hadoop@$MASTER_DNS"
echo "   3. 로컬에서 http://localhost:8888 접속"

# 정리
rm -f /tmp/install_packages.sh /tmp/jupyter_notebook_config.py /tmp/start_jupyter.sh

echo "🎉 Setup completed successfully!"
