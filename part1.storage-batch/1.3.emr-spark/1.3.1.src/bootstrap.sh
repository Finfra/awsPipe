#!/bin/bash

# EMR Bootstrap Script
# 클러스터 시작시 자동 실행되는 초기화 스크립트

set -e

echo "Starting EMR Bootstrap Script..."

# Python 패키지 업데이트
sudo yum update -y
sudo yum install -y python3-pip

# 필수 Python 라이브러리 설치
sudo pip3 install --upgrade pip
sudo pip3 install boto3 pandas numpy matplotlib seaborn

# Jupyter Notebook 설치 및 설정
sudo pip3 install jupyter jupyterlab

# Jupyter 설정
sudo mkdir -p /home/hadoop/.jupyter
sudo chown hadoop:hadoop /home/hadoop/.jupyter

cat > /tmp/jupyter_notebook_config.py << 'EOF'
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.token = ''
c.NotebookApp.password = ''
EOF

sudo mv /tmp/jupyter_notebook_config.py /home/hadoop/.jupyter/
sudo chown hadoop:hadoop /home/hadoop/.jupyter/jupyter_notebook_config.py

# 환경 변수 설정
echo 'export PYSPARK_DRIVER_PYTHON=jupyter' >> /home/hadoop/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8888"' >> /home/hadoop/.bashrc

# 로그 디렉토리 생성
sudo mkdir -p /var/log/custom
sudo chown hadoop:hadoop /var/log/custom

# CloudWatch 에이전트 설치 (옵션)
if [ "$INSTALL_CLOUDWATCH" = "true" ]; then
    wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
    sudo rpm -U ./amazon-cloudwatch-agent.rpm
fi

echo "Bootstrap Script completed successfully!"
