#!/bin/bash
sudo pip install jupyter
sudo mkdir -p /home/hadoop/.jupyter
# 노트북 비밀번호 없이 실행 설정
echo "c.NotebookApp.token = ''" >> /home/hadoop/.jupyter/jupyter_notebook_config.py
# 백그라운드 실행용 alias
echo 'alias jupyter-start="jupyter notebook --no-browser --port=8888 --ip=0.0.0.0"' >> /home/hadoop/.bashrc
#source /home/hadoop/.bashrc
#jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
