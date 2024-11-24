#! bin/bash

source /opt/client1107/bigdata_env
PASSWD=Cl,202308zr!
echo "[INFO]    $(date +"%Y-%m%-d %H:%M:%S"): 进行环境认证"
if [ $? -eq 0 ]; then
	echo "[SUCCESS]  $(date +"%Y-%m%-d %H:%M:%S"): 环境初始化成功"
else 
	echo "[FAILED]   $(date +"%Y-%m%-d %H:%M:%S"): 环境初始化失败"
	exit 1
fi
echo $PASSWD | kinit CL 
if [ $? -eq 0 ]; then
        echo "[INFO]   $(date +"%Y-%m%-d %H:%M:%S"): 认证成功"
else
        echo "[INFO]   $(date +"%Y-%m%-d %H:%M:%S"): 认证失败"
        exit 1
fi
klist



