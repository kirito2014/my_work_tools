#!/bin/bash
# 克隆仓库 存在可不克隆
url=$1 #git
ABSOLUTEPATH=$2 #部署
rows=$3 #分支
#创建克隆代码的目录
echo "make directory ${ABSOLUTEPATH}"
if [ ! -d ${ABSOLUTEPATH} ]
then
    echo "创建拉取目录"
    mkdir -p ${ABSOLUTEPATH}
    #开始克隆
     git clone --branch master http://zhengmengyuan:F8qQp6Vv5zJ0RGaR79wt@"${url}" "${ABSOLUTEPATH}"
     cd ${ABSOLUTEPATH}
else
 #目录已存在 进入目录 判断远程分支是否已存在
     cd ${ABSOLUTEPATH}

fi
#cd 进入项目目录 根据配置文件 切换分支 拷贝项目分支到部署包中
 git config --local user.name 郑梦圆

 git config --local user.email zhengmengyuan@antcode.com
#读取所有需要处理分支的应用清单
#while read rows
#循环应用

#do
	git checkout $rows
	git branch $rows
        echo "判断分支是否存在"
        thisBranch=$(git rev-parse --abbrev-ref HEAD)
	 if [ "$thisBranch" = "$rows" ]; then
		#存在分支 拉取分支
		git pull origin $rows
		#分支携带 版本参数  截取版本参数 并拷
		cp -r  *  ../agl-pakage/
		 #不存在分支 跳过处理
		echo "分支存在 更新并拷贝项目目录至pakage目录"
		
	 fi
	#存在则切换并拉取目标分支
	#拷贝目标项目目录到部署制品目录下


#done < /data/git/develop/code/zhengmengyuan/deploy_branch.txt



# ./prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-DO-DL.git BDP-DO-DL branch

#压缩部署包
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-COMM.git BDP-AGL-COMM aglcomm
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-CORP.git BDP-AGL-CORP agls01
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-RTLB.git BDP-AGL-RTLB agls02
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-LOAN.git BDP-AGL-LOAN agls03
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-ASSM.git BDP-AGL-ASSM agls04
#./agl_prod_deploy_pakage.sh code.kf.zjnx.net/zjrcudevtest/big_data_room/BDP-AGL-FINM.git BDP-AGL-FINM agls05

#zip -r agl-pakage-0529.zip agl-pakage/