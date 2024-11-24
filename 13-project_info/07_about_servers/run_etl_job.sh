#! /bin/bash


#检查参数数量
timer_start=`date +"%Y-%m-%d %H:%M:%S"`
if [ "$#" -ne 3 ]; then
    echo "[INFO] Usage: $0 <table_name> <process_date> <ddl/dml/chk> "
    exit 1
fi 
#初始化环境信息
#source /opt/client1101/bigdata_env
sh /home/etluser/test_init.sh
#ETL_HOME
ETL_HOME="/home/etluser/etl_script"

FILE_NAME=$1
RAW_PROCESS_DATE=$2
RUN_TYPE=$3




if [ "$RUN_TYPE" = "ddl" ]; then 
    EXECUTE_DIR="$ETL_HOME/ddl"
elif [ "$RUN_TYPE" = "dml" ]; then 
    EXECUTE_DIR="$ETL_HOME/dml"
elif [ "$RUN_TYPE" = "chk" ]; then 
    EXECUTE_DIR="$ETL_HOME/chk"
fi
echo "[SUCCESS]     $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取文件参数成功:$RUN_TYPE" 

#检查日期参数是否合规
if [ ${#RAW_PROCESS_DATE} -eq 8 ]; then
    PROCESS_DATE="${RAW_PROCESS_DATE:0:4}-${RAW_PROCESS_DATE:4:2}-${RAW_PROCESS_DATE:6:2}"
elif [ ${#RAW_PROCESS_DATE} -eq 10 ]; then
    PROCESS_DATE="$RAW_PROCESS_DATE"
else 
    echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! 无效的参数，请检查." 
    exit 1
fi

echo "[SUCCESS]     $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取跑批日期成功:$PROCESS_DATE" 
LOGS_DIR="/home/etluser/logs/${PROCESS_DATE}/${RUN_TYPE}_${FILE_NAME}"

#GET DATATIMESTAMPS
TIMESTAMP=$(date +"%Y%m%d%H%M%S")

#创建logs文件
#LOGS_DIR="$ETL_HOME/logs/${PROCESS_DATE}/${FILE_NAME}"
LOG_FILE="${FILE_NAME}_${TIMESTAMP}.log"
LOG_PATH="$LOGS_DIR/$LOG_FILE"
#if not exists the directory then mkidr
if [ ! -d "$LOGS_DIR" ]; then
	mkdir -p "$LOGS_DIR"
	if [ $? -eq 0 ]; then
		echo "[SUCCESS] $(date +"%Y-%m-%d %H:%M:%S"): Success!日志目录创建成功 $LOG_PATH" | tee -a "$LOG_PATH"
	else
		echo "[FAILED]  $(date +"%Y-%m-%d %H:%M:%S"): Error!日志目录创建失败 $LOG_PATH" | tee -a "$LOG_PATH"
	fi 
fi





#拼接目录
echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 开始运行脚本文件!" | tee -a "$LOG_PATH"



if [ "$RUN_TYPE" = "ddl" ]; then 
    FULL_PATH="${EXECUTE_DIR}/createtable_${FILE_NAME}.hql"
elif [ "$RUN_TYPE" = "dml" ]; then 
    FULL_PATH="${EXECUTE_DIR}/etl_proc_${FILE_NAME}_pc.hql"
elif [ "$RUN_TYPE" = "chk" ]; then 
    FULL_PATH="${EXECUTE_DIR}/${FILE_NAME}_chk.hql"
fi
echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取任务名为:$FILE_NAME" | tee -a "$LOG_PATH"
echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取执行目录成功:$EXECUTE_DIR" | tee -a "$LOG_PATH"
echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取脚本文件名为:$FULL_PATH" | tee -a "$LOG_PATH"

#检查文件是否存在
if [ ! -f "$FULL_PATH" ]; then
    echo "[FAILED]  $(date +"%Y-%m-%d %H:%M:%S"): Error! 文件路径不存在: $FULL_PATH !" | tee -a "$LOG_PATH"
    exit 1
else
    echo "[SUCCESS] $(date +"%Y-%m-%d %H:%M:%S"): 初始化跑批脚本成功 " | tee -a "$LOG_PATH"
fi


echo "source /opt/client1101/bigdata_env"  | tee -a "$LOG_PATH"
#tmp检查

if [ "$RUN_TYPE" = "ddl" ]; then
    echo "[INFO]   $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行开始 " | tee -a "$LOG_PATH"
    echo "[INFO]   $(date +"%Y-%m-%d %H:%M:%S"): spark-sql --keytab /home/etluser/US_AGL_ALL.keytab --principal US_AGL_ALL -f $FULL_PATH"
     spark-beeline -f "$FULL_PATH" 2>&1 | tee -a "$LOG_PATH"

    if grep -qE "( ERROR | FAILURE | Error | FAILED | failure |Error | failed)" "$LOG_PATH"; then
        echo "[FAILED] $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行失败,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        exit 1
    else
        timer_end=`date +"%Y-%m-%d %H:%M:%S"`
        echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行成功,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `
    fi 
   
else
	echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 创建脚本临时文件 " | tee -a "$LOG_PATH"			
    TMP_DIR="/home/etluser/tmp"
    if [ ! -d "$TMP_DIR" ]; then
        mkdir -p "$TMP_DIR"
    fi

    TMP_FILE="$TMP_DIR/temp_$FILE_NAME.sql"
    cp "$FULL_PATH" "$TMP_FILE"
    sed -i "s/\${process_date}/${PROCESS_DATE}/gi" "$TMP_FILE"

    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本替换开始 " | tee -a "$LOG_PATH"
    cat "$TMP_FILE" | tee -a "$LOG_PATH"
	echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行开始 " | tee -a "$LOG_PATH"
	echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): spark-sql --keytab /home/etluser/CL.keytab --principal CL -f $TMP_FILE"
    #spark-sql --keytab /home/etluser/CL.keytab --principal CL -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"
    #spark-sql --keytab /home/etluser/US_AGL_ALL.keytab --principal US_AGL_ALL -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"  
    spark-beeline -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"  
	
    if grep -qE "( ERROR | FAILURE | Error | FAILED | failure|Error | failed)" "$LOG_PATH"; then
        echo "[FAILED] $(date +"%Y-%m-%d %H:%M:%S"): 跑批脚本执行失败,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        exit 1
    else
        timer_end=`date +"%Y-%m-%d %H:%M:%S"`
        echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 跑批脚本执行成功,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `
    fi 
    
    #echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行结束,日志目录: $LOG_PATH" | tee -a "$LOG_PATH"
fi
#timer_end=`date +"%Y-%m-%d %H:%M:%S"`
#duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 s 60  24 h 999 d",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `

echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行总耗时: $duration" | tee -a "$LOG_PATH"
#rm "TMP_FILE"
