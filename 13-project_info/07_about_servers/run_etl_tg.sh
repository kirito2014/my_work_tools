#!/bin/bash
#替换跑批日期变量
#替换version_num版本变量
#新增init参数，可执行

#检查参数数量 
timer_start=`date +"%Y-%m-%d %H:%M:%S"`
if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
    echo "[INFO] Usage: $0 <table_name> <process_date> <ddl/dml/chk/init> [version_num]"
    exit 1
fi 
#初始化环境信息
#source client/bigdata_env
#source /data/hadoop_client/bigdata_env
#kinit -kt ${HOME_DIR}/userkey/US_AGL_ALL.keytab US_AGL_ALL
#sh ${HOME_DIR}/test_init.sh
HOME_DIR=$(dirname "$0")
ETL_HOME="${HOME_DIR}/etl_script"
echo "[ INFO  ] $(date +"%Y-%m-%d %H:%M:%S"): 获取脚本文件夹:${ETL_HOME}." 
#获取前3个参数
FILE_NAME=$1
RAW_PROCESS_DATE=$2
RUN_TYPE=$3

optional_version_num=${4:-""}
#检查传入的版本号是否为8位
#检查日期参数是否合规
if [ -z "$optional_version_num" ];then
    #optional_version_num=${4:-""}
    echo "[ INFO  ] $(date +"%Y-%m-%d %H:%M:%S"): 未传入版本号,版本号置为空值:${optional_version_num}." 
elif [ -n "$optional_version_num" ]; then
    if [ ${#optional_version_num} -ne 8 ]; then
        echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! 无效的版本号:${optional_version_num}参数(不为8位)，请检查." 
        exit 1
    elif  [ ${#optional_version_num} -eq 8 ]; then
        echo "[ INFO  ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 版本号获取成功:${optional_version_num}." 
    fi
fi


if [ "$RUN_TYPE" = "ddl" ]; then 
    EXECUTE_DIR="$ETL_HOME/ddl"
elif [ "$RUN_TYPE" = "dml" ]; then 
    EXECUTE_DIR="$ETL_HOME/dml"
elif [ "$RUN_TYPE" = "chk" ]; then 
    EXECUTE_DIR="$ETL_HOME/chk"
elif [ "$RUN_TYPE" = "init" ]; then 
    EXECUTE_DIR="$ETL_HOME/init"
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

PROCESS_TIMESTAMP=$(date -d "$PROCESS_DATE" +%s)
INTERVAL_FLOW_DT=$(date -d "@$((PROCESS_TIMESTAMP - 180 * 24 * 3600))" +%Y-%m-%d)
echo "interval_dt:$INTERVAL_FLOW_DT"


echo "[SUCCESS]     $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取跑批日期成功:$PROCESS_DATE" 
LOGS_DIR="${HOME_DIR}/logs/${PROCESS_DATE}/${RUN_TYPE}_${FILE_NAME}"

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
    FULL_PATH="${EXECUTE_DIR}/${FILE_NAME}_pc.hql"
elif [ "$RUN_TYPE" = "chk" ]; then 
    FULL_PATH="${EXECUTE_DIR}/${FILE_NAME}_chk.hql"
elif [ "$RUN_TYPE" = "init" ]; then 
    FULL_PATH="${EXECUTE_DIR}/init_${FILE_NAME}_pc.hql"
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
    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 创建ddl脚本临时文件 " | tee -a "$LOG_PATH"           
    TMP_DIR="${HOME_DIR}/tmp"
    if [ ! -d "$TMP_DIR" ]; then
        mkdir -p "$TMP_DIR"
    fi

    TMP_FILE="$TMP_DIR/temp_ddl_$FILE_NAME.sql"
    cp "$FULL_PATH" "$TMP_FILE"

    #替换脚本内容
    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本替换参数 " | tee -a "$LOG_PATH"
    sed -i "s/\${version_num}/${optional_version_num}/gi" "$TMP_FILE"

    echo "[INFO]   $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行开始 " | tee -a "$LOG_PATH"
    echo "[INFO]   $(date +"%Y-%m-%d %H:%M:%S"): spark-beeline -f $FULL_PATH"
     spark-beeline -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"

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
    TMP_DIR="${HOME_DIR}/tmp"
    if [ ! -d "$TMP_DIR" ]; then
        mkdir -p "$TMP_DIR"
    fi

    TMP_FILE="$TMP_DIR/temp_dml_$FILE_NAME.sql"
    cp "$FULL_PATH" "$TMP_FILE"

    #替换脚本内容
    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本替换开始 " | tee -a "$LOG_PATH"

    sed -i "s/\${process_date}/${PROCESS_DATE}/gi" "$TMP_FILE"
    sed -i "s/\${interval_flow_dt}/${INTERVAL_FLOW_DT}/gi" "$TMP_FILE"
    sed -i "s/\${version_num}/${optional_version_num}/gi" "$TMP_FILE"

    #cat "$TMP_FILE" | tee -a "$LOG_PATH"

    #执行脚本内容
    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行开始 " | tee -a "$LOG_PATH"
    echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): spark-beeline  -f $TMP_FILE"
    #spark-sql --keytab ${HOME_DIR}/CL.keytab --principal CL -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"
    #spark-sql --keytab ${HOME_DIR}/US_AGL_ALL.keytab --principal US_AGL_ALL -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"  
    spark-beeline -f "$TMP_FILE" 2>&1 | tee -a "$LOG_PATH"  
    
    #查询数据（主键）
    if grep -qE "( ERROR | FAILURE | Error | FAILED | failure|Error | failed)" "$LOG_PATH"; then
        echo "[FAILED] $(date +"%Y-%m-%d %H:%M:%S"): 跑批脚本执行失败,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        exit 1
    else
        timer_end=`date +"%Y-%m-%d %H:%M:%S"`
        echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 跑批脚本执行成功,日志目录: $LOG_PATH " | tee -a "$LOG_PATH"
        duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `
        if [[ "${FILE_NAME: -2}" == "tf" ]];then
            echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 查询当日数据 <全量表>" | tee -a "$LOG_PATH"
            data_query="select count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}' and del_f = '0';"
        elif [[ "${FILE_NAME: -2}" == "tg" ]];then
            echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 查询当日数据 <区间表>" | tee -a "$LOG_PATH"
            data_query="select count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}';"
        elif [[ "${FILE_NAME: -2}" == "ta" ]];then
            echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 查询当日数据 <增量表>" | tee -a "$LOG_PATH"
            data_query="select count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}';"
        else
            echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 查询当日数据 <未识别的表类型>" | tee -a "$LOG_PATH"
            data_query="select count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}';"
        fi
        echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 查询当日数据 -- $data_query" | tee -a "$LOG_PATH"
        result=$(spark-beeline -e "${data_query}" 2>/dev/null)
        process_result=$(echo "$result" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')
        echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 查询成功 ${PROCESS_DATE} 数据量： $process_result" | tee -a "$LOG_PATH"
        #querypk
        if [[ "${FILE_NAME: -2}" == "tf" ]];then
            pk_list=$(sed -n '11s/[^：]*：*\(.*\)/\1/p' "$TMP_FILE" | tr -d '\r' | sed 's/[[:space:]]*$//' )
        elif [[ "${FILE_NAME: -3}" == "_tg" ]];then
            pk_list=$(sed -n '13s/[^：]*：*\(.*\)/\1/p' "$TMP_FILE" | tr -d '\r' | sed 's/[[:space:]]*$//' )
        elif [[ "${FILE_NAME: -2}" == "ta" ]];then
            pk_list=$(sed -n '11s/[^：]*：*\(.*\)/\1/p' "$TMP_FILE" | tr -d '\r' | sed 's/[[:space:]]*$//' )
        else
            pk_list=""
        fi  

        if [ -n "$pk_list" ]; then
                
                pk_query=""
                echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 获取脚本中的主键：$pk_list" | tee -a "$LOG_PATH"
                if [[ "${FILE_NAME: -2}" == "tf" ]];then
                    pk_query="select count(1) from (select ${pk_list} ,count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}' and del_f ='0' group by ${pk_list} having count(1) >1 );"
                elif [[ "${FILE_NAME: -3}" == "_tg" ]];then
                    pk_query="select count(1) from (select ${pk_list} ,count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}' group by ${pk_list} having count(1) >1) ;"
                elif [[ "${FILE_NAME: -2}" == "ta" ]];then
                    pk_query="select count(1) from (select ${pk_list} ,count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}' group by ${pk_list} having count(1) >1 );"
                else
                    pk_query="select count(1) from (select ${pk_list} ,count(1) from agl.${FILE_NAME} where pt_dt = '${PROCESS_DATE}' group by ${pk_list} having count(1) >1 );"
                fi
                echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 查询主键重复

                $pk_query
                
                "  
                pk_result=$(spark-beeline -e "${pk_query}" 2>/dev/null)
                #echo $pk_result
                process_result=$(echo "$pk_result" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')
                if [ $process_result -gt 0 ]; then
                    echo "[WARNING]  $(date +"%Y-%m-%d %H:%M:%S"): 查询 ${PROCESS_DATE} 存在主键重复： 重复 $process_result 条" | tee -a "$LOG_PATH"
                else
                    echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 查询成功 ${PROCESS_DATE} 无主键重复： $process_result 条重复" | tee -a "$LOG_PATH"
                fi
        else
            echo "[ INFO  ]  $(date +"%Y-%m-%d %H:%M:%S"): 未能获取脚本中的主键,跳过主键检查"
        fi

    fi 
    
    #echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行结束,日志目录: $LOG_PATH" | tee -a "$LOG_PATH"
fi

echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"): 脚本执行总耗时: $duration" | tee -a "$LOG_PATH"
#rm "$TMP_FILE"
