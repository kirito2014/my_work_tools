#!/bin/bash
ETL_HOME="/home/dsadm"
#检查是否存在etl_list文件参数
if [ $# -ne 2 ]; then
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): Usage: $0 <etl_list_file> <etl_date>"
    exit 1 
fi
etl_list_file="$ETL_HOME/$1"
etl_date=$2




LOG_FILE="${etl_list_file}.log"
LOG_FILE_DETAIL="${etl_list_file}_detail.log"

if test -f "$LOG_FILE"; then
    if test -f "$LOG_FILE_DETAIL"; then
        rm "$LOG_FILE" "$LOG_FILE_DETAIL"
    fi
fi


#LOG_PATH="$LOGS_DIR/$LOG_FILE"
#if not exists the directory then mkidr


#检查文件是否存在
if [ ! -f "$etl_list_file" ]; then 
    echo "[ERROR]     $(date +"%Y-%m-%d %H:%M:%S"): Error: 文件 $etl_list_file 不存在." | tee -a "$LOG_FILE"
    exit 1 
fi
total_lines=$(wc -l < "$etl_list_file")
counter=0
error_jobs=()
#读取文件内容
while IFS= read -r etl_job_name || [[ -n "$etl_job_name" ]]; do
    ((counter++))
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 当前执行任务名称 :$etl_job_name ." | tee -a "$LOG_FILE"
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 当前执行任务序号 :$counter 总任务数量 :$total_lines ." | tee -a "$LOG_FILE"
    #执行脚本
    timer_start=`date +"%Y-%m-%d %H:%M:%S"`
    sh $ETL_HOME/run_etl_tg.sh $etl_job_name $etl_date chk 
    #sh $ETL_HOME/run_etl_4.sh $etl_job_name $etl_date chk
    
    if [ $? -ne 0 ]; then
        timer_end=`date +"%Y-%m-%d %H:%M:%S"`
        duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `

        echo "[ERROR]     $(date +"%Y-%m-%d %H:%M:%S"): Error: 执行脚本出错,脚本任务名称 $etl_job_name 用时$duration ."| tee -a "$LOG_FILE"
        
        error_jobs+=("$etl_job_name")
        continue
    fi 
    timer_end=`date +"%Y-%m-%d %H:%M:%S"`
    duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `
    echo "[SUCCESS]   $(date +"%Y-%m-%d %H:%M:%S"): Success: 执行脚本成功,脚本任务名称 $etl_job_name 用时$duration ."| tee -a "$LOG_FILE"
done < "$etl_list_file"

#打印执行失败的任务名称
if [ ${#error_jobs[@]} -gt 0 ]; then
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 执行失败任务: ${#error_jobs[@]} 个"| tee -a "$LOG_FILE"
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 执行失败任务如下:"| tee -a "$LOG_FILE"
    for job_name in "${error_jobs[@]}"; do
        echo "$job_name" | tee -a "$LOG_FILE"
    done

fi
