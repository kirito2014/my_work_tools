#!bin/bash
#通过spark-beeline获取传入表名的表结构，输出字段列表
#根据传入的日期参数拼接字段列表 sql语句插入数据
#字段存储到临时表并在执行结束后删除

#spark-beeline认证 一般不需要，只要当前有人操作过服务器跑批就能认证

#检查传入参数是否正确
ETL_HOME="/data/bdsp"
#检查是否存在etl_list文件参数
if [ $# -ne 3 ]; then
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): Usage: $0 <table_name_list> <start_date> <end_date>"
    exit 1 
fi
etl_list_file="$ETL_HOME/$1"
start_date=$2
end_date=$3



LOG_FILE="${etl_list_file}.log"
LOG_FILE_DETAIL="${etl_list_file}_detail.log"

if test -f "$LOG_FILE"; then
    if test -f "$LOG_FILE_DETAIL"; then
        rm "$LOG_FILE" "$LOG_FILE_DETAIL"
    fi
fi


#检查文件是否存在
if [ ! -f "$etl_list_file" ]; then 
    echo "[ERROR]     $(date +"%Y-%m-%d %H:%M:%S"): Error: 文件 $etl_list_file 不存在." | tee -a "$LOG_FILE"
    exit 1 
fi
total_lines=$(wc -l < "$etl_list_file")
counter=0
error_jobs=()






#检查日期参数是否合规
if [ ${#start_date} -eq 8 ]; then
    P_START_DATE="${start_date:0:4}-${start_date:4:2}-${start_date:6:2}"
elif [ ${#start_date} -eq 10 ]; then
    P_START_DATE="$start_date"
else
    echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! 无效的原始日期参数，请检查."
    exit 1
fi

#检查日期参数是否合规
if [ ${#end_date} -eq 8 ]; then
    P_END_DATE="${end_date:0:4}-${end_date:4:2}-${end_date:6:2}"
elif [ ${#end_date} -eq 10 ]; then
    P_END_DATE="$end_date"
else
    echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! 无效的原始日期参数，请检查."
    exit 1
fi


#读取文件内容
while IFS= read -r table_name || [[ -n "$table_name" ]]; do
    ((counter++))
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 当前执行任务名称 :$table_name ." | tee -a "$LOG_FILE"
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 当前执行任务序号 :$counter 总任务数量 :$total_lines ." | tee -a "$LOG_FILE"
    #执行脚本
    timer_start=`date +"%Y-%m-%d %H:%M:%S"`
    #临时文件路径
    temp_file=$(mktemp)

    #执行spark-beeline 查询结果保存到临时文件

    #spark-beeline -e "desc agl.$table_name;" > "$temp_file"
    IFS=$'\n'
    first_flag="1"
    all_column=""
    for context1 in `spark-beeline -e "desc table agl.${table_name}"`;
    do
    colname=$(echo ${context1}|awk -F '|' '{print $2}')
    coltype=$(echo ${context1}|awk -F '|' '{print $3}')
    if [ "${colname}" == "" -o "${coltype}" == "" -o "${colname:0:2}" == "  " -o "${colname:0:2}" == " #" ];then
        continue
    fi
    colname=`echo "${colname}"|awk '$1=$1'`
    coltype=`echo "${coltype}"|awk '$1=$1'`

    if [ "${colname}" == "PT_DT" ];then
        continue
    fi

    if [ "${first_flag}" == "1" ] ;then
        dh=""
    else
        dh=","
    fi
    all_column="${all_column}${dh}${colname}"
    first_flag="0"
    done

    #处理临时文件
    # column_fileds=$(cat "$temp_file" |
    #     #删除前6行表头
    #     tail -n +6 |
    #     #删除所有空行
    #     sed '/^\s*$/d' |
    #     #sed 's/#.*$//' |
    #     #删除所有注释
    #     sed '/^#/d' |
    #     #删除所有空格
    #     sed 's/ //g' |
    #     sed '/ # Partition Information/q' |
    #     sed '/#PartitionInformation/q' |
    #     awk '/PT_DT/ {exit} {print}' |
    #     grep -v 'PT_DT' |
    #     awk -F '|' '{print $2}' |
    #     sed '/#PartitionInformation/q' |
    #     sed '/# Partition Information/q' |
    #     paste -sd ',' -)

    #echo $all_column
    #echo "111111"
    echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取表${table_name}字段成功:$all_column."
    #刪除存量數據
    drop_sql="alter table agl.${table_name} DROP IF EXISTS  partition(pt_dt='${P_END_DATE}');"
    add_sql="alter table agl.${table_name} ADD  partition(pt_dt='${P_END_DATE}');"
    echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}刪除數據语句成功:\n $drop_sql."   
    spark-beeline -e "$drop_sql"
    spark-beeline -e "$add_sql"
    #拼接插入语句
    insert_sql="insert into agl.${table_name} partition(pt_dt='${P_END_DATE}') select ${all_column} from agl.${table_name} where pt_dt = '${P_START_DATE}';"
    echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}插入语句成功:\n $insert_sql."   
    spark-beeline -e "$insert_sql"
    #拼接 数据查询语句
    query_sql="select count(1) from agl.${table_name} where pt_dt = '${P_END_DATE}';"
    echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}查询语句成功:\n $query_sql."  
    spark-beeline -e "$query_sql" | tee -a "$LOG_FILE"
    #rm "$temp_file"

    if [ $? -ne 0 ]; then
        timer_end=`date +"%Y-%m-%d %H:%M:%S"`
        duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `

        echo "[ERROR]     $(date +"%Y-%m-%d %H:%M:%S"): Error: 执行脚本出错,脚本任务名称 $table_name 用时$duration ."| tee -a "$LOG_FILE"
        
        error_jobs+=("$table_name")
        continue
    fi
    timer_end=`date +"%Y-%m-%d %H:%M:%S"`
    duration=`echo $(($(date +%s -d "${timer_end}") - $(date +%s -d "${timer_start}"))) | awk '{t=split("60 seconds 60 mins 24 hours 999 days",a);for(n=1;n<t;n+=2){if($1==0)break;s=$1%a[n]a[n+1]s;$1=int($1/a[n])}print s}' `
    echo "[SUCCESS]   $(date +"%Y-%m-%d %H:%M:%S"): Success: 执行脚本成功,脚本任务名称 $table_name 用时$duration ."| tee -a "$LOG_FILE"
    echo "[ END ] $(date +"%Y-%m-%d %H:%M:%S"): Success! ${table_name}数据回插成功，请检查数据内容是否无误."
done < "$etl_list_file"
    
#打印执行失败的任务名称
if [ ${#error_jobs[@]} -gt 0 ]; then
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 执行失败任务: ${#error_jobs[@]} 个"| tee -a "$LOG_FILE"
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 执行失败任务如下:"| tee -a "$LOG_FILE"
    for job_name in "${error_jobs[@]}"; do
        echo "$job_name" | tee -a "$LOG_FILE"
    done

fi