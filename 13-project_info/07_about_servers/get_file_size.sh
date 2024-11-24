#!/bin/bash
ETL_HOME="/home/dsadm"

usage(){
    echo "Usage: $0 <table_name> [date]"
    echo "Get the size of file in HDFS"
    echo "If 'date' is provided,it will search for file size for the specific date partition"
    exit 1
}
#检查是否存在etl_list文件参数
if [ $# -lt 1 ] || [ "$#" -gt 2 ]; then
    usage
fi

etl_list_file="$ETL_HOME/$1"
etl_date=$2


LOG_FILE="${etl_list_file}.log"

if test -f "$LOG_FILE"; then
        rm $LOG_FILE  
fi

#LOG_PATH="$LOGS_DIR/$LOG_FILE"
#if not exists the directory then mkidr

#检查文件是否存在
if [ ! -f "$etl_list_file" ]; then 
    echo "[ERROR]     $(date +"%Y-%m-%d %H:%M:%S"): Error: 文件 $etl_list_file 不存在." 
    exit 1 
fi

total_lines=$(wc -l < "$etl_list_file")
counter=0
error_jobs=()
#读取文件内容
echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 总任务数量 :$total_lines ." | tee -a "$LOG_FILE"
while IFS= read -r table_name || [[ -n "$table_name" ]]; do
    ((counter++))
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 查询表名称 :$table_name ." 
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S"): 当前查询表名序号 :$counter 总查询数量 :$total_lines ." 
    #echo ${table_name:0:7}
    #对应的schema
    if [[ "${table_name:0:7}" == "agl_s01" ]]; then 
        schema_name="agl_s01"
    elif [[ "${table_name:0:7}" == "agl_s02" ]]; then 
        schema_name="agl_s02"
    elif [[ "${table_name:0:7}" == "agl_s03" ]]; then 
        schema_name="agl_s03"
    elif [[ "${table_name:0:7}" == "agl_s04" ]]; then 
        schema_name="agl_s04"
    elif [[ "${table_name:0:7}" == "agl_s05" ]]; then 
        schema_name="agl_s05"
    elif [[ "${table_name:0:7}" == "agl_s06" ]]; then 
        schema_name="agl_comm" 
    elif [[ "${table_name:0:7}" == "agl_s07" ]]; then 
        schema_name="agl_comm"  
    elif [[ "${table_name:0:4}" == "ods_" ]]; then
        schema_name=$(echo "$table_name" | awk -F _ '{print $1"_"$2}')
    else 
        schema_name="agl_comm"  
    fi
     #执行查询语句
    #--统计表所有分区相加下的大小（G）
    #hdfs dfs -du -h hdfs://hacluster/user/hive/warehouse/agl_loan.db/agl_s03_corp_fin_simple_stat_prft_tf | awk -F ' ' '{print $3}' | awk ' { SUM += $1} END {print SUM/(1024)}'
    #--统计分区下表大小(M)
    #hdfs dfs -du -h hdfs://hacluster/user/hive/warehouse/agl_rtlb.db/agl_s02_indv_debit_card_info_tf/pt_dt=2023-09-30
   
    
    if [ -n "$etl_date" ]; then
        #检查日期参数是否合规
        if [ ${#etl_date} -eq 8 ]; then
            PROCESS_DATE="${etl_date:0:4}-${etl_date:4:2}-${etl_date:6:2}"
        elif [ ${#etl_date} -eq 10 ]; then
            PROCESS_DATE="$etl_date"
        else 
            echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! 无效的日期参数，请检查." 
            exit 1
        fi
        #hdfs_path="hdfs://hacluster/user/hive/warehouse/${schema_name}.db/${table_name}/pt_dt=${PROCESS_DATE}/"
        hdfs_path="hdfs://hacluster/user/hive/warehouse/${schema_name}.db/${table_name}"
        echo $hdfs_path
        #file_size=$(hdfs dfs -du -h "$hdfs_path" | awk -F ' ' '{print $3}' | awk ' { SUM += $1} END {print SUM/(1024)}' )
        file_size=$(hdfs dfs -du -h "$hdfs_path" | grep ${PROCESS_DATE} | awk -F ' ' '{print $1}')
        file_unit=$(hdfs dfs -du -h "$hdfs_path" | grep ${PROCESS_DATE} | awk -F ' ' '{print $2}')
        
        if [[ $file_unit = "0" ]]; then
            file_unit_1="K"
        else 
            file_unit_1=$file_unit
        fi
        
        
        if [[ -z "$file_size" ]]; then
            echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! $table_name 文件不存在." | tee -a "$LOG_FILE"
            continue
        else
            echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"):  $table_name  $file_size $file_unit_1."  | tee -a "$LOG_FILE"
        fi
    else
        hdfs_path="hdfs://hacluster/user/hive/warehouse/${schema_name}.db/${table_name}"
        echo $hdfs_path
        file_num=$(hdfs dfs -du -h "$hdfs_path" | wc -l )
        echo "[INFO]    $(date +"%Y-%m-%d %H:%M:%S"):  $table_name 有$file_num个文件，对文件大小取平均值"
        file_size=$(hdfs dfs -du -h "$hdfs_path" | awk -F ' ' '{print $1}' | awk ' { SUM += $1} END {print SUM/($file_num)}' )
        if [[ -z "$file_size" ]]; then
            echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S"): Error! $table_name 文件不存在."  | tee -a "$LOG_FILE"
            continue
        else
            echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S"):  $table_name  $file_size G."  | tee -a "$LOG_FILE"
        fi
    fi

done < "$etl_list_file"
echo "[INFO]      $(date +"%Y-%m-%d %H:%M:%S"): 结果文件保存在 $LOG_FILE"

