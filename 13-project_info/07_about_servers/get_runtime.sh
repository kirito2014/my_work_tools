#!/bin/bash

#paramter check
if [ "$#" -ne 1 ]; then
    echo "usage: $0 <folder_path>"
    exit 1
fi

#save paramter
folder_path="$1"
output_file="runtime_$(date +%Y%m%d).txt" 

output_dir="/home/dsadm"
output_path="$output_dir/$output_file" > "$output_path"
for sub_folder in "$folder_path"/dml_agl_*/; do
    table_name=$(basename "$sub_folder" | sed 's/^dml_//')
    echo "[INFO]     $(date +"%Y-%m-%d %H:%M:%S") 获取到表名: $table_name"
    #lastest_log_file=$(find "$folder_path" -type f -name '*.log' | sort -n | tail -n 1)
    lastest_log_file=$(find "$sub_folder" -maxdepth 1 -type f -name '*.log' -exec ls -lt --time-style=long-iso {} + | awk '{print $6, $7, $8, $9}' | sort -nr | head -n 1 | awk '{print$NF}')


    if [ -z "$lastest_log_file" ]; then
        echo "[ERROR]   $(date +"%Y-%m-%d %H:%M:%S") 没有找到 $table_name 的日志信息"
        continue
    fi
    last_line=$(tail -n 1 "$lastest_log_file" | awk -F ': ' '{print$NF}')


    output_dir="/home/dsadm"
    #echo $output_dir

    output_path="$output_dir/$output_file"

    #echo "${sub_folder}: $last_line"
    echo "${table_name}  $last_line" >> "$output_path"
done
echo "[SUCCESS]  $(date +"%Y-%m-%d %H:%M:%S") 获取跑批时间完成! 文件保存路径为 : $output_path"
