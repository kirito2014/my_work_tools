#!/bin/bash

# 参数校验
if [ $# -ne 2 ]; then
    echo "错误：需要两个参数，sector_name 和 source_tables.txt路径"
    exit 1
fi

sector_name="$1"
source_tables="$2"

# 检查source_tables文件是否存在
if [ ! -f "$source_tables" ]; then
    echo "错误：source_tables文件不存在或不可读"
    exit 1
fi

# 校验sector_name是否包含下划线
if [[ "$sector_name" == *"_"* ]]; then
    echo "错误：sector_name不能包含下划线，请去除下划线"
    exit 1
fi

# 转为小写
sector_name_lower=$(echo "$sector_name" | tr '[:upper:]' '[:lower:]')
oss_base="oss://dl-bucket/dl_agl_check_result/$sector_name_lower/input"
oss_path="$oss_base/source_tables.txt"

# 检查OSS文件是否存在
if ossutil64 ls "$oss_path" &>/dev/null; then
    # 获取原文件行数
    count_before=$(ossutil64 cat "$oss_path" | wc -l)
    
    # 生成备份文件名
    timestamp=$(date +"%Y%m%d%H%M%S")
    bak_file="source_tables_bak${timestamp}.txt"
    oss_bak_path="$oss_base/$bak_file"
    
    # 备份原文件
    if ! ossutil64 mv "$oss_path" "$oss_bak_path"; then
        echo "错误：文件备份失败"
        exit 1
    fi
    
    # 上传新文件
    if ! ossutil64 cp "$source_tables" "$oss_path"; then
        echo "错误：文件上传失败"
        exit 1
    fi
    
    # 获取新文件行数
    count_after=$(ossutil64 cat "$oss_path" | wc -l)
    
    # 输出结果
    echo "上传成功"
    echo "备份文件: $bak_file"
    echo "原记录数: $count_before"
    echo "新记录数: $count_after"
else
    # 直接上传文件
    if ! ossutil64 cp "$source_tables" "$oss_path"; then
        echo "错误：文件上传失败"
        exit 1
    fi
    count_after=$(ossutil64 cat "$oss_path" | wc -l)
    echo "首次上传成功"
    echo "新记录数: $count_after"
fi

exit 0