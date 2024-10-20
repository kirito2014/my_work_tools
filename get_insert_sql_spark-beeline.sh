#!/bin/bash

# 检查是否传入正确数量的参数
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <table_name> <start_date> <end_date>"
    exit 1
fi

# 获取传入的表名和日期
table_name=$1
start_date=$2
end_date=$3

# 临时文件路径
temp_file=$(mktemp)

# 执行 spark-beeline 查询并将结果保存到临时文件
spark-beeline -e "show create table agl.${table_name};" > "$temp_file"

# 处理临时文件
fields=$(cat "$temp_file" |
    # 删除包含 CREATE TABLE 的行
    sed '/CREATE TABLE/d' |
    # 删除行的第一个空格后面的所有内容
    sed 's/ .*//g' |
    # 删除 PT_DT 字段
    grep -v 'PT_DT' |
    # 将字段以逗号分隔
    paste -sd ',' -)

# 输出结果
echo "表 ${table_name} 的字段（不包含 PT_DT 分区字段）：$fields"

# 删除临时文件
rm "$temp_file"
