#!bin/bash
#通过spark-beeline获取传入表名的表结构，输出字段列表
#根据传入的日期参数拼接字段列表 sql语句插入数据
#字段存储到临时表并在执行结束后删除

#spark-beeline认证 一般不需要，只要当前有人操作过服务器跑批就能认证

#检查传入参数是否正确

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <table_name> <start_date> <end_date>"
    exit
fi

#获取传入的表名和日期
table_name=$1
start_date=$2
end_date=$3

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



#临时文件路径
temp_file=$(mktemp)

#执行spark-beeline 查询结果保存到临时文件

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


#echo "111111"
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 获取表${table_name}字段成功:$all_column."
#刪除存量數據
drop_sql = "alter table agl.${table_name} DROP IF EXISTS  partition(pt_dt='${P_END_DATE}');"
echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}刪除數據语句成功:\n $drop_sql."   
spark-beeline -e "$drop_sql"
#拼接插入语句
insert_sql="insert into agl.${table_name} partition(pt_dt='${P_END_DATE}') select ${all_column} from agl.${table_name} where pt_dt = '${P_START_DATE}';"
echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}插入语句成功:\n $insert_sql."   
spark-beeline -e "$insert_sql"
#拼接数据查询语句
query_sql="select count(1) from agl.${table_name} where pt_dt = '${P_END_DATE}';"
echo -e "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 拼接表${table_name}查询语句成功:\n $query_sql."  
spark-beeline -e "$query_sql"
rm "$temp_file"

echo "[ END ] $(date +"%Y-%m-%d %H:%M:%S"): Success! 数据回插成功，请检查数据内容是否无误."