#!/bin/bash

#检查输入参数是否正确
if [ $# -ne 2 ]; then
    echo "脚本描述: 使用特定格式的外表加载csv文件,将数据插入到内表，并验证数据"
    echo "用例：$0 <表名> <本地文件路径>"
    echo "示例：$0 sh cd_insert.sh user_info /home/dsadm/pub_cd_map/user_info.csv"
    exit 1
fi

#获取参数
TABLE_NAME=$1
LOCAL_FILE=$2

CURRENT_DATE=$(date +%Y%m%d)
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 获取当前日期成功：${CURRENT_DATE}"
#转换表名为大写和小写

TABLE_NAME_UPPER=$(echo "$TABLE_NAME" | tr '[:lower:]' '[:upper:]')
TABLE_NAME_LOWER=$(echo "$TABLE_NAME" | tr '[:upper:]' '[:lower:]')

#HDFS路径
HDFS_PATH="hdfs:///tmp/external/${TABLE_NAME_LOWER}/"

#检查文件是否存在并删除存量文件
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 检查HDFS目录 ${HDFS_PATH} 是否存在..."
hdfs dfs -test  -d "$HDFS_PATH"
if [ $? -eq 0 ]; then
    echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 目录存在，删除文件下的文件"
    hdfs dfs -rm -r "$HDFS_PATH"
else
    echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 目录不存在,跳过删除."
fi

#上传新文件到HDFS 
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 上传文件 ${LOCAL_FILE} 到HDFS目录 ${HDFS_PATH}"
hdfs dfs -mkdir -p "$HDFS_PATH"
# if [ $? -eq 0 ]; then
#     echo "[ ERROR  ] $(date +"%Y-%m-%d %H:%M:%S"): HDFS目录文件创建失败!"
#     exit 1
# fi

hdfs dfs -put "$LOCAL_FILE" "$HDFS_PATH"

# if [ $? -eq 0 ]; then
#     echo "[ ERROR  ] $(date +"%Y-%m-%d %H:%M:%S"): 文件上传失败！"
#     exit 1
# fi

#获取字段信息并生成建表语句
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 获取字段信息并生成外表建表语句..."
COLUMN_DEFINTIONS=""

IFS=$'\n'
first_flag="1"
#all_column=""

command_output=$(spark-beeline -e "desc table AGL.${TABLE_NAME_UPPER};" 2>/dev/null)
if echo "$command_output" | grep -q "Table or view not found"; then 
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"):  表 $TABLE_NAME_LOWER 不存在，结果为空，跳过..." 
    exit 1
fi

#初始化字段序号
first_col=true

for context1 in `spark-beeline -e "desc table AGL.${TABLE_NAME_UPPER}"`;  do
    colname=$(echo ${context1}|awk -F '|' '{print $2}')
    coltype=$(echo ${context1}|awk -F '|' '{print $3}')
    colremk=$(echo ${context1}|awk -F '|' '{print $4}')
    if [ "${colname}" == "" -o "${coltype}" == "" -o "${colname:0:2}" == "  " -o "${colname:0:2}" == " #" ];then
        continue  
    fi
    
    colname=`echo "${colname}"|awk '$1=$1'`
    coltype=`echo "${coltype}"|awk '$1=$1'`
    #coltype 大写
    coltype=$(echo "$coltype" | tr '[:lower:]' '[:upper:]')




    colremk=`echo "${colremk}"|awk '$1=$1'`
    colremk=$(echo "${colremk}" | sed 's/ //g') 
    if [ "${colname}" == "PT_DT" ];then
        continue
    fi

    if [ -n "$colname" ] && [ -n "$coltype" ]; then 
        if [ "$first_col" = true ]; then 
            COLUMN_DEFINTIONS="${COLUMN_DEFINTIONS}     ${colname} ${coltype} COMMENT '${colremk}'"
            first_col=false
        else
            COLUMN_DEFINTIONS="${COLUMN_DEFINTIONS},
        ${colname} ${coltype} COMMENT '${colremk}'"
        fi
    fi
done

#echo $COLUMN_DEFINTIONS

#拼接创建外表语句
CREATE_EXT_TABLE_SQL="
DROP TABLE IF EXISTS AGL.${TABLE_NAME_UPPER}_EXT;
CREATE TABLE AGL.${TABLE_NAME_UPPER}_EXT (
${COLUMN_DEFINTIONS}
)
tblproperties("skip.header.line.count"="1")
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties
(
    'separatorChar'=',',
    'quoteChar'='\\\"',
    'escapeChar'='\\\\'
)
location '${HDFS_PATH}'
;
"

#输出并执行建表语句
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 执行建外表语句..."
echo "$CREATE_EXT_TABLE_SQL"

CREATE_EXT_RESULT=$(spark-beeline -e "$CREATE_EXT_TABLE_SQL" 2>/dev/null)
if echo "$CREATE_EXT_RESULT" | grep -q "error";then
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"):  创建外表失败：$CREATE_EXT_RESULT"
    exit 1 
fi 


#创建备份表
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 创建备份表: AGL.${TABLE_NAME_UPPER}_BAK${CURRENT_DATE}..."
BACKUP_SQL=$(spark-beeline -e " DROP TABLE IF EXISTS  AGL.${TABLE_NAME_UPPER}_BAK${CURRENT_DATE};
CREATE TABLE AGL.${TABLE_NAME_UPPER}_BAK${CURRENT_DATE} AS SELECT * FROM AGL.${TABLE_NAME_UPPER} WHERE 1=1;
" 2>/dev/null)
if echo "$BACKUP_SQL" | grep -q "Error";then
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"): 创建备份失败：$BACKUP_SQL"
    exit 1 
fi 


#备份表查询，是否创建成功
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 查询备份表是否成功: AGL.${TABLE_NAME_UPPER}_BAK${CURRENT_DATE}..."
QUERY_SQL=$(spark-beeline -e "SELECT COUNT(1) FROM  AGL.${TABLE_NAME_UPPER}_BAK${CURRENT_DATE};" 2>/dev/null)
query_backup_result=$(echo "$QUERY_SQL" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')
if echo "$query_backup_result" | grep -q "Error";then
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"): 查询备份表失败，请检查：$QUERY_SQL"
    exit 1 
fi 
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 备份表创建成功，数据量: $query_backup_result"



#导入数据到正式表
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 插入数据到AGL.${TABLE_NAME_UPPER}..."
INSERT_RESULT=$(spark-beeline -e "INSERT OVERWRITE AGL.${TABLE_NAME_UPPER} SELECT * FROM AGL.${TABLE_NAME_UPPER}_EXT;" 2>/dev/null)
if echo "$INSERT_RESULT" | grep -q "Error";then
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"): 导入数据失败：$INSERT_RESULT"
    exit 1 
fi 

#校验数据一致性

COUNT_EXT=$(spark-beeline -e "SELECT COUNT(1) FROM AGL.${TABLE_NAME_UPPER};" 2>/dev/null)
process_result_ext=$(echo "$COUNT_EXT" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')

COUNT_ORG=$(spark-beeline -e "SELECT COUNT(1) FROM AGL.${TABLE_NAME_UPPER}_EXT;" 2>/dev/null)
process_result_org=$(echo "$COUNT_ORG" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')


DIFF_COUNT=$(spark-beeline -e "
SELECT COUNT(1) FROM (
    SELECT * FROM AGL.${TABLE_NAME_UPPER}_EXT
    EXCEPT ALL 
    SELECT * FROM AGL.${TABLE_NAME_UPPER}
);" 2>/dev/null
)
process_result_diff=$(echo "$DIFF_COUNT" | awk '/\+/{if (++count==2) {getline; print}}' | tr -d '| ')


echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 外表记录数：$process_result_ext"
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 内表记录数：$process_result_org"
echo "[ INFO ] $(date +"%Y-%m-%d %H:%M:%S"): 差异记录数：$process_result_diff"

if [ "$process_result_ext" -eq "$process_result_org" ] && [ "$process_result_diff" -eq 0 ]; then
    echo "[ SUCCESS ] $(date +"%Y-%m-%d %H:%M:%S"): 数据一致性校验成功!"
else 
    echo "[ FAILED  ] $(date +"%Y-%m-%d %H:%M:%S"): 数据一致性校验失败!"
    exit 1
fi