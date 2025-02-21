#!/bin/bash
##################
#Description：MRS数据下载程序
#
#
##################
# 获取SCHEMA TABLE 取数条件

rowid=$1
dt=$(date "+%Y%m%d")
tab_list="./table_yg.list"
file_num=$(wc -l < ${tab_list})
file_path=$(cd "$(dirname $0)";pwd)
echo $file_path

export KRB5CCNAME="/tmp/krb5_manual_exp"
kinit -kt /home/dsadm/userkey/etlclear.keytab etlclear

read schema_name table_name filter_criteria <<< $(awk -F '|' 'NR=='${rowid}'{print $1,$2,$3}' ${tab_list})	
#echo ${filter_criteria}
where_cond=`echo ${filter_criteria} | sed -e 's/${dt}/'${ten_dt}'/'`
echo "${schema_name} ${table_name} ${where_cond}"

outsqlfile=$file_path/outsql/create_${schema_name}_${table_name}_ext.sql
oss_path="oss://dl-bucket/mrs-manual_exp/data/manual_${schema_name}_${table_name}/"

echo -n > ${outsqlfile}
echo '' > ${outsqlfile}
echo "DROP TABLE IF EXISTS manual_exp.manual_${schema_name}_${table_name};" >> ${outsqlfile}
default_ifs=$IFS
IFS=$'\n'
start_flag="0"
for context in `spark-beeline -e "show create table ${schema_name}.${table_name}"`;
do
   if [ "${context:2:12}" == "CREATE TABLE" ]; then
      start_flag="1"
      context="CREATE EXTERNAL TABLE manual_exp.manual_${schema_name}_${table_name}("
   fi
   if [ "${context:0:5}" == "USING" ]; then 
      start_flag="0"
   fi

   if [ "${start_flag}" == "1" ];then
       echo ${context} >> ${outsqlfile}
   fi
done
  
echo "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'" >> ${outsqlfile}
echo "LOCATION '${oss_path}'"  >> ${outsqlfile}
echo ";"  >> ${outsqlfile}
# echo "${context:2:12}" >> ./log

IFS=$'\n'
first_flag="1"
all_column=""
for context1 in `spark-beeline -e "desc table ${schema_name}.${table_name}"`; 
do
   colname=$(echo ${context1}|awk -F '|' '{print $2}')
   coltype=$(echo ${context1}|awk -F '|' '{print $3}')
   if [ "${colname}" == "" -o "${coltype}" == "" -o "${colname:0:2}" == "  " -o "${colname:0:2}" == " #" ];then
      continue  
   fi
   colname=`echo "${colname}"|awk '$1=$1'`
   coltype=`echo "${coltype}"|awk '$1=$1'`
 
   if [ "${colname}" == "pt_dt" ];then
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
    
echo "insert into manual_exp.manual_${schema_name}_${table_name}" >> ${outsqlfile}
echo "select ${all_column},pt_dt" >> ${outsqlfile}
echo "from ${schema_name}.${table_name}" >> ${outsqlfile}
echo "${where_cond};" >> ${outsqlfile}
IFS=$default_ifs

ossutil64 rm -rf ${oss_path}

sys=$(echo ${schema_name} |awk -F '_' '{print $2}' | tr '[A-Z]' '[a-z]')
sys_up=$(echo ${sys}|tr '[a-z]' '[A-Z]')
spark-beeline -f "${outsqlfile}"
if [ $? -ne 0 ]; then
   echo "exec outsql error!"
   exit 1;
fi 

#创建存储目录
local_path="/gpfscdc/output/${dt}"
# echo "${local_path}"
mkdir -p ${local_path}

local_file="${local_path}/${schema_name}_${table_name}_${dt}.txt"
local_file_tmp="${local_path}/${schema_name}_${table_name}_${dt}.txt.tmp"
#echo "local_file_tmp:${local_file_tmp}"
if [ -f ${local_file} ]; then
   echo "delete local file"
   rm -rf ${local_file}
fi

#获取oss目录清单
oss_file_list=`ossutil64 ls ${oss_path} | awk '{print $8}' | sort -f`
#echo ${oss_file_list}
#下载数据文件到本地，并合并为一个文件
file_nums=` ossutil64 ls ${oss_path}|grep "Object Number is:"|awk -F 'Object Number is:' '{print $2}'| sed 's/ //'`
num=0
echo "merge file oss to local start:"
for oss_file in ${oss_file_list[@]}
do
  echo > ${local_file_tmp}
  ((num++))
  echo "[${num}/${file_nums}]${oss_file}"
  ossutil64 cat ${oss_file} >> ${local_file_tmp}
  #处理掉cat oss文件出来的非数据内容
  #sed -i '/^$/d' ${local_file}
  #sed -i '$d' ${local_file}
  tac ${local_file_tmp} | sed '1,3d' |tac >> ${local_file}
done
rm -rf ${local_file_tmp}
echo "merge oss file to local end"
