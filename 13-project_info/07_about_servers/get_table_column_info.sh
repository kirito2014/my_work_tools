#!/bin/bash
schema_name='AGL'
# table_name='AGL_SUPCHA_CORE_CORP_UPDWN_STR_CUST_INFO_TA'
#初始化文件信息
INPUT_FILE="get_table_column_info_list.txt"
TEMP_FILE="processd_file.txt"
OUTPUT_FILE="output.csv"

#初始化文件表头
echo "表序号,作业编号,库名,表英文名,表中文名,字段序号,字段英文名,字段注释,字段类型" > "$OUTPUT_FILE"

#文件去重并转换为英文大写
awk -F '|' '!seen[$4]++ {print $1 "|" $2 "|" $3 "|" toupper($4)}' "$INPUT_FILE" > "$TEMP_FILE"

IFS=$'\n'
first_flag="1"
all_column=""

while IFS="|" read -r table_id job_number table_cn_name table_name; do
   echo "处理表 ${table_name} (${table_cn_name})..."

   command_output=$(spark-beeline -e "desc table ${schema_name}.${table_name};" 2>&1)
   if echo "$command_output" | grep -q "Table or view not found"; then 
      echo "表 $table_name 不存在，结果为空，跳过..." 
      continue
   fi

   #初始化字段序号
   field_no=1

   for context1 in `spark-beeline -e "desc table ${schema_name}.${table_name}"`;  do
      colname=$(echo ${context1}|awk -F '|' '{print $2}')
      coltype=$(echo ${context1}|awk -F '|' '{print $3}')
      colremk=$(echo ${context1}|awk -F '|' '{print $4}')
      if [ "${colname}" == "" -o "${coltype}" == "" -o "${colname:0:2}" == "  " -o "${colname:0:2}" == " #" ];then
         continue  
      fi
      colname=`echo "${colname}"|awk '$1=$1'`
      coltype=`echo "${coltype}"|awk '$1=$1'`

      if [[ "$coltype" == *","* ]]; then
         coltype="\"$coltype\""
      fi

      colremk=`echo "${colremk}"|awk '$1=$1'`
      colremk=$(echo "${colremk}" | sed 's/ //g') 
      if [ "${colname}" == "PT_DT" ];then
         continue
      fi

      line="${table_id},${job_number},${schema_name},${table_name},${table_cn_name},${field_no},${colname},${colremk},${coltype}"
      echo "$line" | tr '[:lower:]' '[:upper:]' >> "$OUTPUT_FILE"
      field_no=$((field_no + 1))
      #all_column="${all_column}$(echo "$line" | tr '[:lower:]' '[:upper:]')\n"
      first_flag="0"
   done
   line="${table_id},${job_number},${schema_name},${table_name},${table_cn_name},${field_no},PT_DT,表分区日期,STRING"
   echo "$line" | tr '[:lower:]' '[:upper:]' >> "$OUTPUT_FILE"
   #all_column="${all_column}$(echo "$line" | tr '[:lower:]' '[:upper:]')\n"

   if [[ $field_no -eq 1 ]];then
      echo "$table_name 的字段信息为空,未生成字段信息文件"
   #echo -e "$all_column"
   else 
      echo "$table_name 的字段信息已保存到$OUTPUT_FILE 文件中"
   fi
done < "$TEMP_FILE"

#完成
echo "所有表处理完成，结果已保存在$OUTPUT_FILE 文件中"
#rm $TEMP_FILE
