# -*- coding:utf-8 -*-

def get_head(dict_data,level):
    template ="/*\n" \
            "*********************************************************************** \n" \
            "Purpose:       主题聚合层-加工快照表脚本\n" \
            "Author:        Sunline\n" \
            "Usage:         python $ETL_HOME/script/main.py yyyymmdd [file_name]\n" \
            "CreateDate:    [create_time]\n" \
            "FileType:      DML\n" \
            "logs:\n" \
            "       表英文名：[target_table_en_name]\n" \
            "       表中文名：[target_table_ch_name]\n" \
            "       创建日期：[create_time]\n" \
            "       主键字段：[primary_key]\n" \
            "       归属层次：[belong_level]\n" \
            "       归属主题：[belong_subject]\n" \
            "       主要应用：[main_server]\n" \
            "       分析人员：[mapping_analyst]\n" \
            "       时间粒度：[time_granule]\n" \
            "       保留周期：[keep_time]\n" \
            "       描述信息：[table_comment]\n" \
            "*************************************************************************/ \n\n" \
            "\\timing \n" \
            "/*创建当日分区*/\n" \
            "   call ${itl_schema}.partition_add('" + level + ".[target_table_en_name]','pt_${batch_date}','${batch_date}'); \n\n" \
            "/*删除当前批次历史数据*/\n" \
            "   call ${itl_schema}.partition_drop('" + level + ".[target_table_en_name]','pt_${batch_date}'); \n\n" \
            ""

    #对其中变量进行替换
    template = template.replace("[target_table_en_name]",dict_data["target_table_en_name"])
    template = template.replace("[target_table_ch_name]",dict_data["target_table_ch_name"])
    template = template.replace("[create_time]",dict_data["create_time"])
    template = template.replace("[primary_key]",dict_data["primary_key"])
    template = template.replace("[belong_level]",dict_data["belong_level"])
    template = template.replace("[belong_subject]",dict_data["belong_subject"])
    template = template.replace("[main_server]",dict_data["main_server"])
    template = template.replace("[mapping_analyst]",dict_data["mapping_analyst"])
    template = template.replace("[time_granule]",dict_data["time_granule"])
    template = template.replace("[keep_time]",dict_data["keep_time"])
    template = template.replace("[table_comment]",dict_data["table_comment"])
    template = template.replace("[file_name]",level + "_" + dict_data["target_table_en_name"].lower())
    return template
    #print(template)

def get_update_head(dict_data,level):
    template ="/*\n" \
            "*********************************************************************** \n" \
            "Purpose:       主题聚合层-加工快照表脚本\n" \
            "Author:        Sunline\n" \
            "Usage:         python $ETL_HOME/script/main.py yyyymmdd [file_name]\n" \
            "CreateDate:    [create_time]\n" \
            "FileType:      DML\n" \
            "logs:\n" \
            "       表英文名：[target_table_en_name]\n" \
            "       表中文名：[target_table_ch_name]\n" \
            "       创建日期：[create_time]\n" \
            "       主键字段：[primary_key]\n" \
            "       归属层次：[belong_level]\n" \
            "       归属主题：[belong_subject]\n" \
            "       主要应用：[main_server]\n" \
            "       分析人员：[mapping_analyst]\n" \
            "       时间粒度：[time_granule]\n" \
            "       保留周期：[keep_time]\n" \
            "       描述信息：[table_comment]\n\n" \
            "@[update_time] [update_person] [update_content] \n\n" \
            "*************************************************************************/ \n\n" \
            "\\timing \n" \
            "/*创建当日分区*/\n" \
            "   call ${itl_schema}.partition_add('" + level + ".[target_table_en_name]','pt_${batch_date}','${batch_date}'); \n\n" \
            "/*删除当前批次历史数据*/\n" \
            "   call ${itl_schema}.partition_drop('" + level + ".[target_table_en_name]','pt_${batch_date}'); \n\n" \
            ""

    #对其中变量进行替换
    #template = ""
    update_time= []
    update_time = dict_data["update_time"]
    update_person = dict_data["update_person"]
    update_content = dict_data["update_content"]

    temp = ""
    for i in range(0,len(update_time)):
        if i == 0:
            temp += "       " + update_time[i] + "    " + update_person[i] + "    " + update_content[i]
        else:
            temp += "\n       " + update_time[i] + "    " + update_person[i] + "    " + update_content[i]
    template = template.replace("@[update_time] [update_person] [update_content]",temp)
    return template
# 临时表
def get_group_template_drop_create_insert(dict_data,level):
    template = "DROP TABLE IF EXISTS " + level + ".${target_table_name};\n" \
               "CREATE GLOBAL TEMPORARY TABLE " + level + ".${target_table_name} (\n" \
               "  @[{target_column_name} {target_column_type} -- {target_column_cn_name}]\n" \
               ")\n" \
               "compress(5,5)\n" \
               "DISTRIBUTED BY ( ${db_key} );\n\n" \
               "INSERT INTO " + level + ".${target_table_name}(\n" \
               "  @[{target_column_name} -- {target_column_cn_name}]\n" \
               ")\n" \
               "SELECT\n" \
               "  @[{source_column_name_mapping} AS {source_column_name} -- {source_column_cn_name}]\n" \
               "${table_relation} " \
               "${where_condition}\n" \
               "${groupby_condition}\n"\
               ";"
    template = replace_group_template(dict_data,template)
    return template
#非临时表
def get_group_template_insert(dict_data,level):
    template = "INSERT INTO " + level + ".${target_table_name}(\n" \
               "  @[{target_column_name} -- {target_column_cn_name}]\n" \
               ")\n" \
               "SELECT \n" \
               "  @[{source_column_name_mapping} AS {source_column_name} -- {source_column_cn_name}] \n" \
               "${table_relation} " \
               "${where_condition} \n" \
               "${groupby_condition}\n"\
               ";\n"
    template = replace_group_template(dict_data,template)
    return template 
#替换group template 设置
def replace_group_template(dict_data,template):
    target_column_name = dict_data["target_column_name"]
    target_column_type = dict_data["target_column_type"]
    target_column_cn_name = dict_data["target_column_cn_name"]
    source_column_name_mapping = dict_data["source_column_name_mapping"]
    source_column_cn_name = dict_data["source_column_cn_name"]
    source_column_name = dict_data["source_column_name"]

    #组装建表目标字段列表
    temp1 = ""
    for i in range(0,len(target_column_cn_name)):
        if i == 0:
            temp1 += " " + target_column_name[i] + "  " + target_column_type[i] + " -- " + target_column_cn_name[i]
        else:
            temp1 += "\n  ," + target_column_name[i] + "  " + target_column_type[i] + " -- " + target_column_cn_name[i]
    #组装插入目标表字段列表
    temp2 = ""
    for i in range(0,len(target_column_cn_name)):
        if i == 0:
            temp2 += "  " + target_column_name[i]  + " -- " + target_column_cn_name[i]
        else:
            temp2 += "\n  ," + target_column_name[i]  + " -- " + target_column_cn_name[i]
    #组装选择来源字段列表组合
    temp3 = ""
    for i in range(0,len(source_column_name_mapping)):
        if i == 0:
            temp3 += "  " +source_column_name_mapping[i]  + " AS " + source_column_name[i] + " -- " + source_column_cn_name[i]
        else:
            temp3 += "\n  ," + source_column_name_mapping[i]  + " AS " + source_column_name[i] + " -- " + source_column_cn_name[i]
         
    #替换关键字
    template = template.replace("${target_table_name}",dict_data["target_table_name"])
    template = template.replace("${db_key}", dict_data["db_key"].replace(" ",""))
    template = template.replace("${table_relation}",dict_data["table_relation"])
    template = template.replace("${where_condition}",dict_data["where_condition"])
    template = template.replace("${groupby_condition}",dict_data["groupby_condition"])
    template = template.replace("  @[{target_column_name} {target_column_type} -- {target_column_cn_name}]",temp1)
    template = template.replace("  @[{target_column_name} -- {target_column_cn_name}]",temp2)
    template = template.replace("  @[{source_column_name_mapping} AS {source_column_name} -- {source_column_cn_name}]",temp3)
    print(template)
    return template
#删除临时表
def get_drop_table_sql(table_list,level):
    template = "DROP TABLE IF EXISTS " + level + ".${target_table_name};\n"
    sql = "/*删除所有临时表*/\n"
    for i in range(0,len(table_list)):
        sql += template.replace("${target_table_name}",table_list[i])
    return sql
#事件统计Postgresql Greenplum GaussDB OushuDB 适用
def get_analyst_sql(target_table_name,level):
    #target_table_name = dict_data["target_table_name"]
    template = "\n\n\n"\
               "/*添加表分析*/ \n" \
               "ANALYZE TABLE " + level + ".${target_table_name};\n"
    sql = template.replace("${target_table_name}",target_table_name)
    return sql

#收尾代码
def get_footer(target_table_en_name,level):
    template = "\n\n\n"\
               "/*添加目标表分析*/ \n" \
               "\\echo \"4.analyze table\" \n" \
               "ANALYZE TABLE " + level + ".${target_table_en_name};\n"
    sql = template.replace("${target_table_en_name}",target_table_en_name)
    return sql

# Print or use script_content as needed
# if __name__ == "__main__":
#     print(get_head("1","icl"))

def pad_string_to_length(s, length):
   return s.ljust(length)

def format_source_column_string(source_column_name, source_column_name_mapping):
   max_length = max(len(source_column_name[i]) + len(source_column_name_mapping[i]) + 4 for i in range(len(source_column_name)))
   formatted_strings = []
   for i in range(len(source_column_name)):
       formatted_strings.append(pad_string_to_length("  " + source_column_name_mapping[i] + " AS " + source_column_name[i], max_length))
   return formatted_strings
