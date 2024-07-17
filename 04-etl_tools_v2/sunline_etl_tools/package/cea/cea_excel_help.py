# -*- coding:utf-8 -*-
import sys
sys.path.append(r'D:\vscode\sunline_etl_tools\package')

from package.utils import log
def get_group_data_target_table(group_list):
    logger = log.Logger.get_instance()
    target_table = {}
    find = 0
    find1 = 0
    find2 = 0
    value = ""
    for i in range(0,len(group_list)):
        for j in range(0,len(group_list[i])):
            cell = group_list[i][j]
            if cell is not None:
                cell = str(cell)
                if cell.startswith("是否临时表"):
                    target_table["is_temp_table"] = group_list[i][j + 1 ]
                    find = 1
                if cell.startswith("英文名称"):
                    target_table["target_table_name"] = group_list[i][j + 1 ]
                    find1 = 1
                if cell.startswith("时间粒度"):
                    target_table["time_granule"] = group_list[i][j + 1 ]
                    find2 = 1
        if find == 1 and find1 == 1 and find2 == 1:
            break
    logger.debug("target_table=%s" ,target_table)
    return target_table

#获取group 的相关信息
def get_group_data_infos(group_list):
    logger = log.Logger.get_instance()
    dict_info = {}
    #目标表
    target_column_name = []
    target_column_type = []
    target_column_cn_name = []

    #源表
    source_column_name = []
    source_column_name_mapping = []
    source_column_cn_name = []

    #表字段详细信息辅助变量
    table_detail_row_start = 0
    table_detail_row_end = 0 

    #表关联辅助变量
    table_relation_row_start = 0
    table_relation_row_end = 0 

    #其他变量

    find1 = 0 #英文名是否存在 0否1是
    find2 = 0 #分组条件是否存在 0否1是
    for i in range(0,len(group_list)):
        for j in range(0,len(group_list[i])):
            cell = group_list[i][j]
            if cell is not None:
                cell = str(cell)
                
                if j == len(group_list[i]) - 1:
                    value = str(group_list[i][j])
                else:
                    value = str(group_list[i][ j + 1 ])
                    if value == "None":
                        value = " "
                    
                if cell.startswith("英文名称"):
                    if find1 == 0 :
                        dict_info["target_table_name"] = value 
                        find1 = 1 
                    else:
                        logger.warning("多个单元格英文名称重复：target_table_name = " + dict_info["target_table_name"])
                elif cell == "目标表":
                    if table_detail_row_start == 0:
                        table_detail_row_start = i + 2 
                    else:
                        logger.warning("多个单元格目标表重复：table_detail_row_start = " + table_detail_row_start)
                elif cell.startswith("分布键"):
                    if table_detail_row_end == 0 :
                        dict_info["db_key"] = value
                        table_detail_row_end = i - 1
                    else:
                        raise Exception(
                            "多个单元格分布键重复：table_detail_row_end = " + table_detail_row_end + ",db_key=" + dict_info["db_key"])
                elif cell.startswith("表关联信息"):
                    if table_relation_row_start == 0 :
                        table_relation_row_start = i + 2
                    else:
                        raise Exception(
                            "多个单元格表关联信息重复：table_relation_row_start = " + table_relation_row_start + ",value=" + value)
                elif cell.startswith("过滤条件"):
                    if table_relation_row_end == 0:
                        if value == " ":
                            dict_info["where_condition"] = value
                        else:
                            dict_info["where_condition"] = "WHERE " + value
                        table_relation_row_end = i - 1
                    else:
                        raise Exception(
                            "多个单元格过滤条件重复：table_relation_row_end = " + table_relation_row_end + ",where_condition=" + dict_info["where_contion"])
                elif    cell.startswith("分组条件"):
                    if value == " ":
                        dict_info["groupby_condition"] = value
                    else:
                        dict_info["groupby_condition"] = " GROUP BY " + value
# 载入并加载解析目标表和源表
    for j in range(0,len(group_list[table_detail_row_start - 1 ])):
        cell = group_list[table_detail_row_start - 1 ][j]
        if cell is not None:
            cell = str(cell)
            if cell.startswith("字段英文名"): #target_column_name
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    target_column_name.append(str(group_list[i][j]))
            if cell.startswith("字段中文名"): #target_column_cn_name
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    target_column_cn_name.append(str(group_list[i][j]))
            if cell.startswith("字段类型"): #target_column_type
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    target_column_type.append(str(group_list[i][j]))
            if cell.startswith("映射规则"): #source_column_name_mapping
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    source_column_name_mapping.append(str(group_list[i][j]))
            if cell.startswith("源字段中文名"): #source_column_cn_name
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    source_column_cn_name.append(str(group_list[i][j]))            
            if cell.startswith("源字段英文名"): #source_column_name
                for i in range(table_detail_row_start,table_detail_row_end + 1):
                    source_column_name.append(str(group_list[i][j]))
    #表字段详细信息合法性校验
    if len(target_column_name) != len(target_column_type) or \
        len(target_column_name) != len(target_column_cn_name) or \
        len(target_column_name) != len(source_column_name_mapping) or \
        len(target_column_name) != len(source_column_cn_name) or \
        len(target_column_name) != len(source_column_name):
        raise Exception("表字段详细信息长度不一致")
    
    #过滤空行
    length = len(target_column_name)
    i = 0
    while i < length:
        #print(target_column_name[i] + target_column_type[i])
        if target_column_name[i].lower() == "none" and \
            target_column_type[i].lower() == "none" and \
            target_column_cn_name[i].lower() == "none" and \
            source_column_name_mapping[i].lower() == "none" and \
            source_column_cn_name[i].lower() == "none" and \
            source_column_name[i].lower() == "none":
            target_column_name.pop(i)
            target_column_type.pop(i)
            target_column_cn_name.pop(i)
            source_column_name_mapping.pop(i)
            source_column_cn_name.pop(i)
            source_column_name.pop(i)
            i = 0 
            length = len(target_column_name)
        else:
            i = i + 1
    dict_info["target_column_name"] = target_column_name
    dict_info["target_column_type"] = target_column_type
    dict_info["target_column_cn_name"] = target_column_cn_name
    dict_info["source_column_name"] = source_column_name
    dict_info["source_column_cn_name"] = source_column_cn_name
    dict_info["source_column_name_mapping"] = source_column_name_mapping

    #解析表关联关系
    # source_table_schema_index = 0 #源表schema
    # source_table_cn_name_index = 1 #源表中文名
    # source_table_en_name_index = 2 #源表英文名
    # source_table_alias_index = 3 #源表别名
    # relation_type = 4 #关联关系
    # relation_on = 5 #关联条件
    source_table_schema_index = 1 #源表schema
    source_table_cn_name_index = 2 #源表中文名
    source_table_en_name_index = 3 #源表英文名
    source_table_alias_index = 4 #源表别名
    relation_type = 5 #关联关系
    relation_on = 6 #关联条件
    table_relation = ""
    for i in range(table_relation_row_start,table_relation_row_end + 1 ):
        if group_list[i][source_table_schema_index] is None and \
                group_list[i][source_table_en_name_index] is None and \
                group_list[i][source_table_alias_index] is None and \
                group_list[i][relation_type] is None and \
                group_list[i][relation_on] is None:
            continue #空行过滤
        #主表
        if i == table_relation_row_start:
            if group_list[i][relation_type] is not None:
                print(group_list[i])
                raise Exception("表关联关系第一行必须是主表")
            if group_list[i][source_table_schema_index] is not None:
                table_relation += " FROM " + str(group_list[i][source_table_schema_index]) + "." + \
                                  str(group_list[i][source_table_en_name_index]) + \
                                  "  " + str(group_list[i][source_table_alias_index]) + \
                                  " -- " + str(group_list[i][source_table_cn_name_index]) + " \n"
            else:
                table_relation += " FROM " + str(group_list[i][source_table_en_name_index]) + \
                                  "  " + str(group_list[i][source_table_alias_index]) + \
                                  " -- " + str(group_list[i][source_table_cn_name_index]) + " \n"
        #非主表
        else:
            if group_list[i][source_table_schema_index] is not None:
                table_relation += str(group_list[i][relation_type]) + " " + \
                                  str(group_list[i][source_table_schema_index]) + "." + \
                                  str(group_list[i][source_table_en_name_index]) + \
                                  "  " + str(group_list[i][source_table_alias_index]) + \
                                  " -- " + str(group_list[i][source_table_cn_name_index]) + " \n" \
                                  " ON " + str(group_list[i][relation_on]) + "\n"
            else:
                table_relation += str(group_list[i][relation_type]) + " " + \
                                  str(group_list[i][source_table_en_name_index]) + \
                                  "  " + str(group_list[i][source_table_alias_index]) + \
                                  " -- " + str(group_list[i][source_table_cn_name_index]) + " \n" \
                                  " ON " + str(group_list[i][relation_on]) + "\n"
    dict_info["table_relation"] = table_relation
    logger.debug("dict_info:\n %s",dict_info)
    return dict_info

   
    
""" 
提取sheet中的基本信息,转换为字典
 """
def get_sheet_data_base_info_dict(data_list):
    dict_base_info = {}
    for i in range(0,len(data_list)):
        for j in range(0,len(data_list[i])):
            key = data_list[i][j]
            #print(key)
            if key is not None:
                key = str(key)
                value = ""
                if j < len(data_list[i]) - 1:
                    value = str(data_list[i][ j + 1 ])
                if key.startswith("中文名称"):
                    dict_base_info["target_table_ch_name"] = value
                elif  key.startswith("英文名称"):
                    dict_base_info["target_table_en_name"] = value
                elif key.startswith("上线日期"):
                    dict_base_info["create_time"] = value
                elif key.startswith("创建日期"):
                    dict_base_info["create_time"] = value
                elif key.startswith("归属层次"):
                    dict_base_info["belong_level"] = value
                elif  key.startswith("归属主题"):
                    dict_base_info["belong_subject"] = value
                elif  key.startswith("一级领域"):
                    dict_base_info["belong_subject"] = value
                elif  key.startswith("主要应用"):
                    dict_base_info["main_server"] = value
                elif  key.startswith("下游应用"):
                    dict_base_info["main_server"] = value
                elif  key.startswith("分析人员"):
                    dict_base_info["mapping_analyst"] = value
                elif  key.startswith("时间粒度"):
                    dict_base_info["time_granule"] = value
                elif  key.startswith("保留周期"):
                    dict_base_info["keep_time"] = value
                elif  key.startswith("注释"):
                    dict_base_info["table_comment"] = value
                elif key.startswith("主键字段"):
                    dict_base_info["primary_key"] = value
                #print(dict_base_info["create_time"])
    return dict_base_info

""" 获取sheet的基本信息二维数组 """

def get_sheet_data_base_info(sheet):
    row_count = sheet.max_row
    col_count = sheet.max_column
    print("row_count:",row_count)
    print("col_count:",col_count)
    data_list = []

    for i in range(1,row_count + 1 ):
        row = []
        endName = sheet.cell(row = i, column = 2).value
        if (endName is not None) and (str(endName).startswith("字段映射")):
            break
        for j in range(1,col_count + 1 ):
            row.append(sheet.cell(row=i,column=j).value)
        data_list.append(row)
    return data_list

""" 获取更新记录的二维数组 """
def get_update_record_data(sheet):
    row_count = sheet.max_row
    col_count = 5
    print("row_count:",row_count)
    print("col_count:",col_count)
    data_list = []
    for t in range(1,row_count + 1 ):
        startName = sheet.cell(row=t,column=2).value
        if (startName is not None) and (str(startName).startswith("更新记录")):
            for i in range(t + 1 ,row_count + 1):
                row = []
                endName = sheet.cell(row=i,column=2).value
                if (endName is not None) and (str(endName).startswith("字段映射")):
                    break
                for j in range(2 ,col_count):
                    row.append(sheet.cell(row=i,column=j).value)
                data_list.append(row)    
    return data_list

""" 将更新记录转为数组 """
def get_update_record_dict(update_data):

    update_dict = {}
    #定义更新日期变量
    update_time = []
    update_person = []
    update_content = []

    for i in range(0,len(update_data) - 1 ):
        cell = update_data[0][i]
        if cell is not None:
            cell = str(cell)
            if cell.startswith("日期"): #update_time
                for j in range(0,len(update_data)):
                    update_time.append(str(update_data[j][i]))
            if cell.startswith("更新人"): #update_person
                for j in range(0,len(update_data)):
                    update_person.append(str(update_data[j][i]))
            if cell.startswith("说明"): #update_content
                for j in range(0,len(update_data)):
                    update_content.append(str(update_data[j][i]))
    #过滤空行
    length = len(update_time)
    i = 0
    while i < length:
        #print(target_column_name[i] + target_column_type[i])
        if update_time[i].lower() == "none" and \
            update_person[i].lower() == "none" and \
            update_content[i].lower() == "none" :
            update_time.pop(i)
            update_person.pop(i)
            update_content.pop(i)
            i = 0 
            length = len(update_time)
        else:
            i = i + 1
    update_dict["update_time"] = update_time
    update_dict["update_person"] = update_person
    update_dict["update_content"] = update_content
    print ("更新日期：" + update_dict["update_time"][0])
    return update_dict    

""" 获取字段映射的三位数组 """
def get_sheet_data_by_group(sheet):
    row_count = sheet.max_row
    col_count = sheet.max_column
    print("row_count:",row_count)
    print("col_count:",col_count)
    data_list = []

    for i in range(1,row_count + 1):
        group_list = []
        groupName = sheet.cell(row=i,column=2).value
        if (groupName is not None) and (str(groupName).startswith("字段映射")):
            for j in range(i,row_count + 1 ):
                row = []
                endName = sheet.cell(row=j,column=2).value
                if (endName is not None) and (str(endName).startswith("分组条件")):
                    for k in range(1,col_count + 1):
                        row.append(sheet.cell(row=j,column=k).value)
                    group_list.append(row)
                    break
                for k in range(1,col_count + 1):
                    row.append(sheet.cell(row=j,column=k).value)
                group_list.append(row)
        if len(group_list) > 0:
            data_list.append(group_list)
    return data_list

""" 打印二维数组 """
def echo_data_2(data_list):
    for i in range(len(data_list)):
        lines = ""
        for j in range(len(data_list[i])):
            if data_list[i][j] is None:
                lines += "None" + "\t"
            else:
                lines += str(data_list[i][j]) + "\t"
        print("row" + "-" + str(i + 1) + "\t" + lines)

""" 打印三维数组 """
def echo_data_3(data_list):
    for i in range(len(data_list)):
        print("++++++++++++++++++++第", i + 1,"组++++++++++++++++++++" )
        for j in range(len(data_list[i])):
            lines = ""
            for k in range(len(data_list[i][j])):
                if data_list[i][j][k] is None:
                    lines += "None" + "\t"
                else:
                    lines += data_list[i][j][k] + "\t"
            print("row" + "-" + str(j + 1) + "\t" + lines)