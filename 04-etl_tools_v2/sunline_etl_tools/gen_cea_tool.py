# -*- coding:utf-8 -*-
import os, sys
sys.path.append(r'D:\vscode\sunline_etl_tools\package')
#获取本文件路径



from package.dal import excelhelper
from package.cea import cea_excel_help,cea_template_help
from package.utils import confighelper,log,filehelper

_etl_home = confighelper.get_etl_home()
_autocode_path = os.path.join(_etl_home,"autocode")
_log_file_path = os.path.join(_etl_home, 'logs', 'log_file.log')
#_file_path = os.path.dirname(os.path.realpath(__file__))

#print(_file_path)
#print(_autocode_path)
if __name__ == '__main__':
    logger = log.Logger.get_instance(_log_file_path)
    #log_file_path = os.path.join(_etl_home, 'logs', 'your_log_file.log')
    level =  "icl"
    logger.info("*****************层级获取成功，归属层级为" + level + "*******************")
    #level = sys.argv[1]
    _level = "icl"
    #_level = sys.argv[1]
    file_path = _etl_home  + "\\SDM开发_20231225_王穆军.xlsm"
    #print(file_path)
    #file_path = sys.argv[2] 
    
    if level.lower() == "icl":
        level = "${icl_schema}"
    elif level.lower() == "idl":
        level = "${idl_schema}"
    else:
        logger.error("level参数错误,不支持的层级:level=" + level)
        raise Exception("level参数错误,不支持的层级:level=" + level)
        

    excel = excelhelper.Xlsx(file_path)
    book = excel.book
    groupIdxs = []
    for sheet_name in book.sheetnames:
        if sheet_name == "index" or sheet_name == "模板" or sheet_name.startswith("rem"):
            continue
        file_sql = ""
        #基本信息
        logger.info("**********************开始获取基本信息*********************" )
        base_data = cea_excel_help.get_sheet_data_base_info(book[sheet_name])
        base_dict = cea_excel_help.get_sheet_data_base_info_dict(base_data)
        update_data = cea_excel_help.get_update_record_data(book[sheet_name])
        #转换成三维数组的GROUP信息
        logger.info("**********************开始获取加工信息*********************" )

        group_data = cea_excel_help.get_sheet_data_by_group(book[sheet_name])
        #print(cea_excel_help.echo_data_3(group_data))
        #遍历每个分组
        group_sql = ""
        analyst_sql = ""
        temp_table_list = []
        for i in range(0,len(group_data)):
            #分组字典信息
            group_dict_info = cea_excel_help.get_group_data_infos(group_data[i])
            #目标表信息
            target_table = cea_excel_help.get_group_data_target_table(group_data[i])
            is_temp_table = target_table["is_temp_table"]
            target_table_name = target_table["target_table_name"]
            time_granule = base_dict["time_granule"] 
            #todo:时间粒度判断 每日每月添加删除语句算法配置，根据层级 算法分配模板



            template_sql = "\n\n/*===================第" + str((i + 1)) + "组====================*/\n\n"
            if is_temp_table.upper() == "Y":
                template_sql += cea_template_help.get_group_template_drop_create_insert(group_dict_info,level)
                temp_table_list.append(target_table_name)
                analyst_sql = cea_template_help.get_analyst_sql(temp_table_list[i],level)
                template_sql += analyst_sql
            elif is_temp_table.upper() == "N":
                template_sql += cea_template_help.get_group_template_insert(group_dict_info,level)
                #analyst_sql = cea_template_help.get_analyst_sql(temp_table_list[i - 1],level)
            else:
                raise Exception("填写错误，字段映射（第" + str((i + 1)) + "组）的'是否临时表'字段只能填写Y/N 实际填写值：" + is_temp_table)
            group_sql += template_sql
            template_sql = ""
        file_sql += cea_template_help.get_head(base_dict,level)
        file_sql += group_sql 
        #创建的是临时表，会话级临时表待提交后自动删除，若是其他
        #file_sql += cea_template_help.get_drop_table_sql(temp_table_list,level)
        file_sql += cea_template_help.get_footer(base_dict["target_table_en_name"],level)

        sql_file_path = _autocode_path + "\\dml\\" + _level + "\\" + _level + "_" + sheet_name.lower() + ".sql"

        logger.info("生成执行脚本 :\n%s" %file_sql) 
        if filehelper.write_file(sql_file_path,file_sql):
            logger.info("生成文件成功,路径: %s" %sql_file_path)
        else:
            logger.error("生成文件失败！！,检查源文件填写: %s" %sql_file_path)