# -*- coding:utf-8 -*-
import os, sys
sys.path.append(r'D:\vscode\sunline_etl_tools\package')
#获取本文件路径

from package.dal import excelhelper
from package.cea import cea_excel_help,cea_template_help
from package.utils import confighelper


_etl_home = confighelper.get_etl_home()


file_path = _etl_home  + "\\icl_c_pt_corp_cust_perf_rela1.xlsx"
excel = excelhelper.Xlsx(file_path)
book = excel.book
update_data = []
for sheet_name in book.sheetnames:
    if sheet_name == "index" or sheet_name == "模板" or sheet_name.startswith("rem"):
        continue
    update_data = cea_excel_help.get_update_record_data(book[sheet_name])

update_dict = []
update_dict = cea_excel_help.get_update_record_dict(update_data)   
sql = cea_template_help.get_update_head(update_dict,"icl")
print(sql)