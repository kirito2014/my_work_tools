import pandas as pd
from  openpyxl import load_workbook
import xlrd
import os


#data = pd.read_excel(file_path,engine='openpyxl',sheet_name = '6.表设计', usecols="A,C")
#print(data)

def read_excel(file_path):
    try:
        data = pd.read_excel(file_path,sheet_name = '7.字段设计')
        data_df = data[3:]
        data_df.columns = data.iloc[0]
        data_df.reset_index(inplace=True,drop=True)
        print(data_df)
    except Exception as e:
        print(f"源文件打开失败：{e}，尝试保存为xls文件并重试")

 def write_excel(file_path):
    try:

    except Exception as e       

if __name__ == "__main__":
    file_path = 'D:/git/tools/01_gather_design_file_infos/test/第1.1批-信贷组-对公贷款资产分类形态变更信息聚合-姚依朋.xls'
    read_excel_with_recovery(file_path)
