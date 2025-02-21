#!/usr/bin/env python3
# -*- coding:utf-8 -*-



import os
import sys
import time
s_t = time.time()
import sys
import datetime
import os.path
import openpyxl
import xlrd
from xlutils.copy import copy
from openpyxl import load_workbook
import csv
from package.utils import excelhelper, filehelper

#设置文件夹路径和输出文件名
output_file = '2.1_模型列表-1.csv'
folder_path = 'D:\\git\\bdp-imp\\02_需求开发\\R0001_24年3月聚合模型\\00_模型设计\\模型发布_全量\\0004客户组'
#创建一个空的CSV文件存储结果
with open(output_file,'w',newline='') as f:
    writer = csv.writer(f)
    #写入表头
    writer.writerow(['文件名','主题对象','主题域分类（一级）','主题域分类（二级）','聚合表英文名','聚合表中文名','主题聚合标签','主题聚合粒度','数据范围描述','数据口径描述','数据使用场景'])

#遍历文件夹中的所有excel文件
for filename in os.listdir(folder_path):
    if filename.endswith('.xls'): #只处理.xlsx文件
        file_path= os.path.join(folder_path,filename)
        try:
            wb = xlrd.open_workbook(file_path)
            sheet = wb.sheet_by_name('6.表设计')  #替换为你的sheet名
            c3_value = sheet.cell(2,2).value  #提取C3的值
            c4_value = sheet.cell(3,2).value  #提取C4的值
            c5_value = sheet.cell(4,2).value  #提取C5的值
            c6_value = sheet.cell(5,2).value  #提取C6的值
            c7_value = sheet.cell(6,2).value  #提取C7的值
            print(c7_value)
            c8_value = sheet.cell(7,2).value  #提取C8的值
            c9_value = sheet.cell(8,2).value  #提取C9的值
            c10_value = sheet.cell(9,2).value  #提取C10的值
            c11_value = sheet.cell(10,2).value  #提取C11的值
            c12_value = sheet.cell(11,2).value  #提取C11的值

            # 写入到CSV文件中 ,encoding='utf-8'
            with open(output_file,'a',newline='') as f:
                writer = csv.writer(f)
                writer.writerow([filename,c6_value,c7_value,c8_value,c9_value,c10_value,c11_value])
        except Exception as e:
            print(f"处理文件 {filename}时出错：{e}")
    elif filename.endswith('.xlsx'): #只处理.xlsx文件
        file_path= os.path.join(folder_path,filename)
        try:
            wb = load_workbook(file_path)
            sheet = wb['6.表设计']  #替换为你的sheet名
            c3_value = sheet['C3'].value  #提取C3的值
            c4_value = sheet['C4'].value  #提取C4的值
            c5_value = sheet['C5'].value  #提取C5的值
            c6_value = sheet['C6'].value  #提取C6的值
            c7_value = sheet['C7'].value  #提取C7的值
            print(c7_value)
            c8_value = sheet['C8'].value  #提取C8的值
            c9_value = sheet['C9'].value  #提取C9的值
            c10_value = sheet['C10'].value  #提取C10的值
            c11_value = sheet['C11'].value  #提取C11的值
            c12_value = sheet['C12'].value  #提取C11的值
            

            # 写入到CSV文件中 ,encoding='utf-8'
            with open(output_file,'a',newline='') as f:
                writer = csv.writer(f)
                writer.writerow([filename,c6_value,c7_value,c8_value,c9_value,c10_value,c11_value])
        except Exception as e:
            print(f"处理文件 {filename}时出错：{e}")
            
print(f"已完成提取，结果保存到{output_file}")



#wb =load_workbook("data\")

#