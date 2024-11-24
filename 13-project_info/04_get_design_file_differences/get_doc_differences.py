#获取Excel文件差异信息，用于比对设计文档差异信息

import os
import sys
import re,sys
import openpyxl
import pandas as pd
import numpy as np
import string
from datetime import datetime,date
from openpyxl.styles import Alignment
from openpyxl.utils import get_column_letter


def column_index_to_string(index):
    string_index = ''
    while index >= 0:
        remainder = index % 26
        string_index = string.ascii_uppercase[remainder] + string_index
        index = index // 26 - 1
    return string_index

def process_dataframe(df):
    #新增列记录原始列序号
    df['_original_index'] = df.index
    df = df.iloc[3:,:24] #从第四行开始.到Y列
    df.columns = ['group_no','ser_no'] + [f'columns_{i + 1}' for i in range(22)]
    #重命名原始序列
    df['_original_index'] = df.index
    #df.rename(columns={'columns_25' : '_original_index'},inplace=True)
    #print(df)
    return df

def find_added_and_deleted_rows(df_a,df_b):


    mergedA = df_b.merge(df_a, on=df_b.columns[3],how = 'left',indicator=True)
    mergedB = df_b.merge(df_a, on=df_b.columns[3],how = 'right',indicator=True)
    added_rows = mergedA[mergedA['_merge'] == 'left_only'].drop(columns=['_merge'])
    added_rows.reset_index(inplace=True)
    added_rows =  added_rows.iloc[:,3:5].drop_duplicates()
    added_rows_indices = added_rows.index.tolist()
    added_rows.columns = ['column_cn_name','column_en_name']

    deleted_rows = mergedB[mergedB['_merge'] == 'right_only'].drop(columns=['_merge'])
    deleted_rows.reset_index(inplace=True)
    deleted_rows =  deleted_rows.iloc[:,3:5].drop_duplicates()
    deleted_rows.columns = ['column_cn_name','column_en_name']
    deleted_rows_indices = deleted_rows.index.tolist()

    return added_rows_indices,deleted_rows_indices,added_rows,deleted_rows



def compare_sheets(file_a,file_b,sheet_name):
    
    #读取sheet页
    df_a = pd.read_excel(file_a,sheet_name=sheet_name)
    df_b = pd.read_excel(file_b,sheet_name=sheet_name)

    #输出处理后的数据框架去除前三行并重新命列名

    df_a_new = process_dataframe(df_a)
    df_b_new = process_dataframe(df_b)

    #print(df_a_new)

    #获取变更序号和变更信息
    added_rows_indices,deleted_rows_indices,df_added_rows,df_deleted_rows = find_added_and_deleted_rows(df_a_new,df_b_new)

    row_changes = [(f"2--新增:{len(added_rows_indices)} 行, 删除:{len(deleted_rows_indices)} 行 \n")]
    #print(row_changes)

    #字段变更数据集备用
    added_rows = df_added_rows.iloc[added_rows_indices]
    deleted_rows = df_deleted_rows.iloc[deleted_rows_indices]


    #拼接字段变更的实际内容
    add_row_list = []
    for idx in added_rows_indices:
            column_chn_name = df_added_rows.loc[idx, 'column_cn_name']
            column_eng_name = df_added_rows.loc[idx, 'column_en_name']
            add_row_list.append((idx,column_chn_name,column_eng_name))

    del_row_list = []
    for idx in deleted_rows_indices:
            column_eng_name = df_deleted_rows.loc[idx, 'column_en_name']
            del_row_list.append((idx,column_eng_name))

    #////////////////////////////获取中英文名称变更记录/////////////////////////////


    df_a_new.reset_index(drop=True,inplace=True)
    df_b_new.reset_index(drop=True,inplace=True)

    #读取file_b的信息
    df_a_info = pd.read_excel(file_a,sheet_name="6.表设计")
    df_b_info = pd.read_excel(file_b,sheet_name="6.表设计")

    table_en_name_b = df_b_info.at[4,df_b_info.columns[2]]
    table_en_name_a = df_a_info.at[4,df_a_info.columns[2]]

    table_cn_name_b = df_b_info.at[5,df_b_info.columns[2]]
    table_cn_name_a = df_a_info.at[5,df_a_info.columns[2]]

    #对比文件名是否修改
    #获取6.表设计 表英文名 变动信息

    name_changes = []
    if table_en_name_a != table_en_name_b:
        name_changes.append((f"英文表名表更:{table_en_name_a} --> {table_en_name_b}"))
    if table_cn_name_a != table_cn_name_b:
        name_changes.append((f"中文表名表更:{table_cn_name_a} --> {table_cn_name_b}"))

    #//////////////////////获取变更信息////////////////////////
    #新增一列标识行用于区分数据来源
    compare_list=['columns_1','columns_2','columns_3','columns_4','columns_5','columns_7','columns_13','columns_14','columns_15','columns_16','columns_17','columns_18','columns_19','columns_20','columns_21','columns_22']
    df_a_new['_source'] = 'A'
    df_b_new['_source'] = 'B'


    #去除组号和序号 减少序号变更的干扰
    df_a_new = df_a_new.iloc[:,0:]
    df_b_new = df_b_new.iloc[:,0:]

    #从df_a_new 和df_b_new中去除新增和删除行的变更信息
    df_a_flitered = df_a_new[~df_a_new['columns_2'].isin(deleted_rows['column_en_name'])]
    df_b_flitered = df_b_new[~df_b_new['columns_2'].isin(added_rows['column_en_name'])]

    #print( f"{column_en_name}" for (idx,column_en_name) in df_added_rows.loc[idx, 'column_en_name'])

    merged_df = pd.concat([df_a_flitered, df_b_flitered])

    #根据对比行找出变更的行及其对应的布尔值 类似 0 False 1True
    duplicates = merged_df.duplicated(subset=compare_list,keep=False)

    #去除反选后对应的行得到筛选后的值
    diff_df = merged_df[~duplicates]
    
    #分别在变更前后的数据集中找到对应的数据并裁剪到各自的数据集中(数据应该是一致的 都是一样的行一样的列数量)
    diff_in_a = diff_df[diff_df['_source'] == 'A'].drop(columns=['_source'])
    diff_in_b = diff_df[diff_df['_source'] == 'B'].drop(columns=['_source'])

    #重置数据集索引
    diff_in_a.reset_index(drop=True,inplace=True)
    diff_in_b.reset_index(drop=True,inplace=True)


    
    diff_idx_a = diff_in_a.index.tolist()
    diff_idx_b = diff_in_b.index.tolist()

    #指定对比列
    columns_a = diff_in_b.columns.tolist()
    columns_b = diff_in_b.columns.tolist()

    #print(diff_in_b)
    orginal_index_list = diff_in_b['_original_index']
    compare_columns=['group_no','ser_no']

    #通过对比后的数据所带的_orginal_index 去df_a_new 找到对应的组号和序号
    diff_list = []
    diff_list_info = []
    group_no = ''
    ser_no = ''
  
    #print(diff_in_b.loc[diff_in_b['_original_index'] == 3 ,'group_no'].values[0] )
    for idx in diff_idx_b:
        group_no = diff_in_b.at[idx,'group_no']
        ser_no = diff_in_b.at[idx,'ser_no']
        diff_list_info.append((idx,group_no,ser_no))

        for col in diff_in_b.columns: #todo 去掉原始索引和序号 组号的对比
            #todo 判断列名是 原始索引的时候 continue
            if col in ['group_no','ser_no','_original_index']:
                continue

            old_value = str(diff_in_a.at[idx, col]).replace("\n","") if idx in diff_in_a.index.values    else np.nan
            new_value = str(diff_in_b.at[idx, col]).replace("\n","") if idx in diff_in_b.index.values    else np.nan
            #都为空不输出变更记录
            if pd.isna(old_value) and pd.isna(new_value):
                continue
            #如果旧值为空 指定旧值为原为空
            if old_value != new_value:
                diff_list.append((idx,old_value,new_value))

    output_diff = []
    for info in diff_list_info:
        idx,group_no,ser_no = info
        output_diff.append(f" [{idx + 1 }] 字段组号: {group_no} 字段序号: {ser_no}:")
        num =1
        for diff in diff_list:
            diff_idx,old_value,new_value = diff
            if diff_idx == idx:
                output_diff.append(f"\t<{num}>: {old_value} -> {new_value};")
                num += 1

    output_str = "\n".join(map(str,output_diff))
    #print(output_str)


    diff_result = {
        #"differencesA": [(f"<第{idx + 1}行第{col_str}列>: {old_value}  -->  {new_value}") for (idx,col_str,old_value,new_value) in diff_list_a],
        "differencesB": output_str,
        "add_rows": [(f"<{idx+1}>: 中文字段名：{column_chn_name}      英文字段名:{column_eng_name} " )for (idx,column_chn_name,column_eng_name) in add_row_list],
        "removed_rows":[(f"<{idx+1}>: 英文字段名:{column_eng_name} " )for (idx,column_eng_name) in del_row_list],
        "row_changes":row_changes,
        "name_changes":name_changes if name_changes else ["无变更"],
    }

    return diff_result

#写入文件

def write_to_excel(file_a,file_b,sheet_name,diff_result):
    output_file = "聚合层设计文档对比变更记录.xlsx"
    today_str = date.today().strftime('%Y-%m-%d')

    #读取file_b的信息
    df_a_info = pd.read_excel(file_a,sheet_name=sheet_name)
    df_b_info = pd.read_excel(file_b,sheet_name=sheet_name)

    table_en_name_b = df_b_info.at[4,df_b_info.columns[2]]
    table_cn_name_b = df_b_info.at[5,df_b_info.columns[2]]

    #文件是否存在

    if os.path.exists(output_file):
        writer = pd.ExcelWriter(output_file,engine='openpyxl',mode='a')
        book = writer.book
        if "Sheet1" in book.sheetnames:
            sheet = book["Sheet1"]
        else:
            sheet = book.create_sheet("Sheet1")
    else:
        writer = pd.ExcelWriter(output_file,engine='openpyxl')
        book = writer.book
        sheet = book.create_sheet("Sheet1")

        sheet.append(["文件名","表英文名","表中文名","变更记录","新增行","删除或重命名行","其他变更","对比日期"])

    #删除已有的对比数据
    for row in sheet.iter_rows(min_row=2,max_row=sheet.max_row):
        if row[0].value == file_b and row[7].value == today_str:
            sheet.delete_rows(row[0].row,1)
    #写入数据
    #change_logs= "\n".join(list(output_diff))
    change_logs = diff_result["differencesB"] if diff_result["differencesB"] else "无变更"
    new_rows = "\n".join(diff_result["add_rows"]) if diff_result["add_rows"] else "无变更"
    removed_rows = "\n".join(diff_result["removed_rows"]) if diff_result["removed_rows"] else "无变更"
    other_changes = "\n".join(diff_result["name_changes"])
    

    sheet.append([file_b,table_en_name_b,table_cn_name_b,change_logs,new_rows,removed_rows,other_changes,today_str])

    for col in sheet.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2) if (max_length + 2) < 25 else 25
        sheet.column_dimensions[col_letter].width = adjusted_width
        for cell in col:
            cell.alignment = Alignment(horizontal='left', vertical='center')

    book.save(output_file)
    writer.close()
    return other_changes


if __name__ == "__main__":
    #file_a = 'D:\\sunline_etl_tool\\get_excel_differences\\A.xlsx'
    #print(file_a)
    #file_b = 'D:\\sunline_etl_tool\\get_excel_differences\\B.xlsx'
    #print(file_b)
    #sheet_name = 'Sheet1'
     
    sheet_name = '7.字段设计'
    if len(sys.argv) != 3:
        print("Usage: python get_file_differences.py <original_file> <new_file>")
        sys.exit(1)
    file_a = sys.argv[1]
    file_b = sys.argv[2]
    output_file = "聚合层设计文档对比变更记录.xlsx"
    print (f"基准文档 ：{file_a}")
    print (f"最新文档 ：{file_b}") 

    diff_result = compare_sheets(file_a,file_b,sheet_name)
    write_to_excel(file_a,file_b,"6.表设计",diff_result)

    print("\n-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=-=-=- 以下是文件内容变更明细 -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n")
    if diff_result["differencesB"]:
        print("1--内容变更：\n")
        print(diff_result['differencesB'])

    print(diff_result['row_changes'][0])

    if diff_result["add_rows"]:
        print(" 2.1--新增字段变更：\n")
        for idx, item in enumerate(diff_result['add_rows'],1):
            print(f"\t{item}")
    if diff_result["removed_rows"]:
        print(" 2.2--删除或变更字段：\n")
        for idx, item in enumerate(diff_result['removed_rows'],1):
            print(f"\t{item}")
    if diff_result["name_changes"]:
        print("3--表名变更：\n")
        for idx, item in enumerate(diff_result['name_changes'],1):
            print(f"\t3.{idx}、{item}")
    print(f"\n-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=- 同步记录保存到文件{output_file}中 -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-\n")