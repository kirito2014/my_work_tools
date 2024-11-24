# -*- coding:utf-8 -*-
#脚本文件位置
import os
import sys
import re,sys
import openpyxl
import pandas as pd

ETL_HOME = os.path.dirname(os.path.abspath(__file__))

def get_belong_theme(file_name):
    file_name_upper = file_name.upper()
    #print(file_name_upper[4:7])
    if file_name_upper[0:4] == "AGL_":
        return "AGL"
    else:
        return "Unknown"
#函数用于提取SQL文件中的文件名
def extract_table_name(sql_content):
    pattern = r'(?:FROM|JOIN)\s+(\w+\.\w+)\s+'
    matches = re.findall(pattern, sql_content.upper())
    #print(matches)
    return matches

#提取开发人员名称（15行），脚本中文名名称（9行）
#提取开发人员名称（15行），脚本中文名名称（9行）
def extract_dev_ops(file_path):
    dev_ops = ''
    tab_cn_name = ''
    get_file_info = []
    #print(file_path)

    if file_path.lower().endswith("tg_pc.hql"):
        try:
            with open(file_path, 'r' ,encoding='utf-8') as file:
                lines = file.readlines()
                if len(lines) >= 19:
                    line_11 = lines[10]
                    if '：' in line_11:
                        #print(line_9.split('：',1)[1].strip())
                        tab_cn_name = line_11.split('：',1)[1].strip()
                        #print(tab_cn_name)
                        get_file_info.append(tab_cn_name)
                    line_17 = lines[16]
                    #print(line_15)
                    if '：' in line_17:
                        #print(line_15.split('：',1)[1].strip())
                        dev_ops = line_17.split('：',1)[1].strip()
                        get_file_info.append(dev_ops)
                return get_file_info
        except Exception as e:
            print(f"处理{file_path}获取开发信息失败")
            tab_cn_name = "表名未获取"
            get_file_info.append(tab_cn_name)
            dev_ops = "开发人员未获取"
            get_file_info.append(dev_ops)
            return get_file_info
        finally:
            tab_cn_name = "表名未获取"
            get_file_info.append(tab_cn_name)
            dev_ops = "开发人员未获取"
            get_file_info.append(dev_ops)
            return get_file_info
    elif file_path.lower().endswith("ta_pc.hql") or file_path.lower().endswith("tf_pc.hql"):
        try:
            with open(file_path, 'r' ,encoding='utf-8') as file:
                lines = file.readlines()
                if len(lines) >= 19:
                    line_9 = lines[8]
                    if '：' in line_9:
                        #print(line_9.split('：',1)[1].strip())
                        tab_cn_name = line_9.split('：',1)[1].strip()
                        #print(tab_cn_name)
                        get_file_info.append(tab_cn_name)
                    line_15 = lines[14]
                    #print(line_15)
                    if '：' in line_15:
                        #print(line_15.split('：',1)[1].strip())
                        dev_ops = line_15.split('：',1)[1].strip()
                        get_file_info.append(dev_ops)
                return get_file_info
        except Exception as e:
            print(f"处理{file_path}获取开发信息失败")
            tab_cn_name = "表名未获取"
            get_file_info.append(tab_cn_name)
            dev_ops = "开发人员未获取"
            get_file_info.append(dev_ops)
            return get_file_info
        finally:
            tab_cn_name = "表名未获取"
            get_file_info.append(tab_cn_name)
            dev_ops = "开发人员未获取"
            get_file_info.append(dev_ops)
            return get_file_info
    #非聚合层的情况
    else:
        tab_cn_name = "表名未获取"
        get_file_info.append(tab_cn_name)
        dev_ops = "开发人员未获取"
        get_file_info.append(dev_ops)
        return get_file_info
    return []

def filter_table_names(file_name, table_names):
    filterd_table_names = []
    for table_name in table_names:
        if file_name.upper().replace('_PC.HQL','').replace('ETL_PROC_','') not in table_name.upper() and not table_name.upper().startswith("AGL"):
            filterd_table_names.append(table_name.upper().replace(' ',''))
    filterd_table_names = list(set(filterd_table_names))
    return filterd_table_names

def process_folder(folder_path):
    data = []
    
    total_files = len(os.listdir(folder_path))
    processed_files = 0

    for file_name in os.listdir(folder_path):
        #print(file_name)
        if file_name.endswith(".hql"):
            file_path = os.path.join(folder_path,file_name)
            with open(file_path, 'r',encoding='utf-8') as file:
                sql_content = file.read()
                sql_content = sql_content.replace('${version_num}','')
                table_names = extract_table_name(sql_content)
                filterd_table_names = filter_table_names(file_name,table_names)
                belong_theme = get_belong_theme(file_name)
                #print(extract_dev_ops(file_path))
                if file_name.endswith("tg_pc.hql"):
                    dev_ops  = extract_dev_ops(file_path)[1]
                    tab_cn_name  = extract_dev_ops(file_path)[0]
                else:
                    dev_ops  = extract_dev_ops(file_path)[1]
                    tab_cn_name  = extract_dev_ops(file_path)[0]
                #处理TG表的特殊模板的情况
                #print(tab_cn_name)
                #print(f"{file_name.upper().replace('_PC.HQL','').replace('ETL_PROC_','')} 开发人员:{dev_ops}")
                for table_name in filterd_table_names:
                    source_schema, source_table = table_name.split('.')
                    source_table += '_PC'
                    data.append((file_name,belong_theme,table_name,source_schema,source_table,dev_ops,tab_cn_name))
            processed_files += 1
            progress_bar(processed_files,total_files,20,'1')
    print()
    return data

def write_to_excel(data ,output_file):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet['A1'] = '主题领域'
    sheet['B1'] = '聚合表名'
    sheet['C1'] = '中文表名'
    sheet['D1'] = '开发人员'
    sheet['E1'] = 'ODS表名'
    sheet['F1'] = '来源库名'
    sheet['G1'] = '来源表名'

    for idx,(file_name,belong_theme,table_name,source_schema,source_table,dev_ops,tab_cn_name) in enumerate(data, start=2):
        sheet.cell(row=idx, column=1, value=belong_theme)
        sheet.cell(row=idx, column=2, value=file_name.upper().replace('_PC.HQL','').replace('ETL_PROC_',''))
        sheet.cell(row=idx, column=3, value=tab_cn_name)
        sheet.cell(row=idx, column=4, value=dev_ops)
        sheet.cell(row=idx, column=5, value=table_name)
        sheet.cell(row=idx, column=6, value=source_schema)
        sheet.cell(row=idx, column=7, value=source_table.replace('_PC',''))

    sheet.column_dimensions['A'].width = 10 
    sheet.column_dimensions['B'].width = 40  
    sheet.column_dimensions['C'].width = 40     
    sheet.column_dimensions['D'].width = 8 
    sheet.column_dimensions['E'].width = 40
    sheet.column_dimensions['F'].width = 10
    sheet.column_dimensions['G'].width = 40
 
    workbook.save(output_file)

def write_to_excel1(data ,output_file ):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet['A1'] = 'belong_theme'
    sheet['B1'] = 'file_name'
    sheet['C1'] = 'dev_ops'
    sheet['D1'] = 'table_name'
    sheet['E1'] = 'source_schema'
    sheet['F1'] = 'source_table'
    sheet['G1'] = 'etl_job_no'
    sheet['H1'] = 'etl_job_name'


    total_files = len(data)
    processed_files = 0
    for idx,(file_name,belong_theme,table_name,source_schema,source_table,dev_ops,tab_cn_name) in enumerate(data, start=2):
        sheet.cell(row=idx, column=1, value=belong_theme)
        sheet.cell(row=idx, column=2, value=file_name.upper().replace('_PC.HQL','').replace('ETL_PROC_',''))
        sheet.cell(row=idx, column=3, value=dev_ops)
        sheet.cell(row=idx, column=4, value=table_name)
        sheet.cell(row=idx, column=5, value=source_schema)
        sheet.cell(row=idx, column=6, value=source_table)

        processed_files += 1
        progress_bar(processed_files,total_files,20,'2')
    print()
    sheet.column_dimensions['A'].width = 10 
    sheet.column_dimensions['B'].width = 30     
    sheet.column_dimensions['C'].width = 40 
    sheet.column_dimensions['D'].width = 20 
    sheet.column_dimensions['E'].width = 40
 
    workbook.save(output_file)



def match_job_numbers1(target_file,output_file):
    df_target = pd.read_excel(target_file, sheet_name = None ,dtype={'etl_job': str,'etl_system': str})
    df_output = pd.read_excel(output_file)

    dev_sheet = df_target['dev']
    perf_sheet = df_target['pref']
    prod_sheet = df_target['prod']
    

    merged_df = pd.concat([dev_sheet,perf_sheet,prod_sheet], keys=['dev','pref','prod']).reset_index(level=0)
    merged_df.rename(columns={'level_0':'source_sheet'},inplace=True)

    merged_output = pd.merge(df_output, merged_df,how='left',left_on='source_table',right_on='etl_job')

    merged_output['etl_job_no'] = merged_output['etl_system']
    merged_output.loc[merged_output['etl_job_no'].isnull(),'etl_job_no'] = '没有对应的作业编号'
    merged_output['etl_job_name'] = 'IMP:' + merged_output['etl_system'] + '_' + merged_output['etl_job']
    merged_output.to_excel(output_file,index=False)

def progress_bar(current,total,bar_length,process_type):
    get_process_type = process_type
    progress=current / total
    block = int(bar_length * progress)
    percentage = progress * 100
    if get_process_type == '1':
        text = f"\r---------------------- 处理文件中: [{'#' * block}{'-' * (bar_length - block)}] 【{percentage:.2f}%】 ----------------------"
    else:
        text = f"\r---------------------- 写入文件中: [{'#' * block}{'-' * (bar_length - block)}] 【{percentage:.2f}%】 ----------------------"
    sys.stdout.write(text)
    sys.stdout.flush()

def main(folder_path,output_file):
    data = process_folder(folder_path)
    print("---------------------- 将结果写入文件... ----------------------")
    write_to_excel(data ,output_file )
def main1(folder_path,output_file,target_file):
    data = process_folder(folder_path)
    print("---------------------- 将结果写入文件... ----------------------")
    write_to_excel1(data ,output_file )
    match_job_numbers1(target_file,output_file)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        print("---------------------- 未传入依赖清单，只生成来源表列表 ----------------------")
        folder_path = sys.argv[1]
        output_file = "table_rely_output_list.xlsx"
        #print (folder_path)
        main(folder_path,output_file)
        print (f"---------------------- 文件保存在:{output_file} ----------------------")
    elif len(sys.argv) == 3:
        print("---------------------- 检测到传入依赖清单，生成来源表列表及依赖信息 ----------------------")
        folder_path = sys.argv[1]
        output_file = "table_rely_output.xlsx"
        target_file = sys.argv[2]
        main1(folder_path,output_file,target_file)
        #print (folder_path)
        print (f"---------------------- 文件保存在:{output_file} ----------------------")
    else:
        print("用法: python get_rely_table_new.py <scrpits_folder_path> [rely_table_list.xlsx] ")