from flask import Flask, render_template, send_file,request, send_from_directory, jsonify
import os,sys
from werkzeug.utils import secure_filename
import threading
from time import sleep
import openpyxl
import pandas as pd 
import xlwings as xw 

txt_path  = 'table_list-test4.txt'
xlsx_path = 'pub_cd_map-test4.xlsx'
output_path = './output/pub_cd_map.xlsx'

def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False

def run_script(txt_path, xlsx_path, output_path): 

    #print(txt_path, xlsx_path, output_path)
    #sys.exit()
    global progress
    excel_app = xw.App(visible=False) 
    person_name=os.path.splitext(os.path.basename(txt_path))[0].split('-')[-1] 

    error_log=f"error_log_{person_name}.txt" 
    #output目录拼接
    #print(txt_path)
    table_list_file=txt_path
    #print(table_list_file)
    target_file=output_path
    #print(target_file)
    
    if os.path.exists(target_file):
        tgt_wb=xw.Book(target_file) 
    else: 
        tgt_wb=xw.Book()
        tgt_wb.save(target_file) 

    #update_progress(0)  # 初始进度

    with open(table_list_file,'r',encoding='utf-8') as file: 
        table_names = [line.strip().upper() for line in file.readlines()]
        table_list=len(table_names) 
    print(f"本次共处理{table_list}张表") 
    processed_table_count=0 

    #update_progress(30)  # 进度 30%

    for table_name in table_names: 
        print(f"正在处理<{person_name}>-<{table_name}>的码值映射。") 
        # code_map_files = [ 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xlsx'), 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xls'), 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xlsm') 
        #     ]
        
        #code_map_file = next((file for file in code_map_files if os.path.exists(file)),None)
        code_map_file = xlsx_path
        #print(code_map_files)
        if not code_map_file: 
            log_error(error_log,f"{person_name}的代码映射文件不存在.") 
            sys.exit()         
        #try: 
        src_wb=xw.Book(code_map_file)
        rem_code_map_sheet='rem-代码映射' 
        
        if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]: 
            log_error(error_log,f"{code_map_file}中未找到代码映射sheet。") 
            src_wb.close() 
            sys.exit()

        src_cm_sheet=src_wb.sheets[rem_code_map_sheet] 
        clear_filters(src_cm_sheet)
        if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]: 
            tgt_wb.sheets.add(rem_code_map_sheet) 

        tgt_cm_sheet=tgt_wb.sheets(rem_code_map_sheet)
        clear_filters(tgt_cm_sheet)
        #先删除目标文件中已存在的码值映射
        tgt_cm_data=tgt_cm_sheet.range('A1').expand('table').value 
        if tgt_cm_data: 
            tgt_cm_df=pd.DataFrame(tgt_cm_data[1:],columns=tgt_cm_data[1]) 
            tgt_cm_df=tgt_cm_df[tgt_cm_df['目标表英文名','目标代码码值'] != table_name] 
            tgt_cm_sheet.clear_contents()
            tgt_cm_sheet.range('A1').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()
        #读取并筛选源文件中的码值映射数据
        src_cm_data=src_cm_sheet.range('A1').expand('table').value 
        if src_cm_data: 
            # 获取表头
            headers = src_cm_data[1]
            # 将数据转换为DataFrame，但保持原始格式
            src_cm_df = pd.DataFrame(src_cm_data[1:], columns=headers)
            # 找到需要保持文本格式的列（通常是码值相关的列）
            code_columns = ['源代码码值']  # 根据实际列名调整
            
            # 对这些列进行特殊处理，确保保持文本格式
            for col in code_columns:
                if col in src_cm_df.columns:
                    src_cm_df[col] = src_cm_df[col].astype(str).apply(
                        lambda x: f"'{x}" if x.strip() and x.strip()[0] == '0' else x
                    )
            #print(src_cm_df)
            filtered_src_cm_df = src_cm_df[src_cm_df['目标表英文名']==table_name]
            #print(filtered_src_cm_df)
            
            if filtered_src_cm_df.empty: 
                log_error(error_log,f"{code_map_file}中未找到{table_name}表的代码映射.")
            else:
                start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                # 写入数据并设置格式
                target_range = tgt_cm_sheet.range(f'A{start_row}')
                target_range.value = filtered_src_cm_df.values.tolist()
                
                #对包含代码的列设置文本格式
                for col in code_columns:
                    if col in headers:
                        col_index = headers.index(col)
                        code_range = target_range.offset(0, col_index).resize(len(filtered_src_cm_df))
                        code_range.number_format = '@'  # 设置为文本格式

        tgt_wb.save()
        #except Exception as e:
        #    log_error(error_log,f"处理{code_map_file}中{table_name} 表的代码映射时发生错误:{str(e)}.")
        #finally:
        src_wb.close()
        #tgt_wb.close()
        processed_table_count = processed_table_count + 1
        progress = int((processed_table_count / len(table_names)) * 100)
        print(f"剩余{table_list-processed_table_count}个")

    #update_progress(70)  # 进度 70%

    tgt_wb.save(target_file)
    tgt_wb.close()

    #update_progress(100)  # 完成

    #print('110000101010')

def log_error(log_file, message):
    with open(log_file, 'a', encoding='utf-8') as log:  # 改为 'a' 模式
        log.write(message + '\n')
    print(message)


if __name__ == '__main__':
    run_script(txt_path,xlsx_path,output_path)
    #run_script()