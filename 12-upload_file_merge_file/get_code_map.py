#台并SDM，根已有的SDM文档，将标记为Y的SDM对应的映射文档
import sys 
import os 
import pandas as pd 
import xlwings as xw 

def process_code_mapping(table_list_file): 
    excel_app = xw.App(visible=False) 
    person_name=os.path.splitext(os.path.basename(table_list_file))[0].split('-')[-1] 

    error_log=f"error_log_{person_name}.txt" 
    #output目录拼接
    target_file=os.path.join('output','outpub_cd_map.xlsx')
    
    if os.path.exists(target_file):
        tgt_wb=xw.Book(target_file) 
    else: 
        tgt_wb=xw.Book()
        tgt_wb.save(target_file) 
    
    with open(table_list_file,'r',encoding='utf-8') as file: 
        table_names = [line.strip().upper() for line in file.readlines()]
        table_list=len(table_names) 
    print(f"本次共处理{table_list}张表") 
    processed_table_count=0 
    
    for table_name in table_names: 
        print(f"正在处理<{person_name}>-<{table_name}>的码值映射。") 
        code_map_files = [ 
            os.path.join('uploads',f'pub_cd_map-{person_name}.xlsx'), 
            os.path.join('uploads',f'pub_cd_map-{person_name}.xls'), 
            os.path.join('uploads',f'pub_cd_map-{person_name}.xlsm') 
            ]
        code_map_file = next((file for file in code_map_files if os.path.exists(file)),None)
        print(code_map_files)
        if not code_map_file: 
            log_error(error_log,f"{person_name}的代码映射文件不存在.") 
            sys.exit()         
        try: 
            src_wb=xw.Book(code_map_file)
            rem_code_map_sheet='rem-代码映射' 
            if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]: 
                log_error(error_log,f"{code_map_file}中未找到代码映射sheet。") 
                src_wb.close() 
                sys.exit()

            src_cm_sheet=src_wb.sheets[rem_code_map_sheet] 
            if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]: 
                tgt_wb.sheets.add(rem_code_map_sheet) 

            tgt_cm_sheet=tgt_wb.sheets(rem_code_map_sheet) 
            #先删除目标文件中已存在的码值映射
            tgt_cm_data=tgt_cm_sheet.range('A1').expand('table').value 
            if tgt_cm_data: 
                tgt_cm_df=pd.DataFrame(tgt_cm_data[1:],columns=tgt_cm_data[1]) 
                tgt_cm_df=tgt_cm_df[tgt_cm_df['目标表英文名'] != table_name] 
                tgt_cm_sheet.clear_contents()
                tgt_cm_sheet.range('A1').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()
            #读取并筛选源文件中的码值映射数据
            src_cm_data=src_cm_sheet.range('A1').expand('table').value 
            if src_cm_data: 
                src_cm_df =pd.DataFrame(src_cm_data[1:],columns=src_cm_data[1]) 
                filtered_src_cm_df=src_cm_df[src_cm_df['目标表英文名']==table_name] 
                if filtered_src_cm_df.empty: 
                    log_error(error_log,f"{code_map_file}中未找到{table_name}表的代码映射.")
                else:
                    start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                    tgt_cm_sheet.range(f'A{start_row}').value = filtered_src_cm_df.values.tolist()
            tgt_wb.save()
        except Exception as e:
            log_error(error_log,f"处理{code_map_file}中{table_name} 表的代码映射时发生错误:{str(e)}.")
        finally:
            src_wb.close()
            tgt_wb.save()
            tgt_wb.close()
        processed_table_count = processed_table_count + 1
        print(f"剩余{table_list-processed_table_count}个")
    tgt_wb.save(target_file)
    tgt_wb.close()
def log_error(log_file,message):
    with open(log_file,'w',encoding='utf-8') as log:
        log.write(message + '\n')
    print(message)
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('11111')
        sys.exit(1)
    table_list_file = sys.argv[1]
    process_code_mapping(os.path.join('uploads',table_list_file))
            
            