#合并SDM,根据已有的SDM文档，将标记为Y的SDM对应的映射文档 数据字典和代码映射（若有）复制到模板中，如果已经存在对应的SDM则先删除再复制
import sys
import os 
import pandas as pd
import xlwings as xw
import shutil

def copy_excel_file(target_file,person_name):
    if not os.path.isfile(target_file):
        print(f"[ ERROR ] 模板文件不存在！！")
        return
    new_file_name = f"聚合层检查脚本生成-{person_name}.xlsx"
    if os.path.isfile(new_file_name):
        print(f"[ INFO ] 目标文件< {new_file_name} >已存在,删除旧文件")
        os.remove(new_file_name)
    shutil.copyfile(target_file,new_file_name)
    print(f"[ INFO ] 目标文件< {new_file_name} >已创建")
    return new_file_name

def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False

def copy_sheets_and_metadata(source_file,target_file):
    
    #初始化错误列表
    error_list = []
    table_name_list = []
    table_list = []
    #应用显示用于调试
    excel_app = xw.App(visible=False)
    try:
        print(source_file)
        src_wb = excel_app.books.open(source_file) 
        if os.path.exists(target_file):
            tgt_wb = excel_app.books.open(target_file)
            tgt_sheet = tgt_wb.sheets['Sheet1']
        else:
            tgt_wb = xw.Book()
            #tgt_wb.sheets.add('检查主键和数据量')
            tgt_sheet = tgt_wb.sheets['Sheet1']
            tgt_wb.save(target_file)

        #删除目标文件已存在的数据
        if tgt_sheet.range('A1').value is None:
            tgt_sheet.range('A1').value = ['表英文名','表中文名','主键列表','算法模板','数据查询语句','主键查询语句','主键非空查询语句']
        else:
            tgt_cm_data = tgt_sheet.range('A2').expand('down').value
            if tgt_cm_data:
                tgt_sheet.range('A2').expand('down').clear_contents()

        index_data = pd.read_excel(source_file, sheet_name = 'index', usecols="C,D,E,M,N")
        index_data = index_data.iloc[1:] #从第2行开始
        #print(index_data)
        index_data.columns = ['table_id','table_name','table_ch_name','enable_flag','template_flag'] 
        index_data['table_id'] = index_data['table_id'].apply(lambda x:x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']

        y_count = filtered_index.shape[0]
        total_files = y_count
        print(f"-==========- 检测到 {total_files} 张表 -==========-")
        processed_files = 0

        table_list = [f"{row['table_name']} \n" for i,row in filtered_index.iterrows()]

        data_count_query = ''
        pk_query = ''
        pk_null_query = ''

        for _,row in filtered_index.iterrows():

            table_name = row['table_name']
            table_id = row['table_id']
            table_ch_name = row['table_ch_name']
            template_flag = row['template_flag']

            table_name_list.append(table_name)

            #print(template_flag)
            #检查数据字典，是否存在多个同一张表的数据字典

            rem_data_dict_sheet = 'rem-数据字典'
            if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"[ ERROR ] 源SDM文件未找到数据字典sheet页")
            else:
                src_dd_sheet = src_wb.sheets(rem_data_dict_sheet)
                clear_filters(src_dd_sheet)
                #判断目标文件
                tgt_sheet.range('A1').value = ['表英文名','表中文名','主键列表','算法模板','数据查询语句','主键查询语句','主键非空查询语句']
                #tgt_sheet = tgt_wb.sheets['Sheet1']
            
                #找到对应的数据字典
                src_data = src_dd_sheet.range('A1').expand('table').options(pd.DataFrame,header =1).value
                #去除列名中的空格
                src_data.columns = src_data.columns.str.strip()
                table_data = src_data[src_data['表英文名'] == table_name]

                #检查数据字典是否为空
                if table_data.empty:
                    error_list.append(f"[ ERROR ] 源SDM文件未找到<{table_name}>对应的数据字典")
                    #continue
                
                #检查数据字典是否对应多个
                if table_data['字段序号'].duplicated().any():
                    error_list.append(f"[ ERROR ] 数据字典中找到<{table_name}>对应重复数据字典")
                    #continue
                
                #处理主键信息
                pk_list = ','.join(table_data[table_data['是否主键'] == 'Y']['字段英文名'])
                #拼接非空信息
                pk_null_list =f',\'\') AS STRING),CAST(COALESCE('.join(table_data[table_data['是否主键'] == 'Y']['字段英文名'])
                #print(f"CAST(COALESCE({pk_null_list},\'\') AS STRING)")
                pk_null_list_1 =f"CAST(COALESCE({pk_null_list},\'\') AS STRING)"
                #根据标识选择主键查询类型
                #主键为空的情况,判断算法模板并拼接实际的查询语句

                if not pk_list:
                    error_list.append(f"[ WARNING ]数据字典中<{table_name}>对应的主键为空")
                    if template_flag == 'AGL-PKA' or template_flag == 'AGL-EVP':
                        data_count_query = f"select '{table_name}',count(1) from agl.{table_name} where pt_dt = '${{process_date}}' and del_f = '0' union all"
                        pk_query = "--无主键，检查语句为空"
                    else:
                        data_count_query = f"select '{table_name}',count(1) from agl.{table_name} where pt_dt = '${{process_date}}' union all"
                        pk_query = "--无主键，检查语句为空"
                elif pk_list:
                    if template_flag == 'AGL-PKA' or template_flag == 'AGL-EVP':
                        data_count_query = f"select '{table_name}',count(1) from agl.{table_name} where pt_dt = '${{process_date}}' and del_f = '0' union all"
                        pk_query  = f"select '{table_name}',count(1) from (select {pk_list} ,count(1) from agl.{table_name} where pt_dt = '${{process_date}}' and del_f = '0' group by {pk_list} having count(1) >1 ) union all"
                        pk_null_query  = f"select '{table_name}' ,count(1) from agl.{table_name} where pt_dt = '${{process_date}}' and del_f = '0'   AND CONCAT ({pk_null_list_1}) = ''  union all"

                    else:
                        data_count_query = f"select '{table_name}',count(1) from agl.{table_name} where pt_dt = '${{process_date}}' union all"
                        pk_query  = f"select '{table_name}',count(1) from (select {pk_list} ,count(1) from agl.{table_name} where pt_dt = '${{process_date}}' group by {pk_list} having count(1) >1 ) union all"
                        pk_null_query  = f"select '{table_name}' ,count(1) from agl.{table_name} where pt_dt = '${{process_date}}'   AND CONCAT ({pk_null_list_1}) = ''  union all"
                
                #插入目标表
                last_row = tgt_sheet.range('A1').expand('down').last_cell.row
                tgt_sheet.range(f'A{last_row + 1}').value=[table_name,table_ch_name,pk_list,template_flag,data_count_query,pk_query,pk_null_query]

            processed_files += 1
            print(f"[  INFO  ] 表 {table_name} 处理完成,剩余 < {total_files-processed_files} > 个,共< {total_files} >个")
            tgt_wb.save(target_file)
        tgt_wb.save(target_file)
        tgt_wb.close()
        #src_wb.close()
    except Exception as e:
        error_list.append(str(e))
    finally:
        tgt_wb.close()
        excel_app.quit()

    return error_list,table_name_list,table_list




if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python gen_chk_sql.py <source_file> <target_file> <person_name>")
        sys.exit(1)
    source_file = sys.argv[1]
    target_file = sys.argv[2]
    person_name = sys.argv[3] 
    new_file_name = copy_excel_file(target_file,person_name)
    errors,table_name_list,table_list = copy_sheets_and_metadata(source_file,new_file_name)
    print(f"#### 本次共复制<{len(table_name_list)}>个sdm信息 #### \n| {'| '.join(table_list)} " )
    if errors:
        print(f"#### 提示信息 ####:\n")
        for error in errors:
            print(f"|{error}")