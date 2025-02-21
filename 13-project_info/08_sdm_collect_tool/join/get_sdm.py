#合并SDM,根据已有的SDM文档，将标记为Y的SDM对应的映射文档 数据字典和代码映射（若有）复制到模板中，如果已经存在对应的SDM则先删除再复制
import sys
import os 
import pandas as pd
import xlwings as xw


def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False

def check_vaild_excel(sheet):
    """检查最大行是否一致如果一致则返回True，不一致则返回False"""
    max_row_a=sheet.range('A1').expand('down').last_cell.row
    max_row_i=sheet.range('I1').expand('down').last_cell.row
    if max_row_a and max_row_i and max_row_i != max_row_a:
        print(f"[ ERROR ] {sheet.name} 首列非空校验不通过.")
        return False
    elif max_row_a and max_row_i and max_row_i == max_row_a:
        print(f"[ INFO ] {sheet.name} 首列非空校验通过.")
        if sheet.name == 'rem-代码映射':
            if sheet.range('A1').value != 'SRC_TAB_LIB_NAME' or sheet.range('A1').value == 'None':
                print(f"[ ERROR ] {sheet.name} 首列内容校验不通过，确认是否错行缺失.")
                return False
        return True
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
        else:
            tgt_wb = excel_app.Book()
            tgt_wb.save(target_file)
        
        #检查原文档中数据字典页并去除筛选
        rem_data_dict_sheet = 'rem-数据字典'
        if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
            error_list.append(f"源SDM文件未找到数据字典sheet页")
            src_wb.close()
            tgt_wb.close()
            sys.exit()
        src_dd_sheet = src_wb.sheets(rem_data_dict_sheet)
        if not check_vaild_excel(src_dd_sheet):
            print("数据字典 首列校验失败，存在空格，请检查！")
            src_wb.close()
            tgt_wb.close()
            sys.exit()
        clear_filters(src_dd_sheet)
        
        #检查原文档中代码映射页并去除筛选
        rem_code_map_sheet = 'rem-代码映射'
        if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
            error_list.append(f"处理源SDM文件未找到代码映射sheet页")
            src_wb.close()
            tgt_wb.close()
            sys.exit()
        src_cm_sheet = src_wb.sheets(rem_code_map_sheet)
        if not check_vaild_excel(src_cm_sheet):
            print("代码映射 首列校验失败，存在空格，请检查！")
            src_wb.close()
            tgt_wb.close()
            sys.exit()
        clear_filters(src_cm_sheet)

        #处理index页符合条件的数据

        index_data = pd.read_excel(source_file, sheet_name = 'index', usecols="C,D,M")
        index_data = index_data.iloc[1:] #从第2行开始
        index_data.columns = ['table_id','table_name','enable_flag'] 
        index_data['table_id'] = index_data['table_id'].apply(lambda x:x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']
        y_count = filtered_index.shape[0]
        total_files = y_count
        print(f"-==========- 本次将要处理 {total_files} 个SDM -==========-")
        processed_files = 0

        table_list = [f"{row['table_name']} \n" for i,row in filtered_index.iterrows()]

        for _,row in filtered_index.iterrows():

            table_name = row['table_name']
            table_id = row['table_id']
            table_name_list.append(table_name)

            #检查源文件中是否存在对应的sheet页,有未更新目录的情况存在
            if table_id not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"{table_name}不在要处理的源文件中，检查index目录是否为最新版本.")
           
            #处理目标文件中的sheet页
            if table_id in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets[table_id].delete()

            #复制sheet内容
            src_sheet = src_wb.sheets[table_id]
            src_sheet.copy(after=tgt_wb.sheets[-1])
            tgt_wb.sheets[-1].name = table_id

            #复制数据字典
            if rem_data_dict_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets.add().Name = rem_data_dict_sheet
            tgt_dd_sheet = tgt_wb.sheets(rem_data_dict_sheet)
            clear_filters(tgt_dd_sheet)
            #先删除目标文件中已存在的数据字典
            tgt_dd_data = tgt_dd_sheet.range('A1').expand('table').value
           
            if tgt_dd_sheet.range('A2').value is None and tgt_dd_sheet.range('A1').value is None:
                print("[ ERROR ] 数据字典为空，请检查rem-数据字典 sheet页是否为全空sheet页!.")
                excel_app.quit()
                sys.exit()
            elif tgt_dd_sheet.range('A2').value is None and tgt_dd_sheet.range('A1').value == "层级":
                #只有表头的情况
                print("[ INFO ] 目标文件数据字典无数据，插入数据...")
            else:
                
                tgt_dd_df = pd.DataFrame(tgt_dd_data[1:],columns=tgt_dd_data[0])
                print(f"[ INFO ] 向目标文件数据字典插入表 {table_name} 的数据字典数据...")
                tgt_dd_df = tgt_dd_df[tgt_dd_df['表英文名'] != table_name]   
                tgt_dd_sheet.clear_contents()
                tgt_dd_sheet.range('A1').value = [tgt_dd_data[0]] + tgt_dd_df.values.tolist()

            data_dict_found = False
            for row in src_dd_sheet.range('A2').expand('table').value:
                if row[1] == table_name:
                    data_dict_found = True
                    tgt_dd_sheet.range('A' + str(tgt_dd_sheet.range('A' + str(tgt_dd_sheet.cells.last_cell.row)).end('up').row +1 )).value = row
            if not data_dict_found:
                error_list.append(f"{table_name} 表的数据字典未找到")

            #复制代码映射，由于码值特殊性需要对码值列设置为文本格式
        
            if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets.add().Name = rem_code_map_sheet
            tgt_cm_sheet = tgt_wb.sheets(rem_code_map_sheet)
            clear_filters(tgt_cm_sheet)
            #先删除目标文件中已存在的代码映射
            tgt_cm_data = tgt_cm_sheet.range('A1').expand('table').value
            if tgt_cm_data:
                tgt_cm_df = pd.DataFrame(tgt_cm_data[1:],columns=tgt_cm_data[1])
                tgt_cm_df = tgt_cm_df[tgt_cm_df['目标表英文名'] != table_name]
                tgt_cm_sheet.clear_contents()
                tgt_cm_sheet.range('A1').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()

            src_cm_data = src_cm_sheet.range('A1').expand('table').value
            if src_cm_data:
                src_cm_df = pd.DataFrame(src_cm_data[1:],columns=src_cm_data[1])
                filtered_src_cm_df = src_cm_df[src_cm_df['目标表英文名'] == table_name]
                
                if filtered_src_cm_df.empty:
                    error_list.append(f"{table_name} 表的代码映射未找到")
                else:
                    start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                    tgt_cm_sheet.range(f'A{start_row}').value = filtered_src_cm_df.values.tolist()

            processed_files += 1
            print(f"-==========- 表 {table_name} 处理完成,剩余 < {total_files-processed_files} > 个,共< {total_files} >个-=========-")
        tgt_wb.save(target_file)
    except Exception as e:
        error_list.append(str(e))
    finally:
        tgt_wb.save(target_file)
        tgt_wb.close()
        excel_app.quit()

    return error_list,table_name_list,table_list




if __name__ == "__main__":

    if len(sys.argv) != 3:
            print("Usage: python get_sdm.py <要合并的文件> <目标文件>")
            sys.exit(1)
    source_file = sys.argv[1]   
    target_file = sys.argv[2]   
    errors,table_name_list,table_list = copy_sheets_and_metadata(source_file,target_file)
    print(f"#### 本次共复制<{len(table_name_list)}>个sdm信息 #### \n| {'| '.join(table_list)} " )
    if errors:
        print(f"#### 提示信息 ####:\n")
        for error in errors:
            print(f"|{error}")