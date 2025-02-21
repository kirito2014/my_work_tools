#合并SDM,根据已有的SDM文档，将标记为Y的SDM对应的映射文档 数据字典和代码映射（若有）复制到模板中，如果已经存在对应的SDM则先删除再复制
import sys
from datetime import datetime
import os,re
import pandas as pd
import xlwings as xw
import shutil
from openpyxl import load_workbook
from tqdm import tqdm

def check_and_remove_file(new_file_name:str,output_directory:str):
    """检查文件夹下是否存在该文件，有则删除"""
    #拼接文件名
    new_file_name = os.path.join(output_directory,new_file_name)
    if os.path.exists(new_file_name):
        os.remove(new_file_name)

def create_new_file(template_file:str,new_file_name:str):
    new_file_name = os.path.join(output_directory,new_file_name)
    shutil.copy(template_file,new_file_name)

def get_output_dirname(new_file_name:str):
    """获取文件归属条线"""
    output_directory = new_file_name.split('_')[0]
    return output_directory
def check_file_folder_exists(output_directory:str):
    if not os.path.exists(output_directory):
        print(f"[ INFO ] {output_directory} 文件夹不存在，创建文件夹.")
        os.makedirs(output_directory)
    else:
        print(f"[ INFO ] {output_directory} 文件夹已存在，继续操作.")

def filter_index_data(source_file:str,table_cn_name:str):
    df = pd.read_excel(source_file,sheet_name="index",header=1)

    filltered_df = df[df["表中文名称"] == table_cn_name.strip()]
    return filltered_df

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

    

def write_formula_to_excel(file_name:str):
    wb = load_workbook(file_name)
    ws = wb["index"]
    ws["S3"] = f'=HYPERLINK("#\'rem-数据字典\'!"&ADDRESS(MATCH(index!D3,\'rem-数据字典\'!B:B,0),COLUMN(\'rem-数据字典\'!B:B)),">>>数据字典<<<")'
    ws["T3"] = f'=IFNA(HYPERLINK("#\'rem-代码映射\'!"&ADDRESS(MATCH(index!D3,\'rem-代码映射\'!I:I,0),COLUMN(\'rem-代码映射\'!I:I)),">>>代码映射<<<"),"")'

    wb.save(file_name)

def create_sheet_hyperlink(file_name:str,table_id:str):
    wb = load_workbook(file_name)
    wb.calc_mode = 'auto'
    sheet_names = wb.sheetnames
    #写入公式
    ws = wb["index"]
    ws["S3"] = f'=HYPERLINK("#\'rem-数据字典\'!"&ADDRESS(MATCH(index!D3,\'rem-数据字典\'!B:B,0),COLUMN(\'rem-数据字典\'!B:B)),">>>数据字典<<<")'
    ws["T3"] = f'=IFERROR(HYPERLINK("#\'rem-代码映射\'!"&ADDRESS(MATCH(index!D3,\'rem-代码映射\'!I:I,0),COLUMN(\'rem-代码映射\'!I:I)),">>>代码映射<<<"),"无代码映射")'
    #超链接sheet页
    sheet_name = ws["C3"].value
    if sheet_name and sheet_name in sheet_names:
        ws["C3"].hyperlink = f"#{sheet_name}!A1"
        ws["C3"].style = "Hyperlink"
    else:
        print(f"[ERROR] sheet页不存在:{sheet_name}")
    wb.save(file_name)

def copy_sheets_and_metadata(source_file,target_file):
    
    #初始化错误列表
    error_list = []
    table_name_list = []
    table_list = []
    #应用显示用于调试
    excel_app = xw.App(visible=False)
    scrpits_folder_path = os.path.abspath(__file__)
    scrpits_dir = os.path.dirname(scrpits_folder_path)
    date_str = datetime.today().strftime('%Y%m%d')
    try:
        
        #获取indexsheet页符合拆分条件的数据集
        src_wb = excel_app.books.open(source_file) 
        index_data = pd.read_excel(source_file, sheet_name = 'index', usecols="C,D,E,F,M")
        index_data = index_data.iloc[1:] #从第2行开始
        index_data.columns = ['table_id','table_name','table_cn_name','dev_pers','enable_flag'] 
        index_data['table_id'] = index_data['table_id'].apply(lambda x:x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']
        y_count = filtered_index.shape[0]
        total_files = y_count


        #检查代码映射和数据字典是否存在
        rem_data_dict_sheet = 'rem-数据字典'
        if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
            error_list.append(f"[ ERROR ] 源SDM文件未找到数据字典sheet页.\n")
            src_wb.close()
            sys.exit()
        src_dd_sheet = src_wb.sheets(rem_data_dict_sheet)
        if not check_vaild_excel(src_dd_sheet):
            print("[ ERROR ] 数据字典 首行校验失败，存在空格，请检查.")
            src_wb.close()
            sys.exit()
        clear_filters(src_dd_sheet)

        rem_code_map_sheet = 'rem-代码映射'
        if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
            error_list.append(f"[ ERROR ] 处理源SDM文件未找到代码映射sheet页.\n")
            src_wb.close()
            sys.exit()
        src_cm_sheet = src_wb.sheets(rem_code_map_sheet)
        if not check_vaild_excel(src_cm_sheet):
            print("[ ERROR ] 代码映射 首行校验失败，存在空格，请检查.")
            src_wb.close()
            sys.exit()
        clear_filters(src_cm_sheet)


        print(f"-==========- 本次将要拆分 {total_files} 个SDM -==========-")
        processed_files = 0
        table_list = [f"{row['table_name']} \n" for i,row in filtered_index.iterrows()]

        

        for _,row in filtered_index.iterrows():
            
            table_name = row['table_name']
            table_cn_name = row['table_cn_name']
            dev_pers = row['dev_pers']
            #替换多个开发人员导致文件路径不存在的问题
            dev_pers = re.sub(r'[\\/]','-',dev_pers)
            table_id = row['table_id']
            table_name_list.append(table_name)

            print(f"[ INFO ] 正在拆分 < {table_cn_name} >.")

            #检查对应文件夹是否存在，如果存在则删除文件夹，如果不存在则新建文件夹

            new_file_folder = os.path.join(scrpits_dir,output_directory,f"{output_directory}_{table_cn_name}")
            new_file = f"{output_directory}_{table_cn_name}_{dev_pers}_{version_num}.xlsx"
            new_file_name = os.path.join(new_file_folder,new_file)

            if not os.path.exists(new_file_folder):
                print(f"[ INFO ] {output_directory}_{table_cn_name} 文件夹不存在，创建文件夹.")
                os.makedirs(new_file_folder)
                #重命名文件为目标文件
                
                create_new_file(target_file,new_file_name)

            else:
                print(f"[ INFO ] {output_directory}_{table_cn_name} 文件夹已存在，移除旧文件并继续.")
                if os.path.exists(new_file_name):
                    os.remove(new_file_name)
                    create_new_file(target_file,new_file_name)
                else:
                    create_new_file(target_file,new_file_name)

            tgt_wb = excel_app.books.open(new_file_name)

            #检查源文件中是否存在对应的sheet页,有未更新目录的情况存在
            if table_id not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"{table_name}不在要处理的源文件中，检查index目录是否为最新版本.\n")
        
            #处理目标文件中的sheet页
            if table_id in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets[table_id].delete()

            #复制sheet内容
            src_sheet = src_wb.sheets[table_id]

            #普通更新
            src_sheet.copy(after=tgt_wb.sheets[-1])
            tgt_wb.sheets[-1].name = table_id

            #dataops平台适用的版本
            src_sheet.copy(after=tgt_wb.sheets[-1])
            tgt_wb.sheets[-1].name = "模型设计"

            tgt_wb.sheets["模型设计"].range('1:2').delete()
            tgt_wb.sheets["模型设计"].range('A:A').delete()
            
            tgt_wb.save()


            #复制数据字典
            if rem_data_dict_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets.add().Name = rem_data_dict_sheet
            tgt_dd_sheet = tgt_wb.sheets(rem_data_dict_sheet)
            clear_filters(tgt_dd_sheet)


            tgt_dd_data = tgt_dd_sheet.range('A1').expand('table').value
            tgt_dd_df = pd.DataFrame(tgt_dd_data)

            if  tgt_dd_df.empty:

                tgt_dd_df = pd.DataFrame(tgt_dd_data[1:],columns=tgt_dd_data[0])
                tgt_dd_df = tgt_dd_df[tgt_dd_df['表英文名'] != table_name]
                tgt_dd_sheet.clear_contents()
                tgt_dd_sheet.range('A1').value = [tgt_dd_data[0]] + tgt_dd_df.values.tolist()

            data_dict_found = False
            for row in src_dd_sheet.range('A2').expand('table').value:
                if row[1] == table_name:
                    data_dict_found = True
                    tgt_dd_sheet.range('A' + str(tgt_dd_sheet.range('A' + str(tgt_dd_sheet.cells.last_cell.row)).end('up').row +1 )).value = row
            if not data_dict_found:
                error_list.append(f"{table_name} 表的数据字典未找到.\n")

            #复制为dataops平台格式

            rem_sheet = tgt_wb.sheets['rem-数据字典']

            rem_sheet.api.Copy(After=tgt_wb.sheets[-1].api)
            new_sheet = tgt_wb.sheets[-1]
            new_sheet.name = "目标表元数据"

            new_sheet.range('A1').api.EntireColumn.Insert()
            new_sheet.range('A1').value = "目标库"

            new_sheet.range('I1').api.EntireColumn.Insert()
            new_sheet.range('I1').value = "长度"

            new_sheet.range('J1').api.EntireColumn.Insert()
            new_sheet.range('J1').value = "精度"  

            tgt_wb.save()       


            #复制代码映射
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
                    error_list.append(f"{table_name} 表的代码映射未找到(若此表无代码映射则忽略).\n")
                else:
                    start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                    tgt_cm_sheet.range(f'A{start_row}').value = filtered_src_cm_df.values.tolist()

            
            #复制index列内容
            rem_index_sheet = 'index'
            src_index_sheet = src_wb.sheets(rem_index_sheet)
            tgt_index_sheet = tgt_wb.sheets(rem_index_sheet)
            src_index_data = src_index_sheet.range('B2').expand('table').value
            start_row = tgt_index_sheet.range('B2').expand('down').last_cell.row + 1
            #print(src_index_data)
            if src_index_data:
                src_index_df = pd.DataFrame(src_index_data[0:],columns=src_index_data[0])
                filtered_src_index_df = src_index_df[src_index_df['表英文名称'] == table_name]
                #print(filtered_src_index_df)
                
                if filtered_src_index_df.empty:
                    error_list.append(f"{table_name} 表的目录未找到.\n")
                else:
                    
                    tgt_index_sheet.range(f'B{start_row}').value = filtered_src_index_df.values.tolist()
                    tgt_index_sheet.range(f'S{start_row}').value = f'=HYPERLINK("#\'rem-数据字典\'!"&ADDRESS(MATCH(index!D{start_row},\'rem-数据字典\'!B:B,0),COLUMN(\'rem-数据字典\'!B:B)),">>>数据字典<<<")'
                    tgt_index_sheet.range(f'T{start_row}').value = f'=IFNA(HYPERLINK("#\'rem-代码映射\'!"&ADDRESS(MATCH(index!D{start_row},\'rem-代码映射\'!I:I,0),COLUMN(\'rem-代码映射\'!I:I)),">>>代码映射<<<"),"")'
            
            #index_data = filter_index_data(source_file,table_cn_name)

            #添加超链接
            sheet_names = [sheet.name for sheet in tgt_wb.sheets]
            #print(sheet_names[0])
            if  table_id in sheet_names:
                tgt_index_sheet.range(f'C{start_row}').add_hyperlink(f"#{table_id}!A1",f"{table_id}")
            else:
                print(f"[ERROR] sheet与目录对应有差异，不能建立超链接，请检查：{table_id}")


            tgt_wb.save(new_file_name)
            tgt_wb.close()

            # with pd.ExcelWriter(new_file_name,mode='a',if_sheet_exists='overlay',engine='openpyxl') as writer:
            #     index_data.to_excel(writer,sheet_name="index",index=False,header=False,startrow=2,startcol=0)
            #ST列写入数据字典和代码映射的公式
            #write_formula_to_excel(new_file_name)
            #create_sheet_hyperlink(new_file_name,table_id)

            processed_files += 1
            
            print(f"-==========- 表 {table_cn_name} 处理完成,剩余 < {total_files-processed_files} > 个,共< {total_files} >个-=========-")
        #         outer.update(1)
        # outer.close()
            
    except Exception as e:
        error_list.append(str(e))
    finally:
        excel_app.quit()

    return error_list,table_name_list,table_list




if __name__ == "__main__":
    # source_file = 'D:/git/tools/08_sdm_collect_tool/sdm/3.xlsm'
    # target_file = 'sdm_文档_开发_第1批.xlsm'
    if len(sys.argv) != 4:
            print("Usage: python sdm_split.py <source_file> <template_file> <version>")
            sys.exit(1)
    source_file = sys.argv[1]
    target_file = sys.argv[2]
    version_num = sys.argv[3]
    print(f"[ INFO ] 获取版本号成功：{version_num}.")
    print(f"[ INFO ] 获取源文件成功：{os.path.basename(source_file)}.")
    print(f"[ INFO ] 提示：若提示代码映射未找到请手动核对代码映射及对应表是否对应与缺失.")
    #检查条线文件夹是否创建,获取文件名分割条线
    output_directory = get_output_dirname(os.path.basename(source_file))
    #检查文件夹是否存在，存在则继续，不存在则新建
    check_file_folder_exists(output_directory)
    errors,table_name_list,table_list = copy_sheets_and_metadata(source_file,target_file)
    print(f"#### 本次共拆分<{len(table_name_list)}>个sdm信息 #### \n| {'| '.join(table_list)} " )
    if errors:
        print(f"#### 提示信息 ####:\n")
        for error in errors:
            print(f"|{error}")