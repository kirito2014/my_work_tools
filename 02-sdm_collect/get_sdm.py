import pandas as pd
import os
import xlwings as xw

def copy_sheets_and_metadata(source_file, target_file):
    # 初始化错误列表
    error_list = []

    # 打开 Excel 文件
    excel_app = xw.App(visible=False)
    
    try:
        src_wb = xw.Book(source_file)
        if os.path.exists(target_file):
            tgt_wb = xw.Book(target_file)
        else:
            tgt_wb = xw.Book()
            tgt_wb.save(target_file)

        # 获取 Index 表中所有标记为 Y 的行
        index_data = pd.read_excel(source_file, sheet_name='index', usecols="C,D,M")
        index_data = index_data.iloc[1:]
        index_data.columns = ['table_id', 'table_name', 'enable_flag']
        index_data['table_id'] = index_data['table_id'].apply(lambda x: x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']

        # 计算标记为 Y 的数量
        y_count = filtered_index.shape[0]
        print(f"Number of sheets marked as 'Y': {y_count}")

        for _, row in filtered_index.iterrows():
            table_id = row['table_id']
            table_name = row['table_name']

            # 检查源文件中是否存在对应的 sheet 页
            if table_id not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Sheet {table_id} not found in source file.")
                continue

            # 删除目标文件中的同名 sheet 页
            if table_id in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets[table_id].delete()

            # 复制 sheet 页内容
            src_sheet = src_wb.sheets[table_id]
            src_sheet.api.Copy(Before=tgt_wb.sheets[0].api)
            tgt_wb.sheets[0].name = table_id

            # 复制数据字典
            rem_data_dict_sheet = 'rem-数据字典'
            if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Data dictionary sheet not found for table {table_name}.")
            else:
                src_dd_sheet = src_wb.sheets[rem_data_dict_sheet]
                if rem_data_dict_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                    tgt_wb.sheets.add(rem_data_dict_sheet)
                tgt_dd_sheet = tgt_wb.sheets[rem_data_dict_sheet]

                # 删除目标文件中的数据字典相关内容
                tgt_dd_data = tgt_dd_sheet.range('B2').expand('table').value
                if tgt_dd_data:
                    tgt_dd_df = pd.DataFrame(tgt_dd_data[1:], columns=tgt_dd_data[0])
                    tgt_dd_df = tgt_dd_df[tgt_dd_df['表英文名'] != table_name]
                    tgt_dd_sheet.clear_contents()
                    tgt_dd_sheet.range('B2').value = [tgt_dd_data[0]] + tgt_dd_df.values.tolist()

                data_dict_found = False
                for row in src_dd_sheet.range('A2').expand('table').value:
                    if row[1] == table_name:
                        data_dict_found = True
                        tgt_dd_sheet.range('A' + str(tgt_dd_sheet.range('A' + str(tgt_dd_sheet.cells.last_cell.row)).end('up').row + 1)).value = row

                if not data_dict_found:
                    error_list.append(f"Data dictionary not found for table {table_name}.")

            # 复制代码映射
            rem_code_map_sheet = 'rem-代码映射'
            if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Code mapping sheet not found for table {table_name}.")
            else:
                src_cm_sheet = src_wb.sheets[rem_code_map_sheet]
                if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                    tgt_wb.sheets.add(rem_code_map_sheet)
                tgt_cm_sheet = tgt_wb.sheets[rem_code_map_sheet]

                # 删除目标文件中的代码映射相关内容
                tgt_cm_data = tgt_cm_sheet.range('I3').expand('table').value
                if tgt_cm_data:
                    tgt_cm_df = pd.DataFrame(tgt_cm_data[1:], columns=tgt_cm_data[0])
                    tgt_cm_df = tgt_cm_df[tgt_cm_df['表英文名'] != table_name]
                    tgt_cm_sheet.clear_contents()
                    tgt_cm_sheet.range('I3').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()

                code_map_found = False
                for row in src_cm_sheet.range('A3').expand('table').value:
                    if row[8] == table_name:
                        code_map_found = True
                        tgt_cm_sheet.range('A' + str(tgt_cm_sheet.range('A' + str(tgt_cm_sheet.cells.last_cell.row)).end('up').row + 1)).value = row

                if not code_map_found:
                    error_list.append(f"Code mapping not found for table {table_name}.")

        # 保存并关闭目标文件
        tgt_wb.save(target_file)
        tgt_wb.close()
        src_wb.close()
    except Exception as e:
        error_list.append(str(e))
    finally:
        excel_app.quit()

    # 返回错误列表
    return error_list

# 使用示例
source_file = 'source.xlsx'
target_file = 'target.xlsx'
errors = copy_sheets_and_metadata(source_file, target_file)
print(f"Errors: {errors}")
