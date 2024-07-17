import pandas as pd
import os
import win32com.client as win32

def copy_sheets_and_metadata(source_file, target_file):
    # 初始化错误列表
    error_list = []

    # 加载Excel文件
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = False

    src_wb = excel.Workbooks.Open(source_file)
    tgt_wb = excel.Workbooks.Open(target_file) if os.path.exists(target_file) else excel.Workbooks.Add()

    # 获取Index表中所有标记为Y的行
    index_data = pd.read_excel(source_file, sheet_name='index', usecols="C,D,M")
    index_data = index_data.iloc[1:]
    index_data.columns = ['table_id','table_name','enable_flag']
    index_data['table_id'] = index_data['table_id'].apply(lambda x: x.split("'")[1] if "'" in x else x)
    filtered_index = index_data[index_data['enable_flag'] == 'Y']

    for _, row in filtered_index.iterrows():
        table_id = row['table_id']
        table_name = row['table_name']

        # 检查源文件中是否存在对应的sheet页
        if table_id not in [sheet.Name for sheet in src_wb.Sheets]:
            error_list.append(f"Sheet {table_id} not found in source file.")
            continue

        # 删除目标文件中的同名sheet页
        if table_id in [sheet.Name for sheet in tgt_wb.Sheets]:
            tgt_wb.Sheets(table_id).Delete()

        # 复制sheet页内容
        src_sheet = src_wb.Sheets(table_id)
        src_sheet.Copy(Before=tgt_wb.Sheets(1))
        tgt_wb.Sheets(1).Name = table_id

        # 复制数据字典
        rem_data_dict_sheet = 'rem-数据字典'
        if rem_data_dict_sheet not in [sheet.Name for sheet in src_wb.Sheets]:
            error_list.append(f"Data dictionary sheet not found for table {table_name}.")
        else:
            src_dd_sheet = src_wb.Sheets(rem_data_dict_sheet)
            if rem_data_dict_sheet not in [sheet.Name for sheet in tgt_wb.Sheets]:
                tgt_wb.Sheets.Add().Name = rem_data_dict_sheet
            tgt_dd_sheet = tgt_wb.Sheets(rem_data_dict_sheet)

            # 删除目标文件中的数据字典相关内容
            tgt_dd_range = tgt_dd_sheet.UsedRange
            for row in range(tgt_dd_range.Rows.Count, 1, -1):
                if tgt_dd_range.Cells(row, 2).Value == table_name:
                    tgt_dd_range.Rows(row).Delete()

            data_dict_found = False
            for row in src_dd_sheet.UsedRange.Rows:
                if row.Cells(1, 2).Value == table_name:
                    data_dict_found = True
                    tgt_dd_sheet.Rows(tgt_dd_sheet.UsedRange.Rows.Count + 1).Value = row.Value

            if not data_dict_found:
                error_list.append(f"Data dictionary not found for table {table_name}.")

        # 复制代码映射
        rem_code_map_sheet = 'rem-代码映射'
        if rem_code_map_sheet not in [sheet.Name for sheet in src_wb.Sheets]:
            error_list.append(f"Code mapping sheet not found for table {table_name}.")
        else:
            src_cm_sheet = src_wb.Sheets(rem_code_map_sheet)
            if rem_code_map_sheet not in [sheet.Name for sheet in tgt_wb.Sheets]:
                tgt_wb.Sheets.Add().Name = rem_code_map_sheet
            tgt_cm_sheet = tgt_wb.Sheets(rem_code_map_sheet)

            # 删除目标文件中的代码映射相关内容
            tgt_cm_range = tgt_cm_sheet.UsedRange
            for row in range(tgt_cm_range.Rows.Count, 3, -1):
                if tgt_cm_range.Cells(row, 9).Value == table_name:
                    tgt_cm_range.Rows(row).Delete()

            code_map_found = False
            for row in src_cm_sheet.UsedRange.Rows:
                if row.Cells(1, 9).Value == table_name:
                    code_map_found = True
                    tgt_cm_sheet.Rows(tgt_cm_sheet.UsedRange.Rows.Count + 1).Value = row.Value

            if not code_map_found:
                error_list.append(f"Code mapping not found for table {table_name}.")

    # 保存并关闭目标文件
    tgt_wb.SaveAs(target_file)
    tgt_wb.Close()
    src_wb.Close()
    excel.Application.Quit()

    # 返回错误列表
    return error_list

# 使用示例
source_file = 'source.xlsx'
target_file = 'target.xlsx'
errors = copy_sheets_and_metadata(source_file, target_file)
print(f"Errors: {errors}")
