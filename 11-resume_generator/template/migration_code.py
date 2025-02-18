import openpyxl
from openpyxl.utils import get_column_letter
import pandas as pd

def get_max_row_in_column_a_with_merge(ws):
    """获取A列的最大行数，考虑合并单元格"""
    last_row = ws.max_row
    cell = ws.cell(row=last_row, column=1)

    # 检查是否为合并单元格
    for merged_range in ws.merged_cells.ranges:
        if cell.coordinate in merged_range:
            max_row = merged_range.max_row
            break
    else:
        max_row = last_row

    return max_row

def get_merged_cell_range_rows(ws, row):
    """获取合并单元格的行范围"""
    cell = ws.cell(row=row, column=1)
    start_row = cell.row
    end_row = cell.row

    # 检查是否为合并单元格
    for merged_range in ws.merged_cells.ranges:
        if cell.coordinate in merged_range:
            start_row = merged_range.min_row
            end_row = merged_range.max_row
            break

    return start_row, end_row
def delete_empty_rows_with_pandas(file_path, sheet_name="数据来源"):
    # 使用 pandas 读取 Excel 文件
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # 删除 "PersonName" 列为空的行
    df = df.dropna(subset=["基本情况-姓名"])

    # 使用 openpyxl 加载 Excel 文件
    wb = openpyxl.load_workbook(file_path)
    ws = wb[sheet_name]

    # 清空工作表
    ws.delete_rows(2, ws.max_row)

    # 将处理后的数据写回工作表
    for row in df.itertuples(index=False):
        ws.append(row)

    # 保存文件
    wb.save(file_path)
    print("数据预处理完成！请继续操作")

def delete_empty_rows(ws):
    """删除空行"""
    last_row = get_max_row_in_column_a_with_merge(ws)

    # 从最后一行向前遍历数据
    row = last_row
    print(f"总行数：{last_row}")
    while row >= 2:
        start_row, end_row = get_merged_cell_range_rows(ws, row)
        print(f"起始行：{start_row}，结束行：{end_row}")
        person_name = ws.cell(row=start_row, column=5).value
        print(f"姓名：{person_name}")

        # 检查是否为未填写名称行
        if person_name is None or person_name.strip() == "":
            # 删除内容为空的行
            rows_to_delete = range(start_row, end_row + 1)
            for row in sorted(rows_to_delete, reverse=True):
                print(f"删除行 {row}")
                ws.delete_rows(row)

        # 跳转到下一组行
        row = start_row - 1

def move_columns(ws):
    """移动列"""
    if "结束时间" not in ws.cell(row=1, column=18).value:
        ws.move_range("T:U", rows=0, cols=-2)
        ws.move_range("Y:Z", rows=0, cols=-3)
        ws.move_range("AE:AE", rows=0, cols=-3)
        ws.move_range("AJ:AJ", rows=0, cols=-3)
        ws.move_range("AO:AO", rows=0, cols=-3)

def process_main_data(ws):
    """处理主数据，去重并保留最新数据"""
    remove_dup_sheet = ws.parent.create_sheet("RemoveDuplicate")
    remove_dup_sheet.append(["PersonName", "UpdateTime", "StartRow", "EndRow", "DeleteFlag"])

    person_data = {}
    last_row = get_max_row_in_column_a_with_merge(ws)

    for row in range(last_row, 1, -1):
        start_row, end_row = get_merged_cell_range_rows(ws, row)
        person_name = ws.cell(row=start_row, column=5).value
        update_time = ws.cell(row=start_row, column=3).value

        if person_name in person_data:
            if update_time > person_data[person_name]["update_time"]:
                remove_dup_sheet.cell(row=person_data[person_name]["row"], column=5, value="Y")
                person_data[person_name] = {"update_time": update_time, "row": len(remove_dup_sheet['A']) + 1}
            else:
                remove_dup_sheet.append([person_name, update_time, start_row, end_row, "Y"])
        else:
            person_data[person_name] = {"update_time": update_time, "row": len(remove_dup_sheet['A']) + 1}
            remove_dup_sheet.append([person_name, update_time, start_row, end_row, "N"])

    delete_rows = []
    for row in remove_dup_sheet.iter_rows(min_row=2, values_only=True):
        if row[4] == "Y":
            delete_rows.extend(range(row[2], row[3] + 1))

    for row in sorted(delete_rows, reverse=True):
        ws.delete_rows(row)

    ws.parent.remove(remove_dup_sheet)

def replace_year_and_month(ws):
    """替换日期中的年和月"""
    replace_columns = ["Q", "R", "V", "W", "AA", "AB", "AF", "AG", "AK", "AL", "AT", "AU"]
    for col in replace_columns:
        for cell in ws[col]:
            if cell.value:
                cell.value = cell.value.replace("年", "/").replace("月", "")

def data_preprocessing(file_path):
    """数据预处理主函数"""
    wb = openpyxl.load_workbook(file_path)
    ws = wb["数据来源"]
    delete_empty_rows_with_pandas(file_path)
    #delete_empty_rows(ws)
    # move_columns(ws)
    # process_main_data(ws)
    # replace_year_and_month(ws)

    wb.save(file_path)
    print("数据预处理完成！请继续操作")

# 使用示例
data_preprocessing("人员简历汇总_20241103.xlsx")