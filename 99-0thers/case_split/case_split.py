import os
import openpyxl
import pandas as pd
from openpyxl.utils import get_column_letter
from openpyxl import Workbook

# 模块 1：创建新表并添加内容
def add_sheet(wb, sheet_name="AddSheet"):
    """
    创建一个新的工作表，命名为指定名称
    """
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.create_sheet(title=sheet_name)
    return ws

# 模块 2：插入表格内容到新工作表
def loop_insert(wb, ws, arr="标题及目录,售前情况统计表,部门商机明细,商机报工明细"):
    """
    将数组中的表名插入新创建的工作表，并按顺序填入单元格
    """
    sheet_list = arr.split(',')
    for i, sheet_name in enumerate(sheet_list):
        ws.cell(row=i + 1, column=1, value=sheet_name)
    return sheet_list

# 模块 3：根据指定顺序调整工作表顺序
def change_order(wb, sheet_list):
    """
    根据指定顺序调整工作表的顺序
    """
    for i, sheet_name in enumerate(sheet_list):
        if sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            wb._sheets.remove(sheet)
            wb._sheets.insert(i, sheet)

# 模块 4：删除指定的工作表
def delete_sheet(wb, sheet_name):
    """
    删除指定的工作表
    """
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]

# 模块 5：日期格式化函数
def format_time(start_date, flag=1):
    """
    根据指定的格式标志，将日期格式化为指定格式
    flag:
        1 - yyyy-mm-dd hh:mm:ss
        2 - yyyy-mm-dd
        3 - hh:mm:ss
        4 - yyyy年mm月dd日
        5 - yyyymmdd
        6 - yyyymm
        7 - yyyy-mm+1-dd
        8 - mm
    """
    if pd.isnull(start_date):
        return None
    date_format = {
        1: "%Y-%m-%d %H:%M:%S",
        2: "%Y-%m-%d",
        3: "%H:%M:%S",
        4: "%Y年%m月%d日",
        5: "%Y%m%d",
        6: "%Y%m",
        7: "%Y-%m-%d",  # 假设这是增加月份的格式
        8: "%m"
    }
    return start_date.strftime(date_format.get(flag, "%Y-%m-%d %H:%M:%S"))

# 模块 6：查找指定部门所在的列号
def find_col(sheet, dept_name):
    """
    查找指定部门所在的列号
    """
    for col in sheet.iter_cols(1, sheet.max_column):
        for cell in col:
            if cell.value == dept_name:
                return cell.column
    return None

# 模块 7：查找工作表中是否存在错误，并记录错误信息
def check_errors(wb):
    """
    检查工作簿中的错误，并生成一张错误信息表
    """
    error_list = []
    for sheet in wb.worksheets:
        if sheet.title not in ["部门清单"]:
            for row in sheet.iter_rows():
                for cell in row:
                    if isinstance(cell.value, str) and "#ERROR" in cell.value:
                        error_list.append(f"{sheet.title} 的第 {cell.row} 行第 {cell.column} 列")

    if error_list:
        error_sheet = add_sheet(wb, "错误信息")
        for i, error in enumerate(error_list, 1):
            error_sheet.cell(row=i, column=1, value=error)
        return len(error_list)
    return 0

# 模块 8：文件是否存在检查
def is_file_exists(file_path):
    """
    检查文件是否存在
    """
    return os.path.exists(file_path)

# 模块 9：列号与字母转换
def col_to_letter(col):
    """
    将列号转为字母
    """
    return get_column_letter(col)

def letter_to_col(letter):
    """
    将字母转为列号
    """
    return openpyxl.utils.column_index_from_string(letter)

# 主函数流程
def main_process(excel_path, dept_name):
    """
    主函数，处理 Excel 工作表，并应用各模块的功能
    """
    if not is_file_exists(excel_path):
        print(f"文件 {excel_path} 不存在")
        return

    # 加载工作簿
    wb = openpyxl.load_workbook(excel_path)
    
    # 创建新表
    ws = add_sheet(wb)
    
    # 插入内容
    sheet_list = loop_insert(wb, ws)
    
    # 调整表顺序
    change_order(wb, sheet_list)
    
    # 删除新增的表
    delete_sheet(wb, "AddSheet")

    # 检查是否存在错误
    error_count = check_errors(wb)
    if error_count > 0:
        print(f"发现 {error_count} 个错误，请查看 '错误信息' 表")
    else:
        print("未发现错误")

    # 保存处理后的文件
    wb.save(excel_path)
    print(f"文件 {excel_path} 已成功保存")

# 示例调用
if __name__ == "__main__":
    excel_file = "your_excel_file.xlsx"
    department_name = "业务支持部"
    main_process(excel_file, department_name)
