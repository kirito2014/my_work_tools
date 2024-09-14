import pandas as pd
import os

def filter_department_data(workbook_name, sheet_name, department_name):
    # 检查文件是否存在
    if not os.path.exists(workbook_name):
        raise FileNotFoundError(f"文件 '{workbook_name}' 不存在")
    
    # 检查工作簿是否能被打开，以及是否包含指定的 sheet
    try:
        excel_file = pd.ExcelFile(workbook_name)
    except Exception as e:
        raise ValueError(f"无法读取文件 '{workbook_name}'，错误: {e}")
    
    if sheet_name not in excel_file.sheet_names:
        raise ValueError(f"Sheet '{sheet_name}' 在文件 '{workbook_name}' 中不存在")
    
    # 读取 Excel 文件中的指定 sheet
    df = pd.read_excel(workbook_name, sheet_name=sheet_name)
    
    # 检查 B 列是否存在
    if '归属业务部' not in df.columns:
        raise ValueError("归属业务部 列在工作表中不存在")
    
    # 筛选 B 列中对应部门名称的数据
    filtered_df = df[df['归属业务部'] == department_name]
    
    return filtered_df

# 示例用法
if __name__ == '__main__':
    workbook_name = 'D:\github\99-0thers\case_split\original_file\售前统计_202408(1).xlsm'
    sheet_name = '部门商机明细'
    department_name = '金融业务三部'

    try:
        result_df = filter_department_data(workbook_name, sheet_name, department_name)
        print(result_df)
    except Exception as e:
        print(f"发生错误: {e}")
