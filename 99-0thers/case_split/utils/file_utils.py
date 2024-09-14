import os
import pandas as pd
import shutil

# 模块1: 读取文件并提取部门信息
def get_unique_departments(source_file: str, sheet_name: str) -> list:
    """从源文件中读取部门信息并去重"""
    df = pd.read_excel(source_file, sheet_name=sheet_name, usecols=['B'])
    departments = df['B'].dropna().unique().tolist()  # 去重部门列表
    return departments

# 模块2: 检测并删除旧文件
def check_and_remove_file(directory: str, file_name: str):
    """检查文件夹下是否有该文件，如有则删除"""
    file_path = os.path.join(directory, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"文件 {file_name} 已删除。")
    else:
        print(f"文件 {file_name} 不存在，无需删除。")

# 模块3: 创建新文件并保存
def create_new_file(template_file: str, directory: str, dp_name: str):
    """从模板创建新的部门文件"""
    new_file_name = f"{template_file}-{dp_name}.xlsx"
    new_file_path = os.path.join(directory, new_file_name)
    shutil.copy(template_file, new_file_path)  # 复制模板文件
    print(f"新文件 {new_file_name} 已创建。")
    return new_file_path

# 主函数: 完整流程
def process_files(template_file: str, source_file: str, output_directory: str, sheet_name='售前情况统计表'):
    """循环处理部门列表，创建新文件"""
    departments = get_unique_departments(source_file, sheet_name)
    
    for dp_name in departments:
        new_file_name = f"{template_file}-{dp_name}.xlsx"
        check_and_remove_file(output_directory, new_file_name)
        create_new_file(template_file, output_directory, dp_name)

# 示例调用
template_file = '模板文件.xlsx'
source_file = '源文件.xlsx'
output_directory = 'output_folder'

process_files(template_file, source_file, output_directory)
