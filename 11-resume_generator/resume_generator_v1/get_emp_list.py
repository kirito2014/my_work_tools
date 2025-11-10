import os
import json
import sys
import traceback

# 导入pandas
try:
    import pandas as pd
except ImportError:
    print("错误: 未找到pandas模块，请使用 pip install pandas 安装")
    sys.exit(1)

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入ExcelReader类
from package.utils.excel_reader import ExcelReader
from package.utils.file_helper import create_dirs, write_file

def get_employee_info(excel_file_path, sheet_name=None):
    """
    从Excel文件获取员工编号、工作名称、一级部门、二级部门信息
    
    Args:
        excel_file_path: Excel文件路径
        sheet_name: 工作表名称，默认为None（读取第一个工作表）
    
    Returns:
        员工信息列表
    """
    print(f"正在处理Excel文件: {excel_file_path}")
    
    try:
        # 使用pandas读取Excel文件，支持自动检测格式
        if sheet_name:
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        else:
            # 尝试读取第一个工作表
            df = pd.read_excel(excel_file_path)
        
        print(f"成功读取Excel文件，共 {len(df)} 行数据，工作表: {df.name if hasattr(df, 'name') else '默认工作表'}")
        
        # 显示所有列名以便调试
        print("Excel文件中的列名:")
        for i, col in enumerate(df.columns):
            print(f"  {i}. '{col}'")
        
        # 查找对应的列
        column_mapping = {
            'EmpNo': [],       # 员工编号
            'JobName': [],     # 工作名称
            'Level1Dept': [],  # 一级部门
            'Level2Dept': []   # 二级部门
        }
        
        # 定义关键词映射
        keyword_mappings = {
            'EmpNo': ['员工编号', '工号', 'EmpNo', '编号', 'NO', 'no'],
            'JobName': ['工作名', '姓名', 'Name', 'name', '员工姓名', '姓名'],
            'Level1Dept': ['管理关系一级部门', '一级部门', 'Level1Dept', '部门1'],
            'Level2Dept': ['管理关系二级部门', '二级部门', 'Level2Dept', '部门2']
        }
        
        # 查找匹配的列
        for col_index, col_name in enumerate(df.columns):
            col_name_str = str(col_name).strip()
            for field, keywords in keyword_mappings.items():
                for keyword in keywords:
                    if keyword in col_name_str:
                        column_mapping[field].append(col_index)
                        print(f"  列 '{col_name_str}' 可能匹配字段 '{field}'")
                        break
        
        # 选择第一个匹配的列
        emp_no_col = column_mapping['EmpNo'][0] if column_mapping['EmpNo'] else None
        job_name_col = column_mapping['JobName'][0] if column_mapping['JobName'] else None
        level1_dept_col = column_mapping['Level1Dept'][0] if column_mapping['Level1Dept'] else None
        level2_dept_col = column_mapping['Level2Dept'][0] if column_mapping['Level2Dept'] else None
        
        # 如果找不到员工编号列，尝试使用第一列
        if emp_no_col is None:
            print("警告: 未找到员工编号列，尝试使用第一列")
            emp_no_col = 0
        
        # 如果找不到工作名列，尝试使用第二列
        if job_name_col is None and len(df.columns) > 1:
            print("警告: 未找到工作名列，尝试使用第二列")
            job_name_col = 1
        
        # 提取员工信息
        employee_list = []
        for index, row in df.iterrows():
            emp_info = {}
            
            # 提取员工编号
            if emp_no_col is not None and emp_no_col < len(row):
                emp_no_value = row.iloc[emp_no_col]
                if pd.notna(emp_no_value):
                    emp_no = str(emp_no_value).strip()
                    # 如果是数字，确保格式一致，保留前导零，格式化为5位
                    if emp_no.isdigit():
                        # 格式化为5位，保留前导零
                        emp_info["EmpNo"] = emp_no.zfill(5)
                    else:
                        emp_info["EmpNo"] = emp_no
                else:
                    continue  # 跳过无工号的行
            else:
                continue
            
            # 提取工作名称
            if job_name_col is not None and job_name_col < len(row):
                job_value = row.iloc[job_name_col]
                if pd.notna(job_value):
                    emp_info["JobName"] = str(job_value).strip()
                else:
                    emp_info["JobName"] = ""
            else:
                emp_info["JobName"] = ""
            
            # 提取一级部门
            if level1_dept_col is not None and level1_dept_col < len(row):
                level1_value = row.iloc[level1_dept_col]
                if pd.notna(level1_value):
                    emp_info["Level1Dept"] = str(level1_value).strip()
                else:
                    emp_info["Level1Dept"] = ""
            else:
                emp_info["Level1Dept"] = ""
            
            # 提取二级部门
            if level2_dept_col is not None and level2_dept_col < len(row):
                level2_value = row.iloc[level2_dept_col]
                if pd.notna(level2_value):
                    emp_info["Level2Dept"] = str(level2_value).strip()
                else:
                    emp_info["Level2Dept"] = ""
            else:
                emp_info["Level2Dept"] = ""
            
            # 跳过空记录
            if any(emp_info.values()):
                employee_list.append(emp_info)
        
        # 去重，基于员工编号
        unique_employees = {}
        for emp in employee_list:
            emp_no = emp.get("EmpNo", "")
            if emp_no and emp_no not in unique_employees:
                unique_employees[emp_no] = emp
        
        employee_list = list(unique_employees.values())
        
        print(f"成功提取 {len(employee_list)} 条有效员工信息")
        return employee_list
    
    except Exception as e:
        print(f"读取Excel文件并提取员工信息时出错: {str(e)}")
        traceback.print_exc()
        return []

def save_employee_list(employee_list, output_file):
    """
    保存员工列表到文件
    
    Args:
        employee_list: 员工信息列表
        output_file: 输出文件路径
    
    Returns:
        是否保存成功
    """
    try:
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"创建目录: {output_dir}")
        
        # 保存为JSON格式
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(employee_list, f, ensure_ascii=False, indent=2)
        
        print(f"成功保存员工列表到: {output_file}")
        print(f"共保存 {len(employee_list)} 条员工信息")
        
        # 输出部门统计信息
        if employee_list:
            level1_depts = {}
            for emp in employee_list:
                level1 = emp.get("Level1Dept", "")
                if level1:
                    if level1 not in level1_depts:
                        level1_depts[level1] = set()
                    level2 = emp.get("Level2Dept", "")
                    if level2:
                        level1_depts[level1].add(level2)
            
            print(f"\n部门统计:")
            for dept in sorted(level1_depts.keys()):
                level2_count = len(level1_depts[dept])
                dept_emps = [emp for emp in employee_list if emp.get("Level1Dept") == dept]
                print(f"  {dept}: {len(dept_emps)} 人, {level2_count} 个二级部门")
        
        return True
    except Exception as e:
        print(f"[ERROR] 保存员工列表时出错: {str(e)}")
        traceback.print_exc()
        return False

def main():
    print("\n===== 员工信息提取工具 =====")
    
    # 获取当前脚本的绝对路径
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 设置默认的Excel文件路径
    default_excel_path = os.path.join(script_dir, "input", "技术人员名单-11月.xlsx")
    
    # 允许用户通过命令行参数指定Excel文件路径
    if len(sys.argv) > 1:
        excel_file_path = sys.argv[1]
        # 如果是相对路径，相对于当前工作目录
        if not os.path.isabs(excel_file_path):
            excel_file_path = os.path.abspath(excel_file_path)
    else:
        excel_file_path = default_excel_path
        print(f"未指定Excel文件路径，使用默认路径: {excel_file_path}")
    
    # 确保Excel文件存在，如果不存在则打开文件选择对话框
    if not os.path.exists(excel_file_path):
        print(f"错误: Excel文件不存在: {excel_file_path}")
        print("正在打开文件选择对话框...")
        
        # 尝试导入tkinter用于文件选择
        try:
            import tkinter as tk
            from tkinter import filedialog
            
            # 创建一个隐藏的Tk窗口
            root = tk.Tk()
            root.withdraw()  # 隐藏主窗口
            
            # 打开文件选择对话框
            excel_file_path = filedialog.askopenfilename(
                title="选择技术人员名单Excel文件",
                filetypes=[("Excel files", "*.xlsx;*.xls")]
            )
            
            # 检查用户是否选择了文件
            if not excel_file_path:
                print("未选择文件，程序退出")
                sys.exit(1)
                
        except ImportError:
            print("无法打开文件选择对话框，请手动指定文件路径")
            sys.exit(1)
    
    # 设置输出文件路径
    output_file = os.path.join(script_dir, "config", "emp_list.json")
    
    print(f"\n开始处理Excel文件，提取员工信息...")
    print(f"输入文件: {excel_file_path}")
    print(f"输出文件: {output_file}")
    
    # 获取员工信息
    employee_list = get_employee_info(excel_file_path)
    
    if not employee_list:
        print("[ERROR] 没有成功提取任何员工数据，程序退出")
        sys.exit(1)
    
    # 保存员工列表
    if save_employee_list(employee_list, output_file):
        print("\n处理完成！")
        sys.exit(0)
    else:
        print("\n处理失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()