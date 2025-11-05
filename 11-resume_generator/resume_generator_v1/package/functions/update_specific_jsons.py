import os
import json
import sys
import argparse

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) 

# 导入必要的模块
try:
    import pandas as pd
    # 导入现有的Excel转JSON功能
    from package.functions.excel_2_info_json import convert_excel_to_json, create_modify_json_format, save_json_files
    from package.utils.file_helper import read_json_file, write_json_file
except ImportError as e:
    print(f"导入模块失败: {e}")
    sys.exit(1)

def parse_employee_numbers(employee_arg):
    """
    解析员工参数，返回工号列表
    """
    if employee_arg.upper() == "ALL":
        return "ALL"
    
    # 处理列表格式，支持 ['07003','02794'] 或 07003,02794 格式
    try:
        # 尝试作为JSON列表解析
        if employee_arg.startswith('[') and employee_arg.endswith(']'):
            employee_list = json.loads(employee_arg)
            return [emp.strip() for emp in employee_list if emp.strip()]
        # 尝试作为逗号分隔字符串解析
        else:
            employee_list = [emp.strip() for emp in employee_arg.split(',') if emp.strip()]
            return employee_list
    except Exception:
        print(f"无效的员工参数格式: {employee_arg}")
        print("请使用以下格式之一:")
        print("1. ALL - 更新所有员工")
        print("2. ['工号1', '工号2'] - JSON格式列表")
        print("3. 工号1,工号2 - 逗号分隔的字符串")
        sys.exit(1)

def update_resume_jsons(employee_numbers, excel_data, base_dir="output"):
    """
    更新简历JSON文件
    """
    resume_dir = os.path.join(base_dir, "modify_json")
    if not os.path.exists(resume_dir):
        print(f"简历JSON目录不存在: {resume_dir}")
        return 0
    
    updated_count = 0
    # 创建工号到员工数据的映射
    emp_map = {emp.get("EmpNo", "").zfill(5): emp for emp in excel_data}
    
    # 如果是更新所有员工
    if employee_numbers == "ALL":
        target_emp_numbers = list(emp_map.keys())
    else:
        # 格式化工号为5位数
        target_emp_numbers = [emp_no.zfill(5) for emp_no in employee_numbers]
    
    # 查找并更新对应的简历JSON文件
    for emp_no in target_emp_numbers:
        if emp_no not in emp_map:
            print(f"警告: 工号 {emp_no} 在Excel数据中未找到")
            continue
        
        # 查找对应的简历JSON文件
        found = False
        for filename in os.listdir(resume_dir):
            if filename.startswith(emp_no) and filename.endswith(".json"):
                file_path = os.path.join(resume_dir, filename)
                try:
                    # 读取现有简历文件
                    resume_data = read_json_file(file_path)
                    if not resume_data:
                        continue
                    
                    # 获取员工姓名
                    emp_data = emp_map[emp_no]
                    name = emp_data.get("Name", "未知")
                    
                    # 更新AddtionInfo
                    if name in resume_data:
                        resume_data[name]["AddtionInfo"] = emp_data
                        # 写回文件
                        write_json_file(file_path, resume_data)
                        print(f"✅ 已更新简历JSON: {filename}")
                        updated_count += 1
                        found = True
                        break
                except Exception as e:
                    print(f"❌ 更新简历文件 {filename} 时出错: {e}")
        
        if not found:
            print(f"⚠️  未找到工号 {emp_no} 对应的简历JSON文件")
    
    return updated_count

def update_info_jsons(employee_numbers, excel_data, base_dir="output"):
    """
    更新信息JSON文件
    """
    info_dir = os.path.join(base_dir, "info_json")
    
    # 创建符合modify_json格式的数据
    modify_data = create_modify_json_format(excel_data)
    
    # 如果只更新特定员工，过滤modify_data
    if employee_numbers != "ALL":
        filtered_data = {}
        target_emp_numbers = [emp_no.zfill(5) for emp_no in employee_numbers]
        
        # 从modify_data中过滤出目标员工
        for name, data in modify_data.items():
            emp_no = data["AddtionInfo"].get("EmpNo", "").zfill(5)
            if emp_no in target_emp_numbers:
                filtered_data[name] = data
        
        modify_data = filtered_data
    
    # 保存更新后的信息JSON文件
    if modify_data:
        save_json_files(modify_data, info_dir)
        return len(modify_data)
    return 0

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="更新特定人员的JSON文件")
    parser.add_argument("update_option", type=int, choices=[1, 2, 3], 
                       help="更新选项: 1-只更新简历json, 2-只更新信息json, 3-两者都更新")
    parser.add_argument("employee_numbers", 
                       help="更新人员工号，支持ALL或列表格式如['07003','02794']或逗号分隔如07003,02794")
    parser.add_argument("--excel", default=os.path.join("input", "技术人员名单-8月（删减版）.xlsx"), 
                       help="Excel文件路径")
    
    args = parser.parse_args()
    
    # 验证Excel文件存在
    if not os.path.exists(args.excel):
        print(f"错误: Excel文件不存在: {args.excel}")
        sys.exit(1)
    
    # 解析员工参数
    employee_numbers = parse_employee_numbers(args.employee_numbers)
    
    print(f"开始更新，选项: {args.update_option}, 员工: {employee_numbers}")
    
    # 读取Excel数据
    print("读取Excel数据中...")
    excel_data = convert_excel_to_json(args.excel)
    if not excel_data:
        print("没有成功读取Excel数据，程序退出")
        sys.exit(1)
    
    total_updated = 0
    
    # 根据选项执行更新
    if args.update_option in [1, 3]:  # 更新简历JSON
        print("更新简历JSON文件...")
        resume_updated = update_resume_jsons(employee_numbers, excel_data)
        total_updated += resume_updated
    
    if args.update_option in [2, 3]:  # 更新信息JSON
        print("更新信息JSON文件...")
        info_updated = update_info_jsons(employee_numbers, excel_data)
        total_updated += info_updated
    
    print(f"\n更新完成！共更新 {total_updated} 个文件")

if __name__ == "__main__":
    main()