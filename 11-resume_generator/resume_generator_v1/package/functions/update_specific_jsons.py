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
    from package.utils.file_helper import read_file, write_file
    # 尝试导入特殊字段处理模块
    try:
        from package.functions.add_special_info import process_directory
        has_special_info_module = True
    except ImportError:
        print("警告: 无法导入add_special_info模块，跳过特殊字段处理")
        process_special_info = None
        has_special_info_module = False
    dj_alter = None
    try:
        from package.functions import doc_2_json_alter as dj_alter
    except ImportError:
        try:
            import package.functions.doc_2_json_alter as dj_alter
        except ImportError:
            dj_alter_file_path = os.path.join(base_dir, 'package', 'functions', 'doc_2_json_alter.py')
            if os.path.exists(dj_alter_file_path):
                import importlib.util
                spec = importlib.util.spec_from_file_location("doc_2_json_alter", dj_alter_file_path)
                dj_alter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(dj_alter)
            else:
                print(f"警告: 未找到 doc_2_json_alter.py 文件在路径: {dj_alter_file_path}")
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

def update_resume_jsons(employee_numbers, excel_data, base_dir="output", word_dir=""):
    """
    更新简历JSON文件
    
    :param employee_numbers: 员工工号列表或"ALL"
    :param excel_data: Excel数据
    :param base_dir: 基础目录
    :param word_dir: Word文档目录（可选）
    :return: 更新的文件数量
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
    
    # 如果提供了Word目录，先从Word文档生成/更新JSON
    if word_dir and os.path.exists(word_dir):
        print(f"\n  - [INFO] 正在从Word文档更新简历JSON...")
        # 遍历Word目录中的文件，找到匹配的工号文件
        for root, _, files in os.walk(word_dir):
            for file in files:
                if file.lower().endswith(('.doc', '.docx')):
                    # 尝试从文件名提取工号
                    file_emp_no = None
                    # 文件名格式：工号+姓名.docx
                    if '+' in file:
                        file_emp_no = file.split('+')[0].strip()
                    elif '_' in file:
                        file_emp_no = file.split('_')[0].strip()
                    
                    # 如果文件名中提取到了工号，并且在目标工号列表中
                    if file_emp_no and file_emp_no.zfill(5) in target_emp_numbers:
                        file_path = os.path.join(root, file)
                        print(f"  - 处理文件: {file}")
                        
                        try:
                            # 动态导入doc_2_json模块
                            import importlib.util
                            dj_file_path = os.path.join(os.path.dirname(__file__), "doc_2_json.py")
                            if os.path.exists(dj_file_path):
                                spec = importlib.util.spec_from_file_location("doc_2_json", dj_file_path)
                                dj = importlib.util.module_from_spec(spec)
                                spec.loader.exec_module(dj)
                                
                                # 处理doc格式文件
                                processed_doc_path = file_path
                                if file_path.lower().endswith('.doc'):
                                    print(f"  - 检测到doc格式文件，正在转换为docx...")
                                    # 动态导入doc_converter模块
                                    dc_file_path = os.path.join(os.path.dirname(__file__), "doc_converter.py")
                                    if os.path.exists(dc_file_path):
                                        spec = importlib.util.spec_from_file_location("doc_converter", dc_file_path)
                                        dc = importlib.util.module_from_spec(spec)
                                        spec.loader.exec_module(dc)
                                        
                                        # 创建临时目录
                                        temp_dir = os.path.join(word_dir, "temp_converted")
                                        os.makedirs(temp_dir, exist_ok=True)
                                        
                                        processed_doc_path = dc.convert_doc_to_docx(file_path, temp_dir)
                                        print(f"  - 转换成功")
                                    else:
                                        print(f"  - [ERROR] 未找到doc_converter模块")
                                        continue
                                
                                # 提取原始数据
                                raw_resume_data = dj.extract_resume_universal(processed_doc_path)
                                print(f"  - 提取成功")
                                if not raw_resume_data:
                                    print(f"  - [ERROR] 文档信息提取失败")
                                    continue
                                
                                # 生成JSON文件名（借鉴batch_render_from_docx中的命名逻辑）
                                base_name = os.path.splitext(os.path.basename(processed_doc_path))[0]
                                
                                # 初始化emp_no默认值
                                emp_no = "unknown"
                                # 从文件名提取工号和姓名
                                parts = base_name.split('+')
                                if len(parts) >= 2:
                                    emp_no = parts[0]
                                    name = parts[1]
                                    json_filename = f"{emp_no}_{name}_人员简历.json"
                                else:
                                    json_filename = f"{base_name}.json"
                                print(f"Processing file: {json_filename}")
                                print(f"Processing file: {emp_no}")

                                # 转换为模板格式
                                result = dj.convert_to_template_format(raw_resume_data, emp_no)
                                if not result:
                                    print("转换失败，请检查文档数据格式")
                                    
                                    continue
                                # 设置JSON文件路径
                                json_file = os.path.join(resume_dir, json_filename)
                                    
                                # 保存JSON文件
                                with open(json_file, 'w', encoding='utf-8') as f:
                                    json.dump(result, f, ensure_ascii=False, indent=4)


                                if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
                                    # 读取并验证JSON数据
                                    with open(json_file, 'r', encoding='utf-8') as f:
                                        saved_json_data = json.load(f)
                                    #print(222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222)
                                    # 检查JSON数据有效性
                                    if not check_json_data_validity(saved_json_data):
                                        print(f"JSON数据验证失败: {json_file}，尝试使用备用方法重新生成")
                                        #print(3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333)
                                        # 如果备用模块存在，尝试使用备用方法
                                        if dj_alter:
                                            try:
                                                # 使用备用方法提取和转换数据
                                                raw_resume_data_alter = dj_alter.extract_resume_alt(processed_doc_path)
                                                if isinstance(raw_resume_data_alter, dict):
                                                    result_alter = dj_alter.convert_to_template_format(raw_resume_data_alter, emp_no)
                                                    if isinstance(result_alter, dict):
                                                    # 重新保存JSON文件
                                                        with open(json_file, 'w', encoding='utf-8') as f:
                                                            json.dump(result_alter, f, ensure_ascii=False, indent=4)
                                                        
                                                        # 再次验证备用生成的JSON数据
                                                        with open(json_file, 'r', encoding='utf-8') as f:
                                                            saved_json_data_alter = json.load(f)
                                                        
                                                            if check_json_data_validity(saved_json_data_alter):
                                                                print(f"备用方法生成JSON成功: {json_file}")
                                                                
                                                            else:
                                                                print(f"备用方法生成的JSON数据仍然无效: {json_file}")
                                                                
                                                                continue
                                                    else:
                                                        print("备用方法转换失败")
                                                        
                                                        continue
                                                else:
                                                    print("备用方法提取数据失败")
                                                    
                                                    continue
                                            except Exception as e:
                                                print(f"备用方法执行异常: {str(e)}")
                                                
                                                continue
                                        else:
                                            print("备用模块不可用")
                                            
                                            continue
                                    else:
                                        # JSON数据验证通过
                                        
                                        print(f"处理完成，JSON文件已保存至: {json_file}")
                                else:
                                    print(f"JSON文件保存失败: {json_file}")
                                    
                                    continue
                                    
                        except Exception as e:
                            print(f"  - [ERROR] 处理文件时出错: {e}")
                            import traceback
                            traceback.print_exc()
   
    # 查找并更新对应的简历JSON文件（使用Excel数据更新AdditionInfo）
    print(f"\n  - [INFO] 正在更新简历JSON的AdditionInfo信息...")
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
                    file_content = read_file(file_path)
                    if not file_content:
                        continue
                    
                    try:
                        # 解析JSON字符串为Python字典
                        resume_data = json.loads(file_content)
                    except json.JSONDecodeError as e:
                        print(f"  - [ERROR] 解析简历文件 {filename} 失败: {e}")
                        continue
                    
                    # 获取员工信息
                    emp_data = emp_map[emp_no]
                    
                    # 更新AdditionInfo
                    if resume_data:
                            # 获取第一个键（通常是姓名）
                            person_name = list(resume_data.keys())[0]
                            if person_name in resume_data:
                                resume_data[person_name]["AdditionInfo"] = emp_data
                                # 将字典转换为JSON字符串并写回文件
                                updated_content = json.dumps(resume_data, ensure_ascii=False, indent=2)
                                write_file(file_path, updated_content)
                                print(f"  - [OK] 已更新简历JSON的AdditionInfo信息: {filename}")
                                
                                # 处理特殊字段信息
                                from add_special_info import process_special_info
                                if has_special_info_module and process_special_info:
                                    try:
                                        if process_special_info(file_path):
                                            print(f"  - [OK] 已更新简历JSON的特殊信息: {filename}")
                                        else:
                                            print(f"  - [WARNING] 特殊字段处理失败: {filename}")
                                    except Exception as si_e:
                                        print(f"  - [ERROR] 特殊字段处理出错: {filename} - {si_e}")
                                
                                updated_count += 1
                                found = True
                                break
                except Exception as e:
                    print(f"  - [ERROR] 更新简历文件 {filename} 时出错: {e}")
        
        if not found:
            print(f"  - [WARNING] 未找到工号 {emp_no} 对应的简历JSON文件")
    
    return updated_count
    
def check_json_data_validity(json_data):
        """验证JSON数据的有效性"""
        if not isinstance(json_data, dict) or len(json_data) == 0:
            return False
            
        person_key = next(iter(json_data.keys()))
        person_data = json_data.get(person_key, {})
        
        # 检查基本信息关键字段
        basic_info = person_data.get('BasicInfo', {})
        required_basic_fields = ['Name', 'EmpNo', 'WorkYears',"GraduationTime","GraduationSchool","Major","HighestEducation","Department","Title","PersonalProfile"]
        if not all(basic_info.get(field) for field in required_basic_fields if field in basic_info):
            return False
        
        # 检查工作经历和项目经历是否有有效条目
        work_experience = person_data.get('WorkExperience', [])
        project_experience = person_data.get('ProjectExperience', [])
        
        # 检查工作经历是否有非空条目
        valid_work_exp = any(exp.get('CompanyName') or exp.get('Position') or exp.get('JobDescription')
                          for exp in work_experience)
        
        # 检查项目经历是否有有效条目
        valid_project_exp = any(proj.get('ProjectName') or proj.get('ProjectRole') or proj.get('JobDescription')
                            for proj in project_experience)
        
        # 至少需要有工作经历或项目经历的有效条目
        return valid_work_exp or valid_project_exp

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
            emp_no = data["AdditionInfo"].get("EmpNo", "").zfill(5)
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
    parser.add_argument("--excel", default=os.path.join("input", "技术人员名单-11月.xlsx"), 
                       help="人员基本信息文件路径")
    parser.add_argument("--word", default="", help="简历文件夹路径")
    
    args = parser.parse_args()
    base_dir="output"
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
        # 当员工编号为ALL且提供了word参数时，使用批量更新
        if employee_numbers == "ALL" and args.word:
            print(f"执行批量更新，使用简历文件夹: {args.word}")
            try:
                # 导入batch_render_from_docx模块
                import importlib.util
                batch_render_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'batch_render_from_docx.py')
                if os.path.exists(batch_render_path):
                    spec = importlib.util.spec_from_file_location("batch_render_from_docx", batch_render_path)
                    batch_render_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(batch_render_module)
                    # 调用批量更新函数
                    batch_render_module.batch_modify_json(args.word, args.excel)
                    print(os.path.join(base_dir, "modify_json"))
                    # 更新specialInfo字段
                    if has_special_info_module and process_directory:
                        process_directory(os.path.join(base_dir, "modify_json"))
                        print("已更新specialInfo字段")
                    print("批量更新完成")
                    total_updated += 1  # 标记执行了批量更新
                else:
                    print(f"错误: 未找到batch_render_from_docx.py文件: {batch_render_path}")
            except Exception as e:
                print(f"执行批量更新时出错: {e}")
        else:
            # 如果提供了word参数，传递给update_resume_jsons函数
            resume_updated = update_resume_jsons(employee_numbers, excel_data, word_dir=args.word)
            total_updated += resume_updated
    
    if args.update_option in [2, 3]:  # 更新信息JSON
        print("更新信息JSON文件...")
        info_updated = update_info_jsons(employee_numbers, excel_data)
        total_updated += info_updated
    
    print(f"\n更新完成！共更新 {total_updated} 个文件")

if __name__ == "__main__":
    main()