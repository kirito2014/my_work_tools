#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量从docx/doc文件生成简历入口脚本
功能：处理指定文件夹中的所有.doc和.docx文件，生成简历文档
"""

import os
import sys
import json
from datetime import datetime

# 处理PyInstaller打包后的路径问题
if getattr(sys, 'frozen', False):
    # 打包后的环境
    base_dir = os.path.dirname(sys.executable)
    # 确保工作目录设置为当前目录（exe所在目录）
    os.chdir(base_dir)
    project_root = base_dir
else:
    # 开发环境
    current_file = os.path.abspath(__file__)
    project_root = os.path.dirname(current_file)
    base_dir = project_root
sys.path.append(project_root)

# 导入doc_converter模块
doc_converter = None
try:
    from package.functions import doc_converter
except ImportError:
    try:
        # 尝试动态加载
        import importlib.util
        converter_path = os.path.join(project_root, "package", "functions", "doc_converter.py")
        if os.path.exists(converter_path):
            spec = importlib.util.spec_from_file_location("doc_converter", converter_path)
            doc_converter = importlib.util.module_from_spec(spec)
            sys.modules["doc_converter"] = doc_converter
            spec.loader.exec_module(doc_converter)
            print(f"通过动态加载成功导入 doc_converter.py 文件")
        else:
            print(f"错误: 未找到 doc_converter.py 文件")
            sys.exit(1)
    except Exception as e:
        print(f"导入 doc_converter 模块失败: {e}")
        sys.exit(1)

# 导入render_from_docx模块
render_module = None
try:
    from package.functions import render_from_docx as render_module
except ImportError:
    try:
        # 尝试动态加载
        import importlib.util
        render_path = os.path.join(project_root, "package", "functions", "render_from_docx.py")
        if os.path.exists(render_path):
            spec = importlib.util.spec_from_file_location("render_from_docx", render_path)
            render_module = importlib.util.module_from_spec(spec)
            sys.modules["render_from_docx"] = render_module
            spec.loader.exec_module(render_module)
            print(f"通过动态加载成功导入 render_from_docx.py 文件")
        else:
            print(f"错误: 未找到 render_from_docx.py 文件")
            sys.exit(1)
    except Exception as e:
        print(f"导入 render_from_docx 模块失败: {e}")
        sys.exit(1)

# 导入doc_2_json模块作为dj
dj = None
try:
    from package.functions import doc_2_json as dj
except ImportError:
    try:
        # 尝试动态加载
        import importlib.util
        dj_path = os.path.join(project_root, "package", "functions", "doc_2_json.py")
        if os.path.exists(dj_path):
            spec = importlib.util.spec_from_file_location("doc_2_json", dj_path)
            dj = importlib.util.module_from_spec(spec)
            sys.modules["doc_2_json"] = dj
            spec.loader.exec_module(dj)
            print(f"通过动态加载成功导入 doc_2_json.py 文件")
        else:
            print(f"错误: 未找到 doc_2_json.py 文件")
            sys.exit(1)
    except Exception as e:
        print(f"导入 doc_2_json 模块失败: {e}")
        sys.exit(1)


def batch_generate_resumes(json_files_dir, template_path, bankname, person_names="all"):
    """
    批量生成简历功能
    
    :param json_files_dir: 包含JSON文件的目录路径
    :param template_path: Word模板文件路径
    :param bankname: 银行名称
    :param person_names: 要生成简历的人员名单，默认为"all"表示全部生成
    """
    # 检查输入参数
    if not os.path.isdir(json_files_dir):
        print(f"错误: JSON文件目录 '{json_files_dir}' 不存在")
        return 0, 0
    
    if not os.path.isfile(template_path):
        print(f"错误: 模板文件 '{template_path}' 不存在")
        return 0, 0
    
    # 创建输出目录，使用base_dir确保在打包环境中正确
    output_dir = os.path.join(os.getcwd(), "output", bankname)
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有JSON文件
    all_json_files = [f for f in os.listdir(json_files_dir) if f.endswith('.json')]
    
    # 根据person_names过滤JSON文件
    if person_names != "all":
        # 假设JSON文件名格式为"工号_姓名_人员简历.json"
        # 提取person_names中的工号（如果包含工号格式）
        filtered_json_files = []
        for json_file in all_json_files:
            # 从文件名中提取工号（通常是文件名的第一部分）
            file_prefix = json_file.split('_')[0]
            # 检查是否包含在person_names中
            if file_prefix in person_names:
                filtered_json_files.append(json_file)
        json_files = filtered_json_files
        print(f"根据指定名单过滤后，共发现 {len(json_files)} 个匹配的JSON文件")
    else:
        json_files = all_json_files
        print(f"共发现 {len(json_files)} 个JSON文件")
    
    total_files = len(json_files)
    print("=" * 50)
    
    # 统计信息
    success_count = 0
    failed_count = 0
    
    # 处理每个JSON文件
    for index, json_file in enumerate(json_files, 1):
        json_path = os.path.join(json_files_dir, json_file)
        print(f"\n[{index}/{total_files}] 正在处理JSON文件: {json_file}")
        
        try:
            # 调用render_from_docx模块处理文件
            # 由于我们已经根据工号过滤了JSON文件，处理文件时应该处理其中的所有人员
            render_module.process_json_data(
                json_data=json_path,
                template_path=template_path,
                input_file=None,  # 批量生成时不需要input_file
                output_folder=output_dir,
                person_names="all",  # 处理文件中的所有人员
                bankname=bankname
            )
            success_count += 1
            print(f"  - 处理完成")
        except Exception as e:
            print(f"  - 处理失败: {e}")
            failed_count += 1
            import traceback
            traceback.print_exc()
    
    # 输出统计信息
    print("\n" + "=" * 50)
    print(f"批量生成简历完成！")
    print(f"总简历JSON文件数: {total_files}")
    print(f"成功生成简历: {success_count}")
    print(f"生成失败简历: {failed_count}")
    print(f"简历输出目录: {output_dir}")
    
    return success_count, failed_count

def batch_process_resumes(input_folder, template_path, bankname):
    """
    批量处理文件夹中的所有简历文件（仅处理，不生成）
    
    :param input_folder: 包含简历文件的输入文件夹路径
    :param template_path: Word模板文件路径
    :param bankname: 银行名称
    """
    # 检查输入参数
    if not os.path.isdir(input_folder):
        print(f"错误: 输入路径 '{input_folder}' 不是有效的文件夹")
        sys.exit(1)
    
    if not os.path.isfile(template_path):
        print(f"错误: 模板文件 '{template_path}' 不存在")
        sys.exit(1)
    
    # 创建temp_converted目录
    temp_dir = os.path.join(input_folder, "temp_converted")
    os.makedirs(temp_dir, exist_ok=True)
    
    # 统计信息
    total_files = 0
    processed_files = 0
    failed_files = 0
    converted_files = 0
    
    # 遍历文件夹中的所有.doc和.docx文件
    resume_files = []
    for root, dirs, files in os.walk(input_folder):
        # 跳过temp_converted目录
        if "temp_converted" in dirs:
            dirs.remove("temp_converted")
        
        for file in files:
            if file.lower().endswith(('.doc', '.docx')):
                # 跳过临时文件
                if file.startswith('~$'):
                    continue
                
                file_path = os.path.join(root, file)
                resume_files.append(file_path)
    
    total_files = len(resume_files)
    print(f"共发现 {total_files} 个简历文件")
    print("=" * 50)
    
    # 处理每个文件
    for index, file_path in enumerate(resume_files, 1):
        print(f"\n[{index}/{total_files}] 正在处理文件: {os.path.basename(file_path)}")
        
        try:
            # 处理doc格式文件
            processed_doc_path = file_path
            is_converted = False
            
            if file_path.lower().endswith('.doc'):
                print(f"  - 检测到doc格式文件，正在转换为docx...")
                try:
                    processed_doc_path = doc_converter.convert_doc_to_docx(file_path, temp_dir)
                    converted_files += 1
                    is_converted = True
                    print(f"  - 转换成功: {os.path.basename(processed_doc_path)}")
                except Exception as e:
                    print(f"  - 转换失败: {e}")
                    failed_files += 1
                    continue
            
            # 生成JSON文件名
            base_name = os.path.splitext(os.path.basename(processed_doc_path))[0]
            # 移除"_已转换"后缀
            if is_converted and "_已转换" in base_name:
                base_name = base_name.replace("_已转换", "")
            
            # 从文件名提取工号和姓名
            parts = base_name.split('+')
            if len(parts) >= 2:
                emp_no = parts[0]
                name = parts[1]
                json_filename = f"{emp_no}_{name}_人员简历.json"
            else:
                json_filename = f"{base_name}.json"
            
            # 设置JSON文件路径
            json_file = os.path.join(base_dir, "output", "modify_json", json_filename)
            os.makedirs(os.path.dirname(json_file), exist_ok=True)
            
            processed_files += 1
            print(f"  - 文件处理完成（仅转换，未生成简历）")
            
        except Exception as e:
            print(f"  - 处理失败: {e}")
            failed_files += 1
            import traceback
            traceback.print_exc()
    
    # 输出统计信息
    print("\n" + "=" * 50)
    print(f"批处理完成！")
    print(f"总文件数: {total_files}")
    print(f"成功处理: {processed_files}")
    print(f"转换文件数: {converted_files}")
    print(f"处理失败: {failed_files}")
    print(f"JSON目录: {os.path.join(project_root, 'output', 'modify_json')}")

def batch_modify_json(input_folder, excel_file=None):
    """
    批量从Word文档生成/更新JSON文件
    
    :param input_folder: 包含简历Word文档的文件夹路径
    :param excel_file: 可选的Excel文件路径，用于更新JSON中的人员基本信息
    """
    # 检查输入参数
    if not os.path.isdir(input_folder):
        print(f"错误: 输入路径 '{input_folder}' 不是有效的文件夹")
        return
    
    # 创建temp_converted目录
    temp_dir = os.path.join(input_folder, "temp_converted")
    os.makedirs(temp_dir, exist_ok=True)
    
    # 创建output/modify_json目录，使用base_dir确保在打包环境中正确
    modify_json_dir = os.path.join(os.getcwd(), "output", "modify_json")
    os.makedirs(modify_json_dir, exist_ok=True)
    
    # 统计信息
    total_files = 0
    processed_files = 0
    failed_files = 0
    converted_files = 0
    
    # 遍历文件夹中的所有.doc和.docx文件
    resume_files = []
    for root, dirs, files in os.walk(input_folder):
        # 跳过temp_converted目录
        if "temp_converted" in dirs:
            dirs.remove("temp_converted")
        
        for file in files:
            if file.lower().endswith(('.doc', '.docx')):
                # 跳过临时文件
                if file.startswith('~$'):
                    continue
                
                file_path = os.path.join(root, file)
                resume_files.append(file_path)
    
    total_files = len(resume_files)
    print(f"共发现 {total_files} 个简历文件")
    print("=" * 50)
    
    # 处理每个文件
    for index, file_path in enumerate(resume_files, 1):
        print(f"\n[{index}/{total_files}] 正在处理文件: {os.path.basename(file_path)}")
        
        try:
            # 处理doc格式文件
            processed_doc_path = file_path
            is_converted = False
            
            if file_path.lower().endswith('.doc'):
                print(f"  - 检测到doc格式文件，正在转换为docx...")
                try:
                    processed_doc_path = doc_converter.convert_doc_to_docx(file_path, temp_dir)
                    converted_files += 1
                    is_converted = True
                    print(f"  - 转换成功: {os.path.basename(processed_doc_path)}")
                except Exception as e:
                    print(f"  - 转换失败: {e}")
                    failed_files += 1
                    continue
            
            # 提取原始数据
            print(f"  - 正在从文档提取简历信息...")
            raw_resume_data = dj.extract_resume_universal(processed_doc_path)
            if not raw_resume_data:
                print(f"  - [ERROR] 文档信息提取失败")
                failed_files += 1
                continue
            
            # 从文件名提取工号
            emp_no = dj.extract_emp_no_from_filename(file_path)
            print(f"  - 提取工号: {emp_no}")
            
            # 转换为模板格式
            template_data = dj.convert_to_template_format(raw_resume_data, emp_no)
            if not template_data:
                print(f"  - [ERROR] 数据转换失败")
                failed_files += 1
                continue
            
            # 生成JSON文件名
            base_name = os.path.splitext(os.path.basename(processed_doc_path))[0]
            # 移除"_已转换"后缀
            if is_converted and "_已转换" in base_name:
                base_name = base_name.replace("_已转换", "")
            
            # 从文件名提取工号和姓名（如果可能）
            parts = base_name.split('+')
            if len(parts) >= 2:
                emp_no = parts[0]
                name = parts[1]
                json_filename = f"{emp_no}_{name}_人员简历.json"
            else:
                # 尝试从template_data中获取姓名
                if template_data:
                    name = list(template_data.keys())[0]
                    json_filename = f"{emp_no}_{name}_人员简历.json"
                else:
                    json_filename = f"{base_name}_人员简历.json"
            
            # 设置JSON文件路径
            json_file = os.path.join(modify_json_dir, json_filename)
            
            # 保存JSON文件
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(template_data, f, ensure_ascii=False, indent=4)
            
            processed_files += 1
            print(f"  - [OK] 成功生成JSON文件: {json_filename}")
            
        except Exception as e:
            print(f"  - [ERROR] 处理失败: {e}")
            failed_files += 1
            import traceback
            traceback.print_exc()
    
    # 输出统计信息
    print("\n" + "=" * 50)
    print(f"批量更新JSON完成！")
    print(f"总文件数: {total_files}")
    print(f"成功处理: {processed_files}")
    print(f"转换文件数: {converted_files}")
    print(f"处理失败: {failed_files}")
    print(f"JSON目录: {modify_json_dir}")
    
    # 如果提供了Excel文件，更新JSON中的AdditionInfo
    if excel_file and os.path.exists(excel_file):
        print(f"\n正在从Excel文件更新JSON中的人员基本信息...")
        try:
            # 动态导入Excel处理模块
            from package.functions.excel_2_info_json import convert_excel_to_json
            from package.utils.file_helper import read_file, write_file
            
            # 读取Excel数据
            excel_data = convert_excel_to_json(excel_file)
            if not excel_data:
                print("  - [ERROR] 未成功读取Excel数据")
            else:
                # 创建工号到员工数据的映射
                emp_map = {emp.get("EmpNo", "").zfill(5): emp for emp in excel_data}
                
                # 更新每个JSON文件的AdditionInfo
                updated_count = 0
                for filename in os.listdir(modify_json_dir):
                    if filename.endswith(".json"):
                        # 尝试从文件名提取工号
                        emp_no = filename.split('_')[0]
                        if emp_no in emp_map:
                            file_path = os.path.join(modify_json_dir, filename)
                            try:
                                # 读取现有简历文件
                                file_content = read_file(file_path)
                                if file_content:
                                    resume_data = json.loads(file_content)
                                    # 更新AdditionInfo
                                    if resume_data:
                                        person_name = list(resume_data.keys())[0]
                                        resume_data[person_name]["AdditionInfo"] = emp_map[emp_no]
                                        # 写回文件
                                        updated_content = json.dumps(resume_data, ensure_ascii=False, indent=4)
                                        write_file(file_path, updated_content)
                                        updated_count += 1
                            except Exception as e:
                                print(f"  - 更新文件 {filename} 时出错: {e}")
                
                print(f"  - [OK] 已更新 {updated_count} 个JSON文件的人员基本信息")
        except Exception as e:
            print(f"  - [ERROR] 更新Excel信息时出错: {e}")
    

if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) < 4:
        print("用法: python batch_render_from_docx.py <简历文件夹路径> <模板文件路径> <银行名称>")
        print("\n示例:")
        print("  python batch_render_from_docx.py input template/人员简历_模板.docx 测试银行")
        sys.exit(1)
    
    input_folder = sys.argv[1]
    template_path = sys.argv[2]
    bankname = sys.argv[3]
    
    # 转换为绝对路径
    input_folder = os.path.abspath(input_folder)
    template_path = os.path.abspath(template_path)
    
    print(f"开始批处理简历...")
    print(f"输入文件夹: {input_folder}")
    print(f"模板文件: {template_path}")
    print(f"银行名称: {bankname}")
    
    # 首先处理文件（转换doc到docx）
    batch_process_resumes(input_folder, template_path, bankname)
    
    # 然后批量生成简历
    modify_json_dir = os.path.join(base_dir, 'output', 'modify_json')
    print(f"\n开始批量生成简历...")
    batch_generate_resumes(modify_json_dir, template_path, bankname)