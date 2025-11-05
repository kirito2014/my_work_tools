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

# 导入项目根目录以便导入其他模块
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(current_file)
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
    
    # 创建输出目录
    output_dir = os.path.join(project_root, "output", bankname)
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有JSON文件
    json_files = [f for f in os.listdir(json_files_dir) if f.endswith('.json')]
    total_files = len(json_files)
    print(f"共发现 {total_files} 个JSON文件")
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
            render_module.process_json_data(
                json_data=json_path,
                template_path=template_path,
                input_file=None,  # 批量生成时不需要input_file
                output_folder=output_dir,
                person_names=person_names,
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
    print(f"批量生成完成！")
    print(f"总JSON文件数: {total_files}")
    print(f"成功生成: {success_count}")
    print(f"生成失败: {failed_count}")
    print(f"输出目录: {output_dir}")
    
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
            json_file = os.path.join(project_root, "output", "modify_json", json_filename)
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
    
    batch_process_resumes(input_folder, template_path, bankname)