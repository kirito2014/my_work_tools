#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel模板渲染模块
用于处理xlsx格式的简历模板，支持每个人员创建单独的sheet页
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

try:
    import openpyxl
    from openpyxl import Workbook
    from openpyxl.utils import get_column_letter
    from openpyxl.styles import Font, Alignment, PatternFill
except ImportError:
    print("错误: 未安装openpyxl库，请运行: pip install openpyxl")
    sys.exit(1)


def get_base_dir():
    """获取基础目录，兼容PyInstaller打包环境"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def sanitize_data(data):
    """清理数据以避免类型错误"""
    if isinstance(data, dict):
        return {key: sanitize_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    elif isinstance(data, str):
        # 尝试将数字字符串转换为数字类型
        try:
            if '.' in data:
                return float(data)
            else:
                return int(data)
        except ValueError:
            return data
    return data


def find_placeholder_cells(worksheet) -> Dict[str, str]:
    """
    查找工作表中的占位符单元格
    
    Args:
        worksheet: openpyxl工作表对象
        
    Returns:
        Dict[str, str]: 占位符映射，键为占位符名称，值为单元格位置
    """
    placeholders = {}
    
    for row in worksheet.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str):
                # 查找 {{placeholder}} 格式的占位符
                if '{{' in cell.value and '}}' in cell.value:
                    # 提取占位符名称
                    start = cell.value.find('{{') + 2
                    end = cell.value.find('}}')
                    if start > 1 and end > start:
                        placeholder_name = cell.value[start:end].strip()
                        placeholders[placeholder_name] = cell.coordinate
    
    return placeholders


def render_template_cell(cell, value):
    """
    渲染模板单元格
    
    Args:
        cell: openpyxl单元格对象
        value: 要填充的值
    """
    if value is None:
        cell.value = ""
    elif isinstance(value, (list, dict)):
        # 处理复杂数据类型
        cell.value = json.dumps(value, ensure_ascii=False, indent=2)
    else:
        cell.value = str(value)


def create_person_sheet(template_worksheet, person_name: str, person_data: Dict[str, Any]) -> Any:
    """
    为指定人员创建新的工作表
    
    Args:
        template_worksheet: 模板工作表
        person_name: 人员姓名
        person_data: 人员数据
        
    Returns:
        新创建的工作表对象
    """
    # 创建新工作簿
    new_workbook = Workbook()
    # 删除默认创建的工作表
    new_workbook.remove(new_workbook.active)
    
    # 创建新工作表，命名为"人员名称_简历"
    new_sheet = new_workbook.create_sheet(title=f"{person_name}_简历")
    
    # 复制模板工作表的内容和格式
    for row in template_worksheet.iter_rows():
        for cell in row:
            new_cell = new_sheet.cell(row=cell.row, column=cell.column)
            
            # 复制值
            new_cell.value = cell.value
            
            # 复制样式（简化版本）
            if cell.has_style:
                try:
                    if cell.font:
                        new_cell.font = cell.font.copy()
                    if cell.border:
                        new_cell.border = cell.border.copy()
                    if cell.fill:
                        new_cell.fill = cell.fill.copy()
                    new_cell.number_format = cell.number_format
                    if cell.protection:
                        new_cell.protection = cell.protection.copy()
                    if cell.alignment:
                        new_cell.alignment = cell.alignment.copy()
                except Exception as e:
                    print(f"复制样式时出错: {e}")
                    # 如果样式复制失败，至少复制值
                    pass
    
    # 查找占位符并替换
    placeholders = find_placeholder_cells(new_sheet)
    sanitized_data = sanitize_data(person_data)
    
    for placeholder, cell_coord in placeholders.items():
        cell = new_sheet[cell_coord]
        
        # 支持嵌套键值，如 "personal_info.name"
        keys = placeholder.split('.')
        value = sanitized_data
        
        try:
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                elif isinstance(value, list) and key.isdigit():
                    idx = int(key)
                    if 0 <= idx < len(value):
                        value = value[idx]
                    else:
                        value = None
                        break
                else:
                    value = None
                    break
            
            render_template_cell(cell, value)
        except Exception as e:
            print(f"渲染占位符 {placeholder} 时出错: {str(e)}")
            render_template_cell(cell, f"[错误: {placeholder}]")
    
    return new_sheet, new_workbook


def generate_resume_from_excel(person_data: Dict[str, Any], template_path: str, 
                              output_path: str) -> Optional[str]:
    """
    从Excel模板生成单个简历（简化版本，用于测试）
    
    Args:
        person_data: 人员数据字典
        template_path: Excel模板文件路径
        output_path: 输出文件路径
        
    Returns:
        生成的文件路径，失败时返回None
    """
    try:
        # 检查模板文件是否存在
        if not os.path.exists(template_path):
            print(f"错误: Excel模板文件不存在: {template_path}")
            return None
        
        # 加载Excel模板
        print(f"正在加载Excel模板: {template_path}")
        template_workbook = openpyxl.load_workbook(template_path)
        
        # 查找模板工作表（假设名为"XX_简历"）
        template_sheet = None
        for sheet_name in template_workbook.sheetnames:
            if sheet_name.endswith('XX简历'):
                template_sheet = template_workbook[sheet_name]
                break
        
        if template_sheet is None:
            # 如果没有找到"_简历"结尾的工作表，使用第一个工作表
            if template_workbook.sheetnames:
                template_sheet = template_workbook.active
                print(f"警告: 未找到'XX简历'结尾的工作表，使用第一个工作表: {template_sheet.title}")
            else:
                print("错误: Excel文件中没有工作表")
                return None
        
        # 获取人员姓名
        person_name = "未知人员"
        if isinstance(person_data, dict):
            # 尝试多种可能的姓名字段
            name_info = person_data.get('personal_info', {})
            if not isinstance(name_info, dict):
                name_info = person_data.get('AdditionInfo', {})
            if not isinstance(name_info, dict):
                name_info = person_data
            
            if isinstance(name_info, dict):
                person_name = name_info.get('name', name_info.get('Name', '未知人员'))
        
        # 创建人员专用工作表
        new_sheet, new_workbook = create_person_sheet(template_sheet, person_name, person_data)
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 如果文件已存在，则删除
        if os.path.exists(output_path):
            os.remove(output_path)
        
        # 保存生成的Excel文件
        new_workbook.save(output_path)
        print(f"{person_name} 简历已生成 (Excel格式)，保存到: {os.path.abspath(output_path)}")
        
        return output_path
        
    except Exception as e:
        print(f"生成Excel简历时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def copy_worksheet_with_formatting(source_worksheet, target_worksheet, target_workbook):
    """
    完整复制工作表，包括所有格式、合并单元格等
    
    Args:
        source_worksheet: 源工作表
        target_worksheet: 目标工作表
        target_workbook: 目标工作簿
    """
    # 复制所有单元格的值和格式
    for row in source_worksheet.iter_rows():
        for cell in row:
            target_cell = target_worksheet.cell(row=cell.row, column=cell.column)
            
            # 复制值
            target_cell.value = cell.value
            
            # 完整复制样式
            if cell.has_style:
                try:
                    if cell.font:
                        target_cell.font = cell.font.copy()
                    if cell.border:
                        target_cell.border = cell.border.copy()
                    if cell.fill:
                        target_cell.fill = cell.fill.copy()
                    if cell.alignment:
                        target_cell.alignment = cell.alignment.copy()
                    if cell.number_format:
                        target_cell.number_format = cell.number_format
                    if cell.protection:
                        target_cell.protection = cell.protection.copy()
                except Exception as e:
                    print(f"复制样式时出错: {e}")
    
    # 复制合并单元格
    if source_worksheet.merged_cells:
        for merged_range in source_worksheet.merged_cells.ranges:
            try:
                target_worksheet.merge_cells(str(merged_range))
            except Exception as e:
                print(f"复制合并单元格时出错: {e}")
    
    # 复制行高和列宽
    for row_num in range(1, source_worksheet.max_row + 1):
        if source_worksheet.row_dimensions[row_num].height:
            target_worksheet.row_dimensions[row_num].height = source_worksheet.row_dimensions[row_num].height
    
    for col_num in range(1, source_worksheet.max_column + 1):
        col_letter = get_column_letter(col_num)
        if source_worksheet.column_dimensions[col_letter].width:
            target_worksheet.column_dimensions[col_letter].width = source_worksheet.column_dimensions[col_letter].width
    
    # 复制工作表保护设置
    if source_worksheet.protection:
        target_worksheet.protection = source_worksheet.protection


def create_person_sheet_with_formatting(template_worksheet, person_name: str, person_data: Dict[str, Any], target_workbook) -> Any:
    """
    为指定人员创建新的工作表（完整格式保留版本）
    
    Args:
        template_worksheet: 模板工作表
        person_name: 人员姓名
        person_data: 人员数据
        target_workbook: 目标工作簿
        
    Returns:
        新创建的工作表对象
    """
    # 创建新工作表，命名为"人员名称_简历"
    new_sheet = target_workbook.create_sheet(title=f"{person_name}_简历")
    
    # 完整复制模板工作表的内容和格式
    copy_worksheet_with_formatting(template_worksheet, new_sheet, target_workbook)
    
    # 查找占位符并替换
    placeholders = find_placeholder_cells(new_sheet)
    sanitized_data = sanitize_data(person_data)
    
    for placeholder, cell_coord in placeholders.items():
        cell = new_sheet[cell_coord]
        
        # 支持嵌套键值，如 "personal_info.name"
        keys = placeholder.split('.')
        value = sanitized_data
        
        try:
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                elif isinstance(value, list) and key.isdigit():
                    idx = int(key)
                    if 0 <= idx < len(value):
                        value = value[idx]
                    else:
                        value = None
                        break
                else:
                    value = None
                    break
            
            render_template_cell(cell, value)
        except Exception as e:
            print(f"渲染占位符 {placeholder} 时出错: {str(e)}")
            render_template_cell(cell, f"[错误: {placeholder}]")
    
    return new_sheet


def generate_multiple_resumes_in_one_file(person_data_list: List[Dict[str, Any]], 
                                       template_path: str, output_path: str) -> Optional[str]:
    """
    从Excel模板生成多个人员的简历（所有人员在同一个文件的不同sheet页中）
    
    Args:
        person_data_list: 人员数据列表，每个元素为(人员姓名, 人员数据)的元组
        template_path: Excel模板文件路径
        output_path: 输出文件路径
        
    Returns:
        生成的文件路径，失败时返回None
    """
    try:
        # 检查模板文件是否存在
        if not os.path.exists(template_path):
            print(f"错误: Excel模板文件不存在: {template_path}")
            return None
        
        # 加载Excel模板
        print(f"正在加载Excel模板: {template_path}")
        template_workbook = openpyxl.load_workbook(template_path)
        
        # 查找模板工作表
        template_sheet = None
        for sheet_name in template_workbook.sheetnames:
            if sheet_name.endswith('XX简历'):
                template_sheet = template_workbook[sheet_name]
                break
        
        if template_sheet is None:
            # 如果没有找到"_简历"结尾的工作表，使用第一个工作表
            if template_workbook.sheetnames:
                template_sheet = template_workbook.active
                print(f"警告: 未找到'XX简历'结尾的工作表，使用第一个工作表: {template_sheet.title}")
            else:
                print("错误: Excel文件中没有工作表")
                return None
        
        # 创建新工作簿（基于模板）
        new_workbook = Workbook()
        # 删除默认创建的工作表
        new_workbook.remove(new_workbook.active)
        
        # 首先复制模板中的所有非简历工作表（如果有）
        for sheet_name in template_workbook.sheetnames:
            if not sheet_name.endswith('XX简历'):
                template_sheet_to_copy = template_workbook[sheet_name]
                new_sheet = new_workbook.create_sheet(title=sheet_name)
                copy_worksheet_with_formatting(template_sheet_to_copy, new_sheet, new_workbook)
        
        # 为每个人员创建工作表
        for person_name, person_data in person_data_list:
            print(f"正在为 {person_name} 创建工作表...")
            create_person_sheet_with_formatting(template_sheet, person_name, person_data, new_workbook)
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 如果文件已存在，则删除
        if os.path.exists(output_path):
            os.remove(output_path)
        
        # 保存生成的Excel文件
        new_workbook.save(output_path)
        print(f"多人简历已生成到单一文件 (Excel格式)，保存到: {os.path.abspath(output_path)}")
        
        return output_path
        
    except Exception as e:
        print(f"生成多人Excel简历时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def generate_multiple_resumes_from_excel(person_data_list: List[Dict[str, Any]], 
                                       template_path: str, output_path: str) -> Optional[str]:
    """
    从Excel模板生成多个人员的简历（每个人员一个sheet页）- 保留原有接口
    """
    # 转换数据格式
    formatted_data_list = []
    for i, person_data in enumerate(person_data_list):
        # 获取人员姓名
        person_name = f"人员{i+1}"
        if isinstance(person_data, dict):
            name_info = person_data.get('personal_info', {})
            if isinstance(name_info, dict):
                person_name = name_info.get('name', f"人员{i+1}")
            else:
                # 尝试直接获取姓名字段
                person_name = person_data.get('姓名', f"人员{i+1}")
        
        formatted_data_list.append((person_name, person_data))
    
    return generate_multiple_resumes_in_one_file(formatted_data_list, template_path, output_path)


def generate_resume_from_excel_original(person_data: Dict[str, Any], template_path: str, 
                              output_folder: str, person_name: str, 
                              bankname: str) -> Optional[str]:
    """
    从Excel模板生成简历（原始版本，兼容性）
    
    Args:
        person_data: 人员数据字典
        template_path: Excel模板文件路径
        output_folder: 输出文件夹路径
        person_name: 人员姓名
        bankname: 银行名称
        
    Returns:
        生成的文件路径，失败时返回None
    """
    try:
        # 检查模板文件是否存在
        if not os.path.exists(template_path):
            print(f"错误: Excel模板文件不存在: {template_path}")
            return None
        
        # 加载Excel模板
        print(f"正在加载Excel模板: {template_path}")
        template_workbook = openpyxl.load_workbook(template_path)
        
        # 查找模板工作表（假设名为"XX_简历"）
        template_sheet = None
        for sheet_name in template_workbook.sheetnames:
            if sheet_name.endswith('XX简历'):
                template_sheet = template_workbook[sheet_name]
                break
        
        if template_sheet is None:
            # 如果没有找到"_简历"结尾的工作表，使用第一个工作表
            if template_workbook.sheetnames:
                template_sheet = template_workbook.active
                print(f"警告: 未找到'XX简历'结尾的工作表，使用第一个工作表: {template_sheet.title}")
            else:
                print("错误: Excel文件中没有工作表")
                return None
        
        # 创建人员专用工作表
        new_sheet, new_workbook = create_person_sheet(template_sheet, person_name, person_data)
        
        # 生成文件名
        current_date = datetime.now().strftime("%Y%m%d")
        output_filename = f"{bankname}人员简历_{person_name}_{current_date}.xlsx"
        output_path = os.path.join(output_folder, output_filename)
        
        # 确保输出目录存在
        os.makedirs(output_folder, exist_ok=True)
        
        # 如果文件已存在，则删除
        if os.path.exists(output_path):
            os.remove(output_path)
        
        # 保存生成的Excel文件
        new_workbook.save(output_path)
        print(f"{person_name} 简历已生成 (Excel格式)，保存到: {os.path.abspath(output_path)}")
        
        # 特殊处理：如果在PyInstaller临时目录中，提示用户文件实际位置
        if getattr(sys, 'frozen', False) and 'TEMP' in output_path.upper():
            actual_output_path = os.path.join(os.getcwd(), 'output', bankname, os.path.basename(output_path))
            print(f"[注意] 实际输出位置: {actual_output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"{person_name} 生成Excel简历时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def batch_generate_resumes_excel(json_files_dir: str, template_path: str, 
                               bankname: str, person_names: List[str] = None, 
                               merge_to_single_file: bool = True) -> tuple:
    """
    批量生成Excel格式的简历
    
    Args:
        json_files_dir: JSON文件目录
        template_path: Excel模板文件路径
        bankname: 银行名称
        person_names: 要处理的人员列表，为None时处理所有人员
        merge_to_single_file: 是否合并到单个文件（默认True）
        
    Returns:
        tuple: (成功数量, 失败数量)
    """
    success_count = 0
    failed_count = 0
    
    try:
        # 设置输出目录
        output_folder = os.path.join(get_base_dir(), "output", bankname)
        os.makedirs(output_folder, exist_ok=True)
        
        # 获取所有JSON文件
        if not os.path.exists(json_files_dir):
            print(f"错误: JSON文件目录不存在: {json_files_dir}")
            return 0, 0
        
        json_files = [f for f in os.listdir(json_files_dir) if f.endswith('.json')]
        
        if not json_files:
            print("错误: 未找到JSON文件")
            return 0, 0
        
        print(f"找到 {len(json_files)} 个JSON文件")
        
        if merge_to_single_file:
            # 使用新的合并文件功能
            all_person_data = []
            
            # 收集所有人员数据
            for json_file in json_files:
                json_path = os.path.join(json_files_dir, json_file)
                
                try:
                    # 读取JSON数据
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data:
                        print(f"警告: JSON文件为空: {json_file}")
                        continue
                    
                    # 处理文件中的每个人员
                    for person_name in data.keys():
                        # 如果指定了人员列表，只处理列表中的人员
                        if person_names and person_name not in person_names:
                            continue
                        
                        person_data = data[person_name]
                        all_person_data.append((person_name, person_data))
                        
                except Exception as e:
                    print(f"处理JSON文件 {json_file} 时出错: {str(e)}")
                    failed_count += 1
            
            # 生成输出文件名
            template_name = os.path.splitext(os.path.basename(template_path))[0]
            output_filename = f"{bankname}_批量简历_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            output_path = os.path.join(output_folder, output_filename)
            
            # 生成合并的Excel文件
            result = generate_multiple_resumes_in_one_file(all_person_data, template_path, output_path)
            
            if result:
                success_count = len(all_person_data)
                print(f"\nExcel批量生成完成！成功: {success_count}, 失败: {failed_count}")
                print(f"所有简历已合并到单一文件: {output_path}")
            else:
                failed_count = len(all_person_data)
                print("\n批量生成失败")
                
        else:
            # 原有的单独文件生成方式
            for json_file in json_files:
                json_path = os.path.join(json_files_dir, json_file)
                
                try:
                    # 读取JSON数据
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data:
                        print(f"警告: JSON文件为空: {json_file}")
                        continue
                    
                    # 处理JSON文件中的数据
                    # 当前JSON文件结构是 {"AdditionInfo": {...}}
                    # 需要提取人员姓名和数据
                    if isinstance(data, dict):
                        # 遍历所有键（如"AdditionInfo"）
                        for key in data.keys():
                            person_data = data[key]
                            if isinstance(person_data, dict):
                                # 尝试获取人员姓名
                                name_info = person_data.get('Name', person_data.get('name', key))
                                person_name = name_info
                                
                                # 如果指定了人员列表，只处理列表中的人员
                                if person_names and person_name not in person_names:
                                    continue
                                
                                # 生成简历
                                current_date = datetime.now().strftime("%Y%m%d")
                                output_filename = f"{bankname}人员简历_{person_name}_{current_date}.xlsx"
                                output_path = os.path.join(output_folder, output_filename)
                                
                                result = generate_resume_from_excel(
                                    person_data, template_path, output_path
                                )
                                
                                if result:
                                    success_count += 1
                                else:
                                    failed_count += 1
                            
                except Exception as e:
                    print(f"处理JSON文件 {json_file} 时出错: {str(e)}")
                    failed_count += 1
            
            print(f"\nExcel批量生成完成！成功: {success_count}, 失败: {failed_count}")
        
        return success_count, failed_count
        
    except Exception as e:
        print(f"批量生成Excel简历时出错: {str(e)}")
        return success_count, failed_count


if __name__ == "__main__":
    # 测试代码
    if len(sys.argv) < 4:
        print("用法: python render_2_excel.py <JSON文件路径> <Excel模板路径> <银行名称>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    template_file = sys.argv[2]
    bankname = sys.argv[3]
    
    # 读取JSON数据
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 设置输出目录
    output_dir = os.path.join(get_base_dir(), "output", bankname)
    
    # 处理所有人员
    for person_name, person_data in data.items():
        generate_resume_from_excel(
            person_data, template_file, output_dir, person_name, bankname
        )