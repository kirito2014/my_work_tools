#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel模板渲染模块
用于处理xlsx格式的简历模板，支持每个人员创建单独的sheet页
"""

import os
import sys
import json
import copy
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

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


def copy_workbook(source_workbook):
    """
    完整复制工作簿，包括所有工作表、格式和样式
    
    Args:
        source_workbook: 源工作簿对象
        
    Returns:
        复制后的新工作簿对象
    """
    try:
        # 创建新工作簿
        new_workbook = Workbook()
        # 删除默认创建的工作表
        new_workbook.remove(new_workbook.active)
        
        # 复制所有工作表
        for sheet_name in source_workbook.sheetnames:
            source_sheet = source_workbook[sheet_name]
            new_sheet = new_workbook.create_sheet(title=sheet_name)
            copy_worksheet_with_formatting(source_sheet, new_sheet, new_workbook)
        
        return new_workbook
    except Exception as e:
        print(f"复制工作簿时出错: {str(e)}")
        # 如果复制失败，返回空工作簿
        return Workbook()


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


def extract_person_summary_info(person_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    从人员数据中提取首页需要显示的基本信息
    
    Args:
        person_data: 人员数据字典
        
    Returns:
        包含首页信息的字典
    """
    summary_info = {}
    
    try:
        # 提取基本信息
        ai_info = person_data.get('ai', {})
        bi_info = person_data.get('bi', {})
        if isinstance(ai_info, dict):
            summary_info['姓名'] = ai_info.get('name', '')
            summary_info['最高学历'] = ai_info.get('edu', '')
            summary_info['毕业院校'] = ai_info.get('school', '')


        if isinstance(bi_info, dict):
            summary_info['工作年限'] = bi_info.get('work', '')

        # 提取项目经验中的项目名称
        pe = person_data.get('pe', [])
        if isinstance(pe, list):
            project_names = []
            for project in pe:
                if isinstance(project, dict):
                    project_name = project.get('proj', '')
                    if project_name:
                        project_names.append(project_name)
            summary_info['项目名称'] = ', '.join(project_names)
        elif isinstance(pe, dict):
            # 如果是字典格式，尝试获取项目名称
            project_name = pe.get('proj', '')
            summary_info['项目名称'] = project_name if project_name else ''
        
    except Exception as e:
        print(f"提取人员摘要信息时出错: {str(e)}")
        # 返回空信息，避免程序中断
        summary_info = {
            '姓名': '',
            '工作年限': '',
            '最高学历': '',
            '学历证明材料': '',
            '毕业院校': '',
            '项目名称': ''
        }
    print(summary_info)
    return summary_info


def fill_summary_sheet(workbook, summary_data_list: List[Dict[str, Any]], start_row: int = 3):
    """
    在首页工作表填写多人基本信息
    
    Args:
        workbook: Excel工作簿对象
        summary_data_list: 多人摘要信息列表
        start_row: 开始填写的行号（默认第1行）
    """
    try:
        # 查找首页工作表（非"简历"结尾的工作表）
        summary_sheet = None
        for sheet_name in workbook.sheetnames:
            if not sheet_name.endswith('简历'):
                summary_sheet = workbook[sheet_name]
                break
        
        if summary_sheet is None:
            print("警告: 未找到首页工作表，跳过首页信息填写")
            return
        
        # 定义占位符到列的映射关系
        placeholder_mapping = {
            '{{姓名}}': '姓名',
            '{{工作年限}}': '工作年限', 
            '{{最高学历}}': '最高学历',
            '{{学历证明材料}}': '学历证明材料',
            '{{毕业院校}}': '毕业院校',
            '{{项目名称}}': '项目名称'
        }
        
        # 查找占位符位置，以姓名占位符为基准
        placeholder_positions = {}
        name_placeholder_cell = None
        
        for row in summary_sheet.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    for placeholder in placeholder_mapping.keys():
                        if placeholder in cell.value:
                            placeholder_positions[placeholder] = cell
                            if '{{姓名}}' in placeholder:
                                name_placeholder_cell = cell
                            break
        
        print(f"找到的占位符位置: {placeholder_positions}")
        
        # 如果找到姓名占位符，以其位置为基准，自动查找其他字段对应的列
        if name_placeholder_cell:
            print(f"以姓名占位符 {name_placeholder_cell.coordinate} 为基准，自动查找其他字段列位置")
            base_row = name_placeholder_cell.row
            
            # 在同一行查找所有包含占位符的单元格，自动确定列位置
            for col in range(1, summary_sheet.max_column + 1):
                cell = summary_sheet.cell(row=base_row, column=col)
                if cell.value and isinstance(cell.value, str):
                    cell_value = str(cell.value).strip()
                    # 检查是否包含任何占位符
                    for placeholder in placeholder_mapping.keys():
                        if placeholder in cell_value:
                            placeholder_positions[placeholder] = cell
                            print(f"找到占位符 {placeholder} 在位置: {cell.coordinate}")
                            break
        
        # 如果没有找到占位符，尝试按列标题查找
        if not placeholder_positions:
            # 尝试查找表头行
            header_row = None
            for row_idx, row in enumerate(summary_sheet.iter_rows(), 1):
                for cell in row:
                    if cell.value and isinstance(cell.value, str):
                        cell_value = str(cell.value).strip()
                        if any(keyword in cell_value for keyword in ['姓名', '工作年限', '最高学历', '毕业院校', '项目经验']):
                            header_row = row_idx
                            break
                if header_row:
                    break
            
            # 如果找到表头行，建立列映射
            if header_row:
                header_cells = list(summary_sheet.iter_rows(min_row=header_row, max_row=header_row))[0]
                for cell in header_cells:
                    if cell.value and isinstance(cell.value, str):
                        cell_value = str(cell.value).strip()
                        for field_name, field_key in placeholder_mapping.items():
                            field_key_clean = field_key.replace('{{', '').replace('}}', '')
                            if field_key_clean in cell_value:
                                placeholder_positions[field_name] = cell
                                break
        
        # 填写多人信息，使用实际找到的占位符位置
        start_row = name_placeholder_cell.row if name_placeholder_cell else start_row
        
        print(f"以第 {start_row} 行作为起始行填写 {len(summary_data_list)} 人的信息")
        
        for i, person_summary in enumerate(summary_data_list):
            # 计算当前人员的行位置
            current_row = start_row + i
            
            for placeholder, field_name in placeholder_mapping.items():
                if placeholder in placeholder_positions:
                    # 获取占位符所在的单元格位置
                    placeholder_cell = placeholder_positions[placeholder]
                    target_col = placeholder_cell.column  # 使用占位符实际的列位置
                    
                    target_cell = summary_sheet.cell(row=current_row, column=target_col)
                    
                    # 检查是否为合并单元格，如果是则跳过
                    try:
                        # 填写数据
                        field_value = person_summary.get(field_name, '')
                        if field_value:
                            target_cell.value = field_value
                            print(f"填写 {field_name} 到 {target_cell.coordinate}: {field_value}")
                            # 简化格式复制，避免递归错误
                            try:
                                if hasattr(placeholder_cell, 'font') and placeholder_cell.font:
                                    target_cell.font = placeholder_cell.font
                                if hasattr(placeholder_cell, 'alignment') and placeholder_cell.alignment:
                                    target_cell.alignment = placeholder_cell.alignment
                            except:
                                # 如果格式复制失败，至少保证数据填写成功
                                pass
                    except AttributeError as e:
                        if 'MergedCell' in str(e):
                            print(f"警告: 跳过合并单元格 {target_cell.coordinate} 的填写")
                            continue
                        else:
                            raise e
        
        print(f"已在首页填写 {len(summary_data_list)} 人的基本信息")
        
    except Exception as e:
        print(f"填写首页信息时出错: {str(e)}")
        import traceback
        traceback.print_exc()


def generate_multiple_resumes_in_one_file(person_data_list: List[Tuple[str, Dict[str, Any]]], 
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
        
        # 创建新工作簿（完整复制模板）
        new_workbook = copy_workbook(template_workbook)
        
        # 为每个人员创建工作表
        for person_name, person_data in person_data_list:
            print(f"正在为 {person_name} 创建工作表...")
            create_person_sheet_with_formatting(template_sheet, person_name, person_data, new_workbook)
        
        # 收集所有人员的摘要信息用于首页填写
        summary_data_list = []
        for person_name, person_data in person_data_list:
            summary_info = extract_person_summary_info(person_data)
            summary_data_list.append(summary_info)
        
        # 在首页填写基本信息
        if summary_data_list:
            fill_summary_sheet(new_workbook, summary_data_list)
        
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
            
            # 生成输出文件名 - 修改为要求的格式
            current_date = datetime.now().strftime("%Y%m%d")
            output_filename = f"{bankname}_合并简历_{current_date}.xlsx"
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