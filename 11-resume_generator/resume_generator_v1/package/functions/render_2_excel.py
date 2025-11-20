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
import weakref

try:
    import openpyxl
    from openpyxl import Workbook, load_workbook
    from openpyxl.utils import get_column_letter
    from openpyxl.utils.cell import coordinate_from_string
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from jinja2 import Environment, Template, TemplateError
except ImportError:
    print("错误: 未安装所需库，请运行: pip install openpyxl jinja2")
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


def find_placeholder_cells(worksheet) -> Dict[str, List[str]]:
    """
    查找工作表中的占位符单元格，支持多个相同占位符
    
    Args:
        worksheet: openpyxl工作表对象
        
    Returns:
        Dict[str, List[str]]: 占位符映射，键为占位符名称，值为单元格位置列表
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
                        
                        # 初始化列表（如果不存在）
                        if placeholder_name not in placeholders:
                            placeholders[placeholder_name] = []
                        
                        # 添加单元格位置到列表中
                        placeholders[placeholder_name].append(cell.coordinate)
    
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


def render_jinja2_template(template_str: str, context: dict) -> str:
    """
    使用Jinja2渲染模板字符串
    
    Args:
        template_str: 包含Jinja2语法的模板字符串
        context: 模板上下文数据
        
    Returns:
        渲染后的字符串
    """
    try:
        # 创建Jinja2环境
        env = Environment()
        template = env.from_string(template_str)
        # 渲染模板
        rendered = template.render(**context)
        return rendered
    except TemplateError as e:
        print(f"Jinja2模板渲染错误: {e}")
        return f"[模板错误: {str(e)}]"
    except Exception as e:
        print(f"渲染模板时出错: {e}")
        return f"[渲染错误: {str(e)}]"


def find_jinja2_cells(worksheet) -> List[Tuple[str, str]]:
    """
    查找工作表中的Jinja2模板单元格（排除工作经历和项目经历相关占位符）
    
    Args:
        worksheet: openpyxl工作表对象
        
    Returns:
        List[Tuple[str, str]]: 包含Jinja2语法的单元格列表，格式为[(coordinate, template_str), ...]
    """
    jinja2_cells = []
    
    # 需要排除的工作经历和项目经历相关占位符
    excluded_placeholders = {
        '开始时间', '结束时间', '起始时间', '公司名称', '公司职位', '工作描述',
        '项目开始时间', '项目结束时间', '项目时间', '项目名称', '项目角色', '项目描述'
    }
    
    for row in worksheet.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str):
                # 检查是否包含Jinja2语法
                if ('{{' in cell.value and '}}' in cell.value):
                    # 检查是否包含需要排除的占位符
                    cell_content = cell.value
                    should_exclude = False
                    
                    for placeholder in excluded_placeholders:
                        if f'{{{{{placeholder}}}}}' in cell_content:
                            should_exclude = True
                            break
                    
                    # 如果不包含排除的占位符，则加入处理列表
                    if not should_exclude:
                        jinja2_cells.append((cell.coordinate, cell.value))
    
    return jinja2_cells


def prepare_work_experience_data(person_data: Dict[str, Any]) -> Dict[str, List]:
    """
    将工作经历数据转换为列表格式，用于表格填充
    
    Args:
        person_data: 人员数据字典
        
    Returns:
        包含工作经历列表数据的字典
    """

    #print(person_data)
    work_experience_data = {
        '开始时间': [],
        '结束时间': [],
        '起始时间': [],  # 开始结束时间段
        '公司名称': [],
        '公司职位': [],
        '工作描述': []
    }
    
    # 获取工作经历数据
    we = person_data.get('we', [])
    if not isinstance(we, list):
        we = []
    
    for exp in we:
        if not isinstance(exp, dict):
            continue
        
        # 提取各字段数据
        start_date = exp.get('start', '')
        end_date = exp.get('end', '')
        company = exp.get('comp', '')
        position = exp.get('pos', '')
        description = exp.get('desc', '')
        
        # 构建时间段
        time_period = f"{start_date}-{end_date}" if start_date and end_date else (start_date or end_date or '')
        
        # 添加到对应的列表
        work_experience_data['开始时间'].append(start_date)
        work_experience_data['结束时间'].append(end_date)
        work_experience_data['起始时间'].append(time_period)
        work_experience_data['公司名称'].append(company)
        work_experience_data['公司职位'].append(position)
        work_experience_data['工作描述'].append(description)
    
    return work_experience_data


def prepare_project_experience_data(person_data: Dict[str, Any]) -> Dict[str, List]:
    """
    将项目经历数据转换为列表格式，用于表格填充
    
    Args:
        person_data: 人员数据字典
        
    Returns:
        包含项目经历列表数据的字典
    """
    project_experience_data = {
        '项目开始时间': [],
        '项目结束时间': [],
        '项目时间': [],  # 项目时间段
        '项目名称': [],
        '项目角色': [],
        '项目描述': []
    }
    
    # 获取项目经历数据
    pe = person_data.get('pe', [])
    if not isinstance(pe, list):
        pe = []
    
    for proj in pe:
        if not isinstance(proj, dict):
            continue
        
        # 提取各字段数据
        start_date = proj.get('start', '')
        end_date = proj.get('end', '')
        name = proj.get('proj', '')
        role = proj.get('role', '')
        description = proj.get('desc', '')
        
        # 构建时间段
        time_period = f"{start_date}-{end_date}" if start_date and end_date else (start_date or end_date or '')
        
        # 添加到对应的列表
        project_experience_data['项目开始时间'].append(start_date)
        project_experience_data['项目结束时间'].append(end_date)
        project_experience_data['项目时间'].append(time_period)
        project_experience_data['项目名称'].append(name)
        project_experience_data['项目角色'].append(role)
        project_experience_data['项目描述'].append(description)
    #print(project_experience_data)    
    return project_experience_data


def find_section_row(worksheet, section_name):
    """查找指定章节的行号"""
    # 如果section_name是字符串，转换为列表以便统一处理
    if isinstance(section_name, str):
        section_names = [section_name]
    else:
        section_names = section_name
    
    for row in worksheet.iter_rows():
        for cell in row:
            if cell.value and isinstance(cell.value, str):
                # 检查所有可能的章节名称
                for name in section_names:
                    if name in str(cell.value):
                        print(f"找到章节 '{name}' 在第{cell.row}行，列{cell.column}，内容: '{cell.value}'")
                        return cell.row
    return None


def find_placeholder_columns(sheet: openpyxl.worksheet.worksheet.Worksheet, section_row: int, 
                           placeholders: List[str]) -> Dict[str, int]:
    """
    在指定行中查找占位符所在的列号
    
    Args:
        sheet: Excel工作表对象
        section_row: 章节所在行号
        placeholders: 占位符列表，如["{{开始时间}}", "{{结束时间}}"]
        
    Returns:
        占位符列号映射字典
    """
    placeholder_columns = {}
    
    # 搜索章节行及其下方几行，重点关注section_row+1和section_row+2行
    search_rows = range(section_row, min(section_row + 5, sheet.max_row + 1))
    
    print(f"搜索占位符，从行 {section_row} 到 {min(section_row + 5, sheet.max_row)}")
    
    for row_num in search_rows:
        for cell in sheet[row_num]:
            if cell.value and isinstance(cell.value, str):
                cell_value = str(cell.value).strip()
                for placeholder in placeholders:
                    if placeholder in cell_value:
                        placeholder_columns[placeholder] = cell.column
                        print(f"找到占位符 '{placeholder}' 在列 {cell.column} (行 {cell.row})")
    
    # 如果没找到，尝试在section_row+1和section_row+2行进行更精确的搜索
    if not placeholder_columns:
        print("未在常规范围内找到占位符，尝试精确搜索section_row+1和section_row+2行")
        precise_search_rows = [section_row + 1, section_row + 2]
        
        for row_num in precise_search_rows:
            if row_num <= sheet.max_row:
                print(f"精确搜索行 {row_num} 的内容:")
                for cell in sheet[row_num]:
                    if cell.value:
                        cell_value = str(cell.value).strip()
                        print(f"  列 {cell.column}: '{cell_value}'")
                        
                        for placeholder in placeholders:
                            if placeholder in cell_value:
                                placeholder_columns[placeholder] = cell.column
                                print(f"找到占位符 '{placeholder}' 在列 {cell.column} (行 {cell.row})")
    
    return placeholder_columns


def copy_section_format(sheet: openpyxl.worksheet.worksheet.Worksheet) -> None:
    """
    复制章节格式：将各章节行的格式复制给对应的数据行
    特别处理：将工作经历章节行的格式复制给项目经验章节行
    
    Args:
        sheet: Excel工作表对象
    """
    # 查找所有章节行
    sections = []
    for row in range(1, sheet.max_row + 1):
        cell = sheet.cell(row=row, column=1)  # 假设章节标题在第一列
        if cell.value and isinstance(cell.value, str):
            cell_value = str(cell.value).strip()
            # 识别章节标题
            if any(section in cell_value for section in ['工作经历', '工作经验', '项目经历', '项目经验', '教育背景', '专业技能', '自我评价']):
                sections.append({
                    'row': row,
                    'name': cell_value,
                    'type': 'work' if '工作' in cell_value else 'project' if '项目' in cell_value else 'other'
                })
    
    # 按行号排序
    sections.sort(key=lambda x: x['row'])
    
    print(f"找到章节: {[s['name'] + '(行' + str(s['row']) + ')' for s in sections]}")
    
    # 处理每个章节的格式复制
    for i, section in enumerate(sections):
        current_row = section['row']
        
        # 确定章节结束行（下一个章节行-1，或表格末尾）
        if i + 1 < len(sections):
            next_section_row = sections[i + 1]['row']
            end_row = next_section_row - 1
        else:
            end_row = sheet.max_row
        
        # 将当前章节行的格式复制给该章节的所有数据行（下一行到结束行）
        for target_row in range(current_row + 1, end_row + 1):
            copy_row_format(sheet, current_row, target_row)
        
        print(f"已将章节 '{section['name']}'(行{current_row})的格式复制给行{current_row + 1}-{end_row}")
    
    # 特别处理：将工作经历章节行的格式复制给项目经验章节行
    work_section = None
    project_section = None
    
    for section in sections:
        if section['type'] == 'work':
            work_section = section
        elif section['type'] == 'project':
            project_section = section
    
    if work_section and project_section:
        copy_row_format(sheet, work_section['row'], project_section['row'])
        print(f"已将工作经历章节行(行{work_section['row']})的格式复制给项目经验章节行(行{project_section['row']})")


def add_experience_rows(sheet: openpyxl.worksheet.worksheet.Worksheet, section_row: int, data_length: int, placeholder_columns: Dict[str, int] = None) -> None:
    """
    在章节下方添加新行以容纳经历数据
    
    Args:
        sheet: Excel工作表对象
        section_row: 章节所在行号
        data_length: 需要添加的数据行数
        placeholder_columns: 占位符列位置字典
    """
    if data_length <= 1:
        return  # 如果只有1行数据，不需要添加新行（占位符行本身就是第一行）
    
    # 找到占位符行位置
    placeholder_row = None
    if placeholder_columns:
        for col in placeholder_columns.values():
            for row in range(section_row + 1, min(section_row + 10, sheet.max_row + 1)):
                cell = sheet.cell(row=row, column=col)
                if cell.value and isinstance(cell.value, str):
                    if any(placeholder in str(cell.value) for placeholder in ['{{', '}}', '{%', '%}']):
                        placeholder_row = row
                        break
            if placeholder_row:
                break
    
    if not placeholder_row:
        # 如果没找到占位符行，使用默认位置（章节行+2）
        placeholder_row = section_row + 2
    
    # 在占位符行下方插入新行（数据行数-1）
    rows_to_add = data_length - 1
    for i in range(rows_to_add):
        sheet.insert_rows(placeholder_row + 1 + i)
    
    print(f"已添加 {rows_to_add} 行在占位符行下方")


def copy_row_format(sheet: openpyxl.worksheet.worksheet.Worksheet, source_row: int, target_row: int) -> None:
    """
    复制源行的格式到目标行
    
    Args:
        sheet: Excel工作表对象
        source_row: 源行号
        target_row: 目标行号
    """
    # 复制每个单元格的格式
    for col in range(1, sheet.max_column + 1):
        source_cell = sheet.cell(row=source_row, column=col)
        target_cell = sheet.cell(row=target_row, column=col)
        
        # 复制字体
        if source_cell.font:
            target_cell.font = openpyxl.styles.Font(
                name=source_cell.font.name,
                size=source_cell.font.size,
                bold=source_cell.font.bold,
                italic=source_cell.font.italic,
                color=source_cell.font.color
            )
        
        # 复制填充
        if source_cell.fill:
            target_cell.fill = openpyxl.styles.PatternFill(
                fill_type=source_cell.fill.fill_type,
                start_color=source_cell.fill.start_color,
                end_color=source_cell.fill.end_color
            )
        
        # 复制边框
        if source_cell.border:
            target_cell.border = openpyxl.styles.Border(
                left=source_cell.border.left,
                right=source_cell.border.right,
                top=source_cell.border.top,
                bottom=source_cell.border.bottom
            )
        
        # 复制对齐方式
        if source_cell.alignment:
            target_cell.alignment = openpyxl.styles.Alignment(
                horizontal=source_cell.alignment.horizontal,
                vertical=source_cell.alignment.vertical,
                wrap_text=source_cell.alignment.wrap_text
            )
        
        # 复制数字格式
        if source_cell.number_format:
            target_cell.number_format = source_cell.number_format


def fill_work_experience(sheet: openpyxl.worksheet.worksheet.Worksheet, work_data: Dict[str, List], prefound_placeholders: Dict[str, int] = None) -> None:
    """
    填充工作经历数据到表格
    
    Args:
        sheet: Excel工作表对象
        work_data: 工作经历数据字典
        prefound_placeholders: 预先找到的占位符位置字典
    """
    if not work_data or not any(work_data.values()):
        print("没有工作经历数据需要填充")
        return
    
    # 查找工作经历章节行
    section_row = find_section_row(sheet, ["工作经历", "工作经验"])
    if not section_row:
        print("未找到工作经历章节")
        return
    
    # 获取数据行数
    data_length = len(work_data.get('开始时间', []))
    if data_length == 0:
        print("工作经历数据为空")
        return
    
    # 使用预先保存的占位符位置，如果没有则重新查找
    if prefound_placeholders:
        placeholder_columns = prefound_placeholders
        print(f"使用预先保存的工作经历占位符位置: {placeholder_columns}")
    else:
        placeholders = ['{{work.start}}', '{{work.end}}', '{{work.comp}}', '{{work.pos}}', '{{work.desc}}']
        placeholder_columns = find_placeholder_columns(sheet, section_row, placeholders)
        print(f"重新查找的工作经历占位符位置: {placeholder_columns}")
    
    if not placeholder_columns:
        print("未找到工作经历占位符列")
        return
    
    # 添加新行（数据行数-1，因为占位符行本身算作一行）
    add_experience_rows(sheet, section_row, data_length, placeholder_columns)
    
    # 找到占位符所在的最大行号，数据填充起始行应该是占位符行
    max_placeholder_row = section_row
    if placeholder_columns:
        for col in placeholder_columns.values():
            for row in range(section_row + 1, min(section_row + 6, sheet.max_row + 1)):
                cell = sheet.cell(row=row, column=col)
                if cell.value and isinstance(cell.value, str):
                    if cell.value and isinstance(cell.value, str):
                        if any(placeholder in str(cell.value) for placeholder in ['{{', '}}', '{%', '%}']):
                            max_placeholder_row = max(max_placeholder_row, row)
                            break
    
    # 填充数据从占位符行开始（占位符行本身作为第一行数据）
    start_row = max_placeholder_row
    for i in range(data_length):
        current_row = start_row + i
        
        # 填充各个字段 - 支持两种占位符名称格式
        # 开始时间/起始时间（统一处理）
        if '{{work.start}}' in placeholder_columns:
            time_data = work_data.get('开始时间') or work_data.get('起始时间', [])
            if i < len(time_data):
                col = placeholder_columns['{{work.start}}']
                sheet.cell(row=current_row, column=col, value=time_data[i])
        elif '{{开始时间}}' in placeholder_columns:
            time_data = work_data.get('开始时间', [])
            if i < len(time_data):
                col = placeholder_columns['{{开始时间}}']
                sheet.cell(row=current_row, column=col, value=time_data[i])
        elif '{{起始时间}}' in placeholder_columns:
            time_data = work_data.get('起始时间', [])
            if i < len(time_data):
                col = placeholder_columns['{{起始时间}}']
                sheet.cell(row=current_row, column=col, value=time_data[i])

        # 结束时间
        if '{{work.end}}' in placeholder_columns and i < len(work_data.get('结束时间', [])):
            col = placeholder_columns['{{work.end}}']
            sheet.cell(row=current_row, column=col, value=work_data['结束时间'][i])
        elif '{{结束时间}}' in placeholder_columns and i < len(work_data.get('结束时间', [])):
            col = placeholder_columns['{{结束时间}}']
            sheet.cell(row=current_row, column=col, value=work_data['结束时间'][i])
            
        # 公司名称
        if '{{work.comp}}' in placeholder_columns and i < len(work_data.get('公司名称', [])):
            col = placeholder_columns['{{work.comp}}']
            sheet.cell(row=current_row, column=col, value=work_data['公司名称'][i])
        elif '{{公司名称}}' in placeholder_columns and i < len(work_data.get('公司名称', [])):
            col = placeholder_columns['{{公司名称}}']
            sheet.cell(row=current_row, column=col, value=work_data['公司名称'][i])
            
        # 公司职位
        if '{{work.pos}}' in placeholder_columns and i < len(work_data.get('公司职位', [])):
            col = placeholder_columns['{{work.pos}}']
            sheet.cell(row=current_row, column=col, value=work_data['公司职位'][i])
        elif '{{公司职位}}' in placeholder_columns and i < len(work_data.get('公司职位', [])):
            col = placeholder_columns['{{公司职位}}']
            sheet.cell(row=current_row, column=col, value=work_data['公司职位'][i])
            
        # 工作描述
        if '{{work.desc}}' in placeholder_columns and i < len(work_data.get('工作描述', [])):
            col = placeholder_columns['{{work.desc}}']
            sheet.cell(row=current_row, column=col, value=work_data['工作描述'][i])
        elif '{{工作描述}}' in placeholder_columns and i < len(work_data.get('工作描述', [])):
            col = placeholder_columns['{{工作描述}}']
            sheet.cell(row=current_row, column=col, value=work_data['工作描述'][i])
    
    print(f"已填充 {data_length} 行工作经历数据")


def fill_project_experience(sheet: openpyxl.worksheet.worksheet.Worksheet, project_data: Dict[str, List], prefound_placeholders: Dict[str, int] = None) -> None:
    """
    填充项目经历数据到表格
    
    Args:
        sheet: Excel工作表对象
        project_data: 项目经历数据字典
        prefound_placeholders: 预先找到的占位符位置字典
    """
    if not project_data or not any(project_data.values()):
        print("没有项目经历数据需要填充")
        return
    
    # 查找项目经历章节行
    section_row = find_section_row(sheet, ["项目经历", "项目经验"])
    if not section_row:
        print("未找到项目经历章节")
        return
    
    # 获取数据行数
    data_length = len(project_data.get('项目名称', []))
    if data_length == 0:
        print("项目经历数据为空")
        return
    
    # 使用预先保存的占位符位置，如果没有则重新查找
    if prefound_placeholders:
        placeholder_columns = prefound_placeholders
        print(f"使用预先保存的项目经历占位符位置: {placeholder_columns}")
    else:
        placeholders = ['{{prj.proj}}', '{{prj.desc}}', '{{prj.start}} 至 {{prj.end}}', '{{prj.role}}']
        placeholder_columns = find_placeholder_columns(sheet, section_row, placeholders)
        print(f"重新查找的项目经历占位符位置: {placeholder_columns}")
    
    if not placeholder_columns:
        print("未找到项目经历占位符列")
        return
    
    # 添加新行（数据行数-1，因为占位符行本身算作一行）
    add_experience_rows(sheet, section_row, data_length, placeholder_columns)
    
    # 找到占位符所在的最大行号，数据填充起始行应该是占位符行
    max_placeholder_row = section_row
    if placeholder_columns:
        for col in placeholder_columns.values():
            for row in range(section_row + 1, min(section_row + 6, sheet.max_row + 1)):
                cell = sheet.cell(row=row, column=col)
                if cell.value and isinstance(cell.value, str):
                    if any(placeholder in str(cell.value) for placeholder in ['{{', '}}', '{%', '%}']):
                        max_placeholder_row = max(max_placeholder_row, row)
                        break
    
    # 填充数据从占位符行开始（占位符行本身作为第一行数据）
    start_row = max_placeholder_row
    for i in range(data_length):
        current_row = start_row + i
        
        # 填充各个字段 - 支持两种占位符名称格式
        # 项目名称
        if '{{prj.proj}}' in placeholder_columns and i < len(project_data.get('项目名称', [])):
            col = placeholder_columns['{{prj.proj}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目名称'][i])
        elif '{{项目名称}}' in placeholder_columns and i < len(project_data.get('项目名称', [])):
            col = placeholder_columns['{{项目名称}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目名称'][i])
            
        # 项目描述
        if '{{prj.desc}}' in placeholder_columns and i < len(project_data.get('项目描述', [])):
            col = placeholder_columns['{{prj.desc}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目描述'][i])
        elif '{{项目描述}}' in placeholder_columns and i < len(project_data.get('项目描述', [])):
            col = placeholder_columns['{{项目描述}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目描述'][i])
            
        # 项目时间
        if '{{prj.start}} 至 {{prj.end}}' in placeholder_columns and i < len(project_data.get('项目时间', [])):
            col = placeholder_columns['{{prj.start}} 至 {{prj.end}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目时间'][i])
        elif '{{项目时间}}' in placeholder_columns and i < len(project_data.get('项目时间', [])):
            col = placeholder_columns['{{项目时间}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目时间'][i])
            
        # 项目角色
        if '{{prj.role}}' in placeholder_columns and i < len(project_data.get('项目角色', [])):
            col = placeholder_columns['{{prj.role}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目角色'][i])
        elif '{{项目角色}}' in placeholder_columns and i < len(project_data.get('项目角色', [])):
            col = placeholder_columns['{{项目角色}}']
            sheet.cell(row=current_row, column=col, value=project_data['项目角色'][i])
    
    print(f"已填充 {data_length} 行项目经历数据")


def copy_worksheet_with_formatting(source_worksheet: openpyxl.worksheet.worksheet.Worksheet,
                                  target_worksheet: openpyxl.worksheet.worksheet.Worksheet, 
                                  target_workbook: openpyxl.Workbook):
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


def copy_workbook(source_workbook: openpyxl.Workbook) -> openpyxl.Workbook:
    """
    完整复制工作簿，包括所有工作表和格式
    
    Args:
        source_workbook: 源工作簿
        
    Returns:
        复制的新工作簿
    """
    new_workbook = openpyxl.Workbook()
    
    # 删除默认创建的工作表
    if new_workbook.sheetnames:
        new_workbook.remove(new_workbook.active)
    
    # 复制所有工作表
    for sheet_name in source_workbook.sheetnames:
        source_sheet = source_workbook[sheet_name]
        new_sheet = new_workbook.create_sheet(title=sheet_name)
        
        # 复制工作表内容和格式
        copy_worksheet_with_formatting(source_sheet, new_sheet, new_workbook)
    
    return new_workbook


def create_person_sheet_with_formatting(template_worksheet: openpyxl.worksheet.worksheet.Worksheet, 
                                       person_name: str, 
                                       person_data: Dict[str, Any], 
                                       target_workbook: openpyxl.Workbook) -> openpyxl.worksheet.worksheet.Worksheet:
    """
    为指定人员创建工作表，完整复制模板格式并处理数据填充
    
    Args:
        template_worksheet: 模板工作表
        person_name: 人员姓名
        person_data: 人员数据
        target_workbook: 目标工作簿
        
    Returns:
        创建的新工作表
    """
    # 创建新工作表
    new_sheet = target_workbook.create_sheet(title=f"{person_name}_简历")
    
    # 完整复制模板工作表的内容和格式
    copy_worksheet_with_formatting(template_worksheet, new_sheet, target_workbook)
    
    # 预先查找并保存工作经历和项目经历的占位符位置（在Jinja2渲染之前）
    work_experience_placeholders = None
    project_experience_placeholders = None
    
    try:
        # 预先查找工作经历占位符位置
        print("预先查找工作经历占位符位置...")
        work_section_row = find_section_row(new_sheet, "工作经验")
        if not work_section_row:
            work_section_row = find_section_row(new_sheet, "工作经历")
        
        if work_section_row:
            work_placeholders = ['{{起始时间}}', '{{结束时间}}', '{{公司名称}}', '{{公司职位}}', '{{工作描述}}']
            work_placeholder_columns = find_placeholder_columns(new_sheet, work_section_row, work_placeholders)
            print(f"找到工作经历占位符位置: {work_placeholder_columns}")
        else:
            work_placeholder_columns = {}
            print("未找到工作经历章节")
        
        # 预先查找项目经历占位符位置
        print("预先查找项目经历占位符位置...")
        project_section_row = find_section_row(new_sheet, "项目经验")
        if not project_section_row:
            project_section_row = find_section_row(new_sheet, "项目经历")
        
        if project_section_row:
            project_placeholders = ['{{项目名称}}', '{{项目描述}}', '{{项目时间}}', '{{项目角色}}']
            project_placeholder_columns = find_placeholder_columns(new_sheet, project_section_row, project_placeholders)
            print(f"找到项目经历占位符位置: {project_placeholder_columns}")
        else:
            project_placeholder_columns = {}
            print("未找到项目经历章节")
            
    except Exception as e:
        print(f"预先查找占位符位置时出错: {str(e)}")
    
    # 查找占位符并替换（除了工作经历和项目经历的占位符）
    placeholders = find_placeholder_cells(new_sheet)
    jinja2_cells = find_jinja2_cells(new_sheet)
    sanitized_data = sanitize_data(person_data)
    
    # 定义需要保留给后续处理的占位符集合
    reserved_placeholders = {'起始时间', '结束时间', '公司名称', '公司职位', '工作描述', '项目名称', '项目时间', '项目角色', '项目描述'}
    
    # 处理Jinja2模板单元格
    for cell_coord, template_str in jinja2_cells:
        cell = new_sheet[cell_coord]
        try:
            rendered = render_jinja2_template(template_str, sanitized_data)
            cell.value = rendered
        except Exception as e:
            print(f"渲染Jinja2模板 {cell_coord} 时出错: {str(e)}]")
            cell.value = f"[模板错误: {str(e)}]"
    
    # 处理普通占位符（排除保留的占位符）
    for placeholder, cell_coords in placeholders.items():
        # 跳过保留的占位符
        if placeholder in reserved_placeholders:
            print(f"跳过保留占位符: {placeholder}")
            continue
            
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
            
            # 处理该占位符的所有单元格位置
            for cell_coord in cell_coords:
                cell = new_sheet[cell_coord]
                render_template_cell(cell, value)
                
        except Exception as e:
            print(f"渲染占位符 {placeholder} 时出错: {str(e)}")
            # 为所有相同占位符的单元格设置错误信息
            for cell_coord in cell_coords:
                cell = new_sheet[cell_coord]
                render_template_cell(cell, f"[错误: {placeholder}]")
    
    # 处理工作经历和项目经历的表格数据
    try:
        # 准备工作经历数据并填充
        work_experience_data = prepare_work_experience_data(person_data)
        print(f"工作经历数据: {work_experience_data}")
        if work_experience_data and any(work_experience_data.values()):
            print(f"开始处理工作经历数据: {len(work_experience_data.get('开始时间', []))} 条记录")
            # 使用预先保存的占位符位置，如果没有则重新查找
            fill_work_experience(new_sheet, work_experience_data, work_placeholder_columns)
        
        # 准备项目经历数据并填充
        project_experience_data = prepare_project_experience_data(person_data)
        print(f"项目经历数据: {project_experience_data}")
        if project_experience_data and any(project_experience_data.values()):
            print(f"开始处理项目经历数据: {len(project_experience_data.get('项目名称', []))} 条记录")
            # 使用预先保存的占位符位置，如果没有则重新查找
            fill_project_experience(new_sheet, project_experience_data, project_placeholder_columns)
            
    except Exception as e:
        print(f"处理工作经历和项目经历时出错: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # 在所有数据处理完成后，执行章节格式复制
    try:
        print("开始执行章节格式复制...")
        
        # 查找工作经历和项目经历章节行
        work_section_row = find_section_row(new_sheet, "工作经验")
        if not work_section_row:
            work_section_row = find_section_row(new_sheet, "工作经历")
        
        project_section_row = find_section_row(new_sheet, "项目经验")
        if not project_section_row:
            project_section_row = find_section_row(new_sheet, "项目经历")
        
        # 复制工作经历章节格式到其数据行
        if work_section_row:
            # 找到工作经历数据的结束行（下一章节的前一行）
            work_end_row = None
            if project_section_row and project_section_row > work_section_row:
                work_end_row = project_section_row - 1
            else:
                # 如果没有项目经历，查找工作经历数据的实际结束行
                work_end_row = work_section_row
                for row in range(work_section_row + 1, new_sheet.max_row + 1):
                    has_data = False
                    for col in range(1, new_sheet.max_column + 1):
                        cell = new_sheet.cell(row=row, column=col)
                        if cell.value and str(cell.value).strip():
                            has_data = True
                            break
                    if has_data:
                        work_end_row = row
                    else:
                        break
            
            if work_end_row and work_end_row > work_section_row:
                copy_section_format(new_sheet, work_section_row, work_section_row + 1, work_end_row)
                print(f"已复制工作经历章节行 {work_section_row} 格式到数据行 {work_section_row + 1}-{work_end_row}")
        
        # 将工作经历章节格式复制到项目经验章节行
        if work_section_row and project_section_row:
            copy_section_format(new_sheet, work_section_row, project_section_row, project_section_row)
            print(f"已复制工作经历章节行 {work_section_row} 格式到项目经验章节行 {project_section_row}")
        
        # 复制项目经历章节格式到其数据行
        if project_section_row:
            # 找到项目经历数据的结束行
            project_end_row = project_section_row
            for row in range(project_section_row + 1, new_sheet.max_row + 1):
                has_data = False
                for col in range(1, new_sheet.max_column + 1):
                    cell = new_sheet.cell(row=row, column=col)
                    if cell.value and str(cell.value).strip():
                        has_data = True
                        break
                if has_data:
                    project_end_row = row
                else:
                    break
            
            if project_end_row and project_end_row > project_section_row:
                copy_section_format(new_sheet, project_section_row, project_section_row + 1, project_end_row)
                print(f"已复制项目经历章节行 {project_section_row} 格式到数据行 {project_section_row + 1}-{project_end_row}")
        
        print("章节格式复制完成")
        
    except Exception as e:
        print(f"执行章节格式复制时出错: {str(e)}")
        import traceback
        traceback.print_exc()
    
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
            '{{项目名称}}': '项目名称',
            '{{总人数}}': '总人数'
        }
        
        # 查找占位符位置，支持多个相同占位符
        placeholder_positions = {}  # 改为字典，值为列表，存储所有相同占位符的位置
        name_placeholder_cell = None
        
        for row in summary_sheet.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    for placeholder in placeholder_mapping.keys():
                        if placeholder in cell.value:
                            # 初始化列表（如果不存在）
                            if placeholder not in placeholder_positions:
                                placeholder_positions[placeholder] = []
                            placeholder_positions[placeholder].append(cell)
                            
                            if '{{姓名}}' in placeholder:
                                name_placeholder_cell = cell
                            break
        
        # 打印找到的占位符位置信息
        print("找到的占位符位置:")
        for placeholder, cells in placeholder_positions.items():
            positions = [cell.coordinate for cell in cells]
            print(f"  {placeholder}: {positions}")
        
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
                            # 初始化列表（如果不存在）
                            if placeholder not in placeholder_positions:
                                placeholder_positions[placeholder] = []
                            placeholder_positions[placeholder].append(cell)
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
                                # 初始化列表（如果不存在）
                                if field_name not in placeholder_positions:
                                    placeholder_positions[field_name] = []
                                placeholder_positions[field_name].append(cell)
                                break
        
        # 填写多人信息，使用实际找到的占位符位置
        start_row = name_placeholder_cell.row if name_placeholder_cell else start_row
        
        print(f"以第 {start_row} 行作为起始行填写 {len(summary_data_list)} 人的信息")
        
        # 首先处理总人数（只需要填写一次）
        if '{{总人数}}' in placeholder_positions:
            total_count_cells = placeholder_positions['{{总人数}}']
            for total_count_cell in total_count_cells:
                try:
                    total_count_cell.value = f"人员简历汇总表（共{len(summary_data_list)}人）"
                    print(f"填写总人数到 {total_count_cell.coordinate}: 共{len(summary_data_list)}人")
                    # 复制格式
                    try:
                        if hasattr(total_count_cell, 'font') and total_count_cell.font:
                            total_count_cell.font = total_count_cell.font
                        if hasattr(total_count_cell, 'alignment') and total_count_cell.alignment:
                            total_count_cell.alignment = total_count_cell.alignment
                    except:
                        pass
                except AttributeError as e:
                    if 'MergedCell' in str(e):
                        print(f"警告: 跳过合并单元格 {total_count_cell.coordinate} 的填写")
                    else:
                        raise e
        
        # 然后处理每个人的信息
        for i, person_summary in enumerate(summary_data_list):
            # 计算当前人员的行位置
            current_row = start_row + i
            
            for placeholder, field_name in placeholder_mapping.items():
                # 跳过总人数，已经处理过了
                if placeholder == '{{总人数}}':
                    continue
                    
                if placeholder in placeholder_positions:
                    # 获取所有相同占位符的单元格位置
                    placeholder_cells = placeholder_positions[placeholder]
                    
                    for placeholder_cell in placeholder_cells:
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
        
        print(f"已在首页填写 {len(summary_data_list)} 人的基本信息，总人数: 共{len(summary_data_list)}人")
        
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
        new_sheet, new_workbook = create_person_sheet_with_formatting(template_sheet, person_name, person_data)
        
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