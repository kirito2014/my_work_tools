#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
备用简历数据提取脚本
当doc_2_json.py无法完美提取数据时使用此脚本
"""

import os
import json
import logging
import sys
from docx import Document
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATE_FORMATS = [
    '%Y/%m',
    '%Y-%m',
    '%Y年%m月',
    '%Y.%m',
    '%Y'
]

# 处理PyInstaller打包后的路径问题
if getattr(sys, 'frozen', False):
    # 打包后的环境
    base_dir = os.path.dirname(sys.executable)
    # 确保工作目录设置为当前目录（exe所在目录）
    os.chdir(base_dir)
else:
    # 开发环境
    # 获取项目根目录（package 的上两级）
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# 添加项目根目录到Python路径，以便能够导入package模块
sys.path.append(base_dir)

'''
功能: 判断是否docx文件（通过文件头magic number）
参数： 
    file_path   文件绝对路径
返回:
    True or False
'''
def is_real_docx(file_path):
	with open(file_path,'rb') as f:
		header = f.read(4)	
	return header == b'PK\x03\x04' #docx文件前4个字节

def check_file_format(file_path):
    """
    检查文件是否为docx格式，如果不是则尝试调用转换功能
    
    Args:
        file_path: 文件路径
    
    Returns:
        bool: 是否为有效的docx文件
    """
    if not os.path.exists(file_path):
        logger.error(f"文件不存在: {file_path}")
        return False
    
    # 检查是否为docx格式
    if not file_path.lower().endswith('.docx'):
        logger.warning(f"文件不是docx格式: {file_path}")
        # 尝试调用doc_converter进行格式转换
        try:
            from . import doc_converter
            converted_file = doc_converter.convert_to_docx(file_path)
            if converted_file:
                logger.info(f"文件已转换为docx格式: {converted_file}")
                return True, converted_file
            else:
                logger.error(f"文件转换失败: {file_path}")
                return False, file_path
        except ImportError:
            logger.error("未找到doc_converter模块")
            return False, file_path
    
    # 验证是否为有效的docx文件
    if not is_real_docx(file_path):
        logger.warning(f"文件虽然扩展名为docx，但不是有效的docx文件: {file_path}")
        return False, file_path
    
    return True, file_path


def clean_keyword(keyword):
    """
    清理关键词中的空格、回车和冒号，用于匹配
    
    Args:
        keyword: 原始关键词
    
    Returns:
        str: 清理后的关键词
    """
    if not keyword:
        return ''
    # 去除空格、回车、冒号等特殊字符
    return re.sub(r'[\s\r\n:：\t\xa0]+', '', keyword)


def getPosCell(table, row, col, keyword):
    """
    在表格中查找包含指定关键词的单元格
    参考docxResume2DF_v2.py中的实现
    
    Args:
        table: docx表格对象
        row: 行索引
        col: 列索引
        keyword: 要查找的关键词
    
    Returns:
        bool: 是否找到匹配的关键词
    """
    try:
        # 检查表格、行和列的有效性
        if not table or row >= len(table.rows) or col >= len(table.rows[row].cells):
            return False
            
        cell_text = table.rows[row].cells[col].text
        # 清理单元格文本和关键词中的空格、回车和冒号
        cleaned_cell_text = clean_keyword(cell_text)
        cleaned_keyword = clean_keyword(keyword)
        
        # 检查清理后的关键词是否在清理后的单元格文本中
        # 确保关键词不为空
        if not cleaned_keyword:
            return False
            
        return cleaned_keyword in cleaned_cell_text
    except Exception as e:
        logger.debug(f"查找关键词 '{keyword}' 时出错: {e}")
        return False


def extract_emp_no_from_filename(doc_path):
    """
    从文件名中提取工号
    
    Args:
        doc_path: 文档路径
    
    Returns:
        str: 工号
    """
    try:
        # 获取文件名（不含路径）
        filename = os.path.basename(doc_path)
        # 尝试从文件名中提取工号（假设工号为文件名开头的数字部分）
        match = re.match(r'^(\d+)', filename)
        if match:
            return match.group(1)
        # 如果文件名包含'+'，尝试按照'+'分隔提取第一个部分作为工号
        if '+' in filename:
            emp_no_part = filename.split('+')[0]
            # 确保提取的工号只包含数字
            if emp_no_part.isdigit():
                return emp_no_part
        return 'unknown'
    except Exception as e:
        logger.error(f"从文件名提取工号失败: {str(e)}")
        return 'unknown'

def extract_basic_info(doc, filename):
    """
    提取基本信息
    
    Args:
        doc: Document对象
        filename: 文件名
    
    Returns:
        dict: 基本信息字典
    """
    # 初始化基本信息
    basic_info = {
        'EmpNo': extract_emp_no_from_filename(filename),
        'Name': '/',
        'WorkYears': '/',
        'GraduationTime': '/',
        'GraduationSchool': '/',
        'Major': '/',
        'HighestEducation': '/',
        'Department': '/',
        'Title': '/',
        'PersonalProfile': '/',
        'BusinessAbility': '/',
        'Certification': '/',
        'Training': '/',
        'SkillTag': '/'
    }
    
    # 确保文档有表格
    if not doc.tables:
        logger.warning("文档中没有表格")
        return basic_info
    
    # 关键词映射，支持多个关键词变体
    keyword_mapping = {
        'Name': ['姓名', '姓名：', "姓    名"],
        'WorkYears': ['工作年限'],
        'GraduationTime': ['毕业时间'],
        'GraduationSchool': ['毕业学校'],
        'Major': ['专业', '所学专业', "专    业"],
        'HighestEducation': ['最高学历', '学历'],
        'Department': ['所在部门', '部门'],
        'Title': ['职称','职    称'],
        'PersonalProfile': ['个人简介', '个人概述', '自我介绍'],
        'BusinessAbility': ['业务与技术能力', '业务与技术能力详述', '技术能力', '技能'],
        'Certification': ['资质认证', '证书', '认证'],
        'Training': ['参与培训', '培训经历'],
        'SkillTag': ['技能标签', '技术栈']
    }
    
    # 遍历所有表格查找信息
    for table_idx, table in enumerate(doc.tables):
        logger.debug(f"开始处理表格 {table_idx+1}")
        
        for r, row in enumerate(table.rows):
            for c, cell in enumerate(row.cells):
                # 检查每个字段的所有可能关键词
                for field, keywords in keyword_mapping.items():
                    # 如果该字段已经找到值，则跳过
                    if basic_info[field] != '/':
                        continue
                    
                    # 尝试所有可能的关键词变体
                    for keyword in keywords:
                        if getPosCell(table, r, c, keyword):
                            # 对HighestEducation字段进行特殊处理，检查关键词后是否有冒号
                            cell_text = cell.text.strip()
                            if field == 'HighestEducation':
                                # 仅匹配完全等于关键词的单元格
                                if cell_text != keyword:
                                    # 检查是否以关键词加冒号开头
                                    pattern = re.compile(rf'^{re.escape(keyword)}[:：]')
                                    if pattern.search(cell_text):
                                        logger.debug(f"跳过包含冒号的{field}关键词: {cell_text}")
                                    else:
                                        logger.debug(f"跳过非精确匹配的{field}关键词: {cell_text}")
                                    continue
                            
                            # 尝试获取右侧单元格的值，跳过与关键词相同的内容
                            found_value = None
                            current_col = c + 1
                            
                            # 向右查找直到找到有效值或到达行尾
                            while current_col < len(row.cells):
                                candidate_value = table.rows[r].cells[current_col].text.strip()
                                
                                # 如果候选值与任何关键词变体都不相同，则认为是有效值
                                is_keyword = False
                                for keyword_variant in keywords:
                                    if clean_keyword(candidate_value) == clean_keyword(keyword_variant):
                                        is_keyword = True
                                        break
                                
                                if candidate_value and not is_keyword:
                                    found_value = candidate_value
                                    break
                                
                                current_col += 1
                            
                            if found_value:
                                basic_info[field] = found_value
                                logger.debug(f"在表格 {table_idx+1} 行 {r+1} 列 {current_col+1} 找到 {field}: {found_value}")
                                break
                            
                            # 如果没有找到右侧的有效值，尝试获取同一单元格中的内容（关键词后面的部分）
                            else:
                                cell_text = cell.text.strip()
                                # 尝试提取关键词后面的内容
                                for keyword_variant in keywords:
                                    if keyword_variant in cell_text:
                                        # 对HighestEducation字段进行特殊处理
                                        if field == 'HighestEducation':
                                            # 仅匹配完全等于关键词的单元格
                                            if cell_text != keyword_variant:
                                                # 检查是否以关键词加冒号开头
                                                pattern = re.compile(rf'^{re.escape(keyword_variant)}[:：]')
                                                if pattern.search(cell_text):
                                                    logger.debug(f"跳过包含冒号的{field}关键词: {cell_text}")
                                                else:
                                                    logger.debug(f"跳过非精确匹配的{field}关键词: {cell_text}")
                                                continue
                                        
                                        # 尝试提取关键词后面的内容
                                        try:
                                            value = cell_text.split(keyword_variant, 1)[1].strip()
                                            if value:
                                                # 检查提取的值是否不是关键词
                                                is_extracted_keyword = False
                                                for kw in keywords:
                                                    if clean_keyword(value) == clean_keyword(kw):
                                                        is_extracted_keyword = True
                                                        break
                                                
                                                if not is_extracted_keyword:
                                                    basic_info[field] = value
                                                    logger.debug(f"在表格 {table_idx+1} 行 {r+1} 列 {c+1} 同一单元格中找到 {field}: {value}")
                                                    break
                                        except:
                                            pass
                    
        # 如果所有基本信息都已找到，可以提前退出
        if all(value != '/' for value in basic_info.values()):
            logger.debug("所有基本信息已找到，提前退出")
            break
    
    # 在最后处理特定字段中的中文冒号替换为英文冒号
    fields_to_process = ['GraduationTime', 'GraduationSchool', 'Major']
    for field in fields_to_process:
        if basic_info[field] != '/':
            # 将中文冒号替换为英文冒号
            basic_info[field] = basic_info[field].replace('：', ':').replace('\n', '|')
            logger.debug(f"已将 {field} 中的中文冒号替换为英文冒号: {basic_info[field]}")
    
    # 按照要求不对学历进行处理
    return basic_info


def _parse_date(date_str: str) -> datetime:
    """尝试多种格式解析日期"""
    if date_str.lower() in ['至今', 'current', 'now', 'present']:
        return datetime.now()
    
    for fmt in DATE_FORMATS:
        try:
            # 如果只有年份，添加月份
            if fmt == '%Y':
                date_str = f"{date_str}/01"
                fmt = '%Y/%m'
            
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # 如果无法解析，返回当前时间
    logger.warning(f"无法解析日期格式: {date_str}，使用当前时间替代")
    return datetime.now()

def calculate_months(start_str, end_str):
    """计算月份差"""
    # 处理空字符串的情况
    if not start_str:
        return 0
    
    try:
        # 处理"至今"的情况
        end = _parse_date(end_str) 
        start = _parse_date(start_str)
        
        # 如果两个都是"至今"，返回0
        if start_str == '至今' and end_str == '至今':
            return 0
        
        delta = relativedelta(end, start)
        return max(delta.years * 12 + delta.months, 0)
    except Exception as e:
        logger.warning(f"计算月份差时出错: {e}")
        return 0

def convert_to_template_format(raw_resume_data, emp_no):
    """
    将原始简历数据转换为模板格式，符合参考文件结构
    以姓名为键，内部包含标准字段结构，使用驼峰命名法
    不对学历进行处理
    
    Args:
        raw_resume_data: 原始提取的简历数据
        emp_no: 员工工号
        
    Returns:
        dict: 模板格式数据，以姓名为键，内部包含标准字段结构和正确的字段命名
    """
    # 获取原始数据中的基本信息
    basic_info = raw_resume_data.get('BasicInfo', {})
    
    # 提取姓名
    person_name = basic_info.get('Name', '')
    if not person_name or person_name.strip() == '':
        person_name = 'Unknown'
    
    # 字段名称映射表
    basic_info_mapping = {
        'EmpNo': 'EmpNo',
        'Name': 'Name',
        'WorkYears': 'WorkYears',
        'GraduationTime': 'GraduationTime',
        'GraduationSchool': 'GraduationSchool',
        'Major': 'Major',
        'HighestEducation': 'HighestEducation',
        'Department': 'Department',
        'Title': 'Title',
        'PersonalProfile': 'PersonalProfile',
        'EmpNo': 'EmpNo'
    }
    
    work_ability_mapping = {
        'BusinessAbility': 'BusinessAbility',
        'Certification': 'Certification',
        'Training': 'Training',
        'SkillTag': 'SkillTag'
    }
    
    # 转换BasicInfo字段名称
    formatted_basic_info = {}
    for old_key, new_key in basic_info_mapping.items():
        if old_key in basic_info:
            formatted_basic_info[new_key] = basic_info[old_key]
    
    # 确保包含EmpNo字段
    if emp_no:
        formatted_basic_info['EmpNo'] = emp_no
    
    # 转换WorkAbility字段名称
    work_ability = raw_resume_data.get('WorkAbility', {})
    formatted_work_ability = {}
    for old_key, new_key in work_ability_mapping.items():
        if old_key in work_ability:
            formatted_work_ability[new_key] = work_ability[old_key]
    
    # 构建标准结构的数据
    template_data = {
        person_name: {
            'BasicInfo': formatted_basic_info,
            'WorkExperience': raw_resume_data.get('WorkExperience', []),
            'ProjectExperience': raw_resume_data.get('ProjectExperience', []),
            'WorkAbility': formatted_work_ability
        }
    }
    
    return template_data

def extract_work_experience(doc):
    """
    提取工作经历信息
    
    Args:
        doc: Document对象
    
    Returns:
        list: 工作经历列表，每个元素为字典
    """
    work_experience = []
    
    # 关键词映射
    section_keywords = ['工作经历（由近至远）']
    project_keywords = ['项目经历（由近至远）']
    
    # 遍历所有表格
    for table_idx, table in enumerate(doc.tables):
        logger.debug(f"处理表格 {table_idx+1} 中的工作经历")
        
        # 查找包含工作经历关键词的单元格
        found_section = False
        start_row = -1
        
        # 同时查找项目经历位置作为终止点
        project_end_row = len(table.rows)  # 默认到表格末尾
        
        # 先查找项目经历的位置
        for r, row in enumerate(table.rows):
            for c, cell in enumerate(row.cells):
                for keyword in project_keywords:
                    if clean_keyword(keyword) in clean_keyword(cell.text):
                        project_end_row = r
                        logger.debug(f"在表格 {table_idx+1} 行 {r+1} 找到项目经历部分，作为工作经历的终止位置")
                        break
        
        # 再查找工作经历的开始位置
        for r, row in enumerate(table.rows):
            for c, cell in enumerate(row.cells):
                for keyword in section_keywords:
                    if clean_keyword(keyword) in clean_keyword(cell.text):
                        found_section = True
                        start_row = r
                        logger.debug(f"在表格 {table_idx+1} 行 {r+1} 找到工作经历部分")
                        break
                if found_section:
                    break
            if found_section:
                break
        
        if found_section:
            # 尝试提取表头
            header_row = None
            for r in range(start_row + 1, project_end_row):  # 只搜索到项目经历前
                if len(table.rows[r].cells) >= 5:  # 假设至少有5列
                    header_row = r
                    break
            
            if header_row:
                # 识别列名
                col_mapping = {}
                for c, cell in enumerate(table.rows[header_row].cells):
                    cell_text = clean_keyword(cell.text)
                    logger.debug(f"表头单元格内容: {cell_text}")
                    if '开始时间' in cell_text:
                        col_mapping['StartTime'] = c
                    elif '结束时间' in cell_text:
                        col_mapping['EndTime'] = c
                    elif '公司名称' in cell_text or '单位' in cell_text:
                        col_mapping['CompanyName'] = c
                    elif '担任职务' in cell_text or '岗位' in cell_text:
                        col_mapping['Position'] = c
                    elif '工作职责说明' in cell_text or '工作职责说明（稍微详细一点）' in cell_text or '职责' in cell_text:
                        col_mapping['JobDescription'] = c
                
                # 提取数据行，只提取到项目经历之前
                for r in range(header_row + 1, project_end_row):
                    row_data = table.rows[r].cells
                    # 检查col_mapping是否为空
                    if not col_mapping:
                        continue
                    if len(row_data) >= max(col_mapping.values()) + 1 if col_mapping else False:
                        start_time = row_data[col_mapping.get('StartTime', 0)].text.strip() if 'StartTime' in col_mapping else ''
                        end_time = row_data[col_mapping.get('EndTime', 1)].text.strip() if 'EndTime' in col_mapping else ''
                        exp = {
                            'StartTime': start_time,
                            'EndTime': end_time,
                            'CompanyName': row_data[col_mapping.get('CompanyName', 0)].text.strip() if 'CompanyName' in col_mapping else '',
                            'Position': row_data[col_mapping.get('Position', 1)].text.strip() if 'Position' in col_mapping else '',
                            'JobDescription': row_data[col_mapping.get('JobDescription', 2)].text.strip() if 'JobDescription' in col_mapping else '',
                            'Duration': calculate_months(start_time, end_time)
                        }
                        # 只添加有效数据
                        if any(exp.values()):
                            work_experience.append(exp)
                            logger.debug(f"提取到工作经历: {exp}")
    
    return work_experience

def extract_project_experience(doc):
    """
    提取项目经历信息
    
    Args:
        doc: Document对象
    
    Returns:
        list: 项目经历列表，每个元素为字典
    """
    project_experience = []
    
    # 关键词映射
    section_keywords = ['项目经历（由近至远）']
    end_section_keywords = ['能力与资质']
    
    # 遍历所有表格
    for table_idx, table in enumerate(doc.tables):
        logger.debug(f"处理表格 {table_idx+1} 中的项目经历")
        
        # 查找包含项目经历关键词的单元格
        found_section = False
        start_row = -1
        
        # 同时查找"能力与资质"位置作为终止点
        end_row = len(table.rows)  # 默认到表格末尾
        
        # 先查找"能力与资质"的位置
        for r, row in enumerate(table.rows):
            for c, cell in enumerate(row.cells):
                for keyword in end_section_keywords:
                    if clean_keyword(keyword) in clean_keyword(cell.text):
                        end_row = r
                        logger.debug(f"在表格 {table_idx+1} 行 {r+1} 找到'能力与资质'部分，作为项目经历的终止位置")
                        break
        
        # 再查找项目经历的开始位置
        for r, row in enumerate(table.rows):
            for c, cell in enumerate(row.cells):
                for keyword in section_keywords:
                    if clean_keyword(keyword) in clean_keyword(cell.text):
                        found_section = True
                        start_row = r
                        logger.debug(f"在表格 {table_idx+1} 行 {r+1} 找到项目经历部分")
                        break
                if found_section:
                    break
            if found_section:
                break
        
        if found_section:
            # 尝试提取表头
            header_row = None
            for r in range(start_row + 1, end_row):  # 只搜索到"能力与资质"前
                if len(table.rows[r].cells) >= 5:  # 假设至少有4列
                    header_row = r
                    break

            if header_row:
                # 识别列名
                col_mapping = {}
                for c, cell in enumerate(table.rows[header_row].cells):
                    cell_text = clean_keyword(cell.text)
                    #print(f"表头单元格内容: {cell_text}")
                    logger.debug(f"表头单元格内容: {cell_text}")
                    if '开始时间' in cell_text:
                        col_mapping['StartTime'] = c
                    elif '结束时间' in cell_text:
                        col_mapping['EndTime'] = c
                    elif '项目名称' in cell_text:
                        col_mapping['ProjectName'] = c
                    elif '项目角色' in cell_text:
                        col_mapping['ProjectRole'] = c
                    elif '项目职责说明' in cell_text or '项目职责说明（稍微详细一点）' in cell_text or '项目描述' in cell_text:
                        col_mapping['JobDescription'] = c
                
                # 提取数据行，只提取到"能力与资质"之前
                for r in range(header_row + 1, end_row):
                    row_data = table.rows[r].cells
                    # 检查col_mapping是否为空
                    if not col_mapping:
                        continue
                    if len(row_data) >= max(col_mapping.values()) + 1 if col_mapping else False:
                        start_time = row_data[col_mapping.get('StartTime', 0)].text.strip() if 'StartTime' in col_mapping else ''
                        end_time = row_data[col_mapping.get('EndTime', 1)].text.strip() if 'EndTime' in col_mapping else ''
                        exp = {
                            'StartTime': start_time,
                            'EndTime': end_time,
                            'ProjectName': row_data[col_mapping.get('ProjectName', 0)].text.strip() if 'ProjectName' in col_mapping else '',
                            'ProjectRole': row_data[col_mapping.get('ProjectRole', 1)].text.strip() if 'ProjectRole' in col_mapping else '',
                            'JobDescription': row_data[col_mapping.get('JobDescription', 2)].text.strip() if 'JobDescription' in col_mapping else '',
                            'Duration': calculate_months(start_time, end_time)
                        }
                        # 只添加有效数据，并且过滤掉明显不是项目经历的数据（比如包含"能力与资质"等关键词的行）
                        if any(exp.values()):
                            # 检查是否包含终止关键词
                            contains_end_keyword = False
                            for cell in row_data:
                                cell_text = clean_keyword(cell.text)
                                for keyword in end_section_keywords:
                                    if keyword in cell_text:
                                        contains_end_keyword = True
                                        break
                                if contains_end_keyword:
                                    break
                            
                            if not contains_end_keyword:
                                project_experience.append(exp)
                                logger.debug(f"提取到项目经历: {exp}")
    
    return project_experience

def extract_resume_alt(file_path):
    """
    备用简历数据提取函数 - 只传入简历路径
    
    Args:
        file_path: 简历文件路径
    
    Returns:
        bool: 是否成功提取
    """
    try:
        # 处理PyInstaller打包后的路径问题
        if getattr(sys, 'frozen', False):
            # 打包后的环境
            base_dir = os.path.dirname(sys.executable)
            # 确保工作目录设置为当前目录（exe所在目录）
            os.chdir(base_dir)
        else:
            # 开发环境
            # 获取项目根目录（package 的上两级）
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        # 添加项目根目录到Python路径，以便能够导入package模块
        sys.path.append(base_dir)
        
        # 检查文件格式
        is_valid, file_path = check_file_format(file_path)
        if not is_valid:
            logger.error(f"无效的文件格式: {file_path}")
            return False
        
        # 读取docx文件
        doc = Document(file_path)
        filename = os.path.basename(file_path)
        
        # 提取工号
        emp_no = extract_emp_no_from_filename(file_path)
        
        # 提取基本信息
        basic_info = extract_basic_info(doc, filename)
        
        # 提取工作经历
        work_experience = extract_work_experience(doc)
        
        # 提取项目经历
        project_experience = extract_project_experience(doc)
        
        # 创建原始提取数据结构
        raw_resume_data = {
            'BasicInfo': {
                'EmpNo': emp_no,
                'Name': basic_info['Name'],
                'WorkYears': basic_info['WorkYears'],
                'GraduationTime': basic_info['GraduationTime'],
                'GraduationSchool': basic_info['GraduationSchool'],
                'Major': basic_info['Major'],
                'HighestEducation': basic_info['HighestEducation'],
                'Department': basic_info['Department'],
                'Title': basic_info['Title'],
                'PersonalProfile': basic_info['PersonalProfile']
            },
            'WorkExperience': work_experience,
            'ProjectExperience': project_experience,
            'WorkAbility': {
                'BusinessAbility': basic_info['BusinessAbility'],
                'Certification': basic_info['Certification'],
                'Training': basic_info['Training'],
                'SkillTag': basic_info['SkillTag']
            }
        }
        
        # 转换为模板格式（传入工号）
        template_formatted_data = convert_to_template_format(raw_resume_data, emp_no)
        
        # 获取人员姓名
        person_name = basic_info['Name'] if basic_info['Name'] != '/' else 'Unknown'
        
        # 输出结果
        print(f"【{person_name}的简历 - 原始提取数据】")
        print(json.dumps(raw_resume_data, ensure_ascii=False, indent=4))
        
        print(f"\n【{person_name}的简历 - 模板格式数据】")
        template_json = json.dumps(template_formatted_data, ensure_ascii=False, indent=4)
        
        # 创建输出目录
        #original_dir = os.path.join(base_dir, "output", "original_json")
        modify_dir = os.path.join(base_dir, "output", "modify_json")
        #os.makedirs(original_dir, exist_ok=True)
        os.makedirs(modify_dir, exist_ok=True)
        
        # 获取原始文件名（去掉扩展名）
        original_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # 保存原始提取结果（文件名格式：工号_姓名_人员简历.json）
        # raw_output_filename = os.path.join(original_dir, f"{emp_no}_{person_name}_人员简历.json")
        # with open(raw_output_filename, "w", encoding="utf-8") as f:
        #     json.dump(raw_resume_data, f, ensure_ascii=False, indent=2)
        # print(f"[OK] 已保存{person_name}的简历原始提取数据到：{raw_output_filename}")
        
        # 保存模板格式数据（文件名格式：工号_姓名_人员简历.json）
        modify_output_filename = os.path.join(modify_dir, f"{emp_no}_{person_name}_人员简历.json")
        with open(modify_output_filename, "w", encoding="utf-8") as f:
            f.write(template_json)
        print(f"[OK] 已保存{person_name}的简历模板格式数据到：{modify_output_filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"提取简历数据时出错: {str(e)}", exc_info=True)
        return False



def main():
    """
    主函数
    """
    import argparse
    parser = argparse.ArgumentParser(description='备用简历数据提取工具')
    parser.add_argument('file_path', help='简历文件路径')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志信息')
    parser.add_argument('--test', action='store_true', help='运行测试函数')
    args = parser.parse_args()
    
    # 如果启用详细日志，设置日志级别为DEBUG
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 如果是测试模式，运行测试函数
    if args.test:
        test_functions()
        sys.exit(0)
    
    # 执行提取
    success = extract_resume_alt(args.file_path)
    
    # 返回适当的退出码
    sys.exit(0 if success else 1)


def batch_extract_resume_alt(input_dir):
    """
    批量提取目录中的所有简历文件
    
    Args:
        input_dir: 输入目录
    
    Returns:
        dict: 处理统计信息
    """
    # 验证输入目录是否存在
    if not os.path.exists(input_dir):
        logger.error(f"输入目录不存在: {input_dir}")
        return {'error': '输入目录不存在'}
    
    stats = {
        'total': 0,
        'success': 0,
        'failed': 0,
        'failed_files': []
    }
    
    # 获取目录中的所有docx文件
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith(('.docx', '.doc')):
                file_path = os.path.join(root, file)
                stats['total'] += 1
                logger.info(f"开始处理文件 {file_path} ({stats['total']})")
                
                if extract_resume_alt(file_path):
                    stats['success'] += 1
                else:
                    stats['failed'] += 1
                    stats['failed_files'].append(file_path)
    
    # 输出统计信息
    logger.info(f"批量处理完成：总文件数 {stats['total']}, 成功 {stats['success']}, 失败 {stats['failed']}")
    if stats['failed'] > 0:
        logger.warning(f"失败的文件列表：{stats['failed_files']}")
    
    return stats


def validate_output_json(json_file):
    """
    验证输出的JSON文件格式是否正确
    
    Args:
        json_file: JSON文件路径
    
    Returns:
        dict: 验证结果
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 检查原始JSON文件格式
        if 'original_json' in json_file and isinstance(data, dict):
            required_keys = ['BasicInfo', 'WorkExperience', 'ProjectExperience', 'WorkAbility']
            missing_keys = [key for key in required_keys if key not in data]
            if missing_keys:
                return {'valid': False, 'error': f'原始JSON缺少必要的键: {missing_keys}'}
        
        # 检查modify_json文件格式（以姓名为键的嵌套格式）
        elif 'modify_json' in json_file and isinstance(data, dict):
            # 确保至少有一个键（姓名）
            if not data:
                return {'valid': False, 'error': '修改后的JSON为空，没有找到姓名键'}
            
            # 检查第一个值是否包含必要的键
            first_name = list(data.keys())[0]
            nested_data = data.get(first_name, {})
            
            if isinstance(nested_data, dict):
                required_keys = ['BasicInfo', 'WorkExperience', 'ProjectExperience', 'WorkAbility']
                missing_keys = [key for key in required_keys if key not in nested_data]
                if missing_keys:
                    return {'valid': False, 'error': f'嵌套数据缺少必要的键: {missing_keys}'}
        
        return {'valid': True, 'data': data}
    except json.JSONDecodeError as e:
        return {'valid': False, 'error': f'JSON格式错误: {str(e)}'}
    except Exception as e:
        return {'valid': False, 'error': f'验证失败: {str(e)}'}


# 添加一个命令行入口点，支持批量处理
def batch_main():
    """
    批量处理的命令行入口
    """
    import argparse
    parser = argparse.ArgumentParser(description='批量备用简历数据提取工具')
    parser.add_argument('input_dir', help='简历文件输入目录')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志信息')
    args = parser.parse_args()
    
    # 如果启用详细日志，设置日志级别为DEBUG
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # 执行批量提取
    stats = batch_extract_resume_alt(args.input_dir)
    
    # 返回适当的退出码
    sys.exit(0 if stats.get('failed', 0) == 0 else 1)


if __name__ == "__main__":
    # 自动检测是单文件处理还是批量处理
    import sys
    
    # 如果参数长度大于1且第二个参数是目录，则执行批量处理
    if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
        batch_main()
    else:
        # 否则执行单文件处理
        main()