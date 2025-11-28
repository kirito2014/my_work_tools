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

# # 导入项目中的工具函数
# try:
#     from .tools import is_real_docx, find_pos_bgn_end_r
# except ImportError:
#     # 如果相对导入失败，尝试绝对导入
#     import sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from tools import is_real_docx, find_pos_bgn_end_r

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
        'empl_ID': extract_emp_no_from_filename(filename),
        'name': '/',
        'work_years': '/',
        'grad_date': '/',
        'grad_school': '/',
        'major': '/',
        'high_Edu': '/',
        'high_Degree': '/',  # 保留字段但不做处理
        'department': '/',
        'Jop_Title': '/',
        'Per_Profile': '/',
        'tech_skill': '/',
        'credential': '/',
        'training': '/',
        'Skill_tags': '/'
    }
    
    # 确保文档有表格
    if not doc.tables:
        logger.warning("文档中没有表格")
        return basic_info
    
    # 关键词映射，支持多个关键词变体
    keyword_mapping = {
        'name': ['姓名', '姓名：',"姓    名"],
        'work_years': ['工作年限'],
        'grad_date': ['毕业时间'],
        'grad_school': ['毕业学校'],
        'major': ['专业', '所学专业',"专    业"],
        'high_Edu': ['最高学历', '学历'],
        'department': ['所在部门', '部门'],
        'Jop_Title': ['职称', '职位', '岗位',"职    称"],
        'Per_Profile': ['个人简介', '个人概述', '自我介绍'],
        'tech_skill': ['业务与技术能力', '业务与技术能力详述', '技术能力', '技能'],
        'credential': ['资质认证', '证书', '认证'],
        'training': ['参与培训', '培训经历'],
        'Skill_tags': ['技能标签', '技术栈']
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
                            # 尝试获取下一个单元格的值
                            if c + 1 < len(row.cells):
                                value = table.rows[r].cells[c+1].text.strip()
                                if value:  # 确保值不为空
                                    basic_info[field] = value
                                    logger.debug(f"在表格 {table_idx+1} 行 {r+1} 列 {c+1} 找到 {field}: {value}")
                                    break
                            # 如果没有下一个单元格，尝试获取同一单元格中的内容（关键词后面的部分）
                            else:
                                cell_text = cell.text.strip()
                                # 尝试提取关键词后面的内容
                                for keyword_variant in keywords:
                                    if keyword_variant in cell_text:
                                        # 尝试提取关键词后面的内容
                                        try:
                                            value = cell_text.split(keyword_variant, 1)[1].strip()
                                            if value:
                                                basic_info[field] = value
                                                logger.debug(f"在表格 {table_idx+1} 行 {r+1} 列 {c+1} 同一单元格中找到 {field}: {value}")
                                                break
                                        except:
                                            pass
                    
        # 如果所有基本信息都已找到，可以提前退出
        if all(value != '/' for value in basic_info.values()):
            logger.debug("所有基本信息已找到，提前退出")
            break
    
    # 按照要求不对学历进行处理
    return basic_info


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
    person_name = basic_info.get('name', '')
    if not person_name or person_name.strip() == '':
        person_name = 'Unknown'
    
    # 字段名称映射表
    basic_info_mapping = {
        'empl_ID': 'EmpNo',
        'name': 'Name',
        'work_years': 'WorkYears',
        'grad_date': 'GraduationTime',
        'grad_school': 'GraduationSchool',
        'major': 'Major',
        'high_Edu': 'HighestEducation',
        'high_Degree': 'Degree',
        'department': 'Department',
        'Jop_Title': 'Title',
        'Per_Profile': 'PersonalProfile',
        'EmpNo': 'EmpNo'
    }
    
    work_ability_mapping = {
        'tech_skill': 'BusinessAbility',
        'credential': 'Certification',
        'training': 'Training',
        'Skill_tags': 'SkillTag'
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
            'WorkAbility': formatted_work_ability,
            'AdditionInfo': raw_resume_data.get('AdditionInfo', {}),
            'SpecialInfo': raw_resume_data.get('SpecialInfo', {})
        }
    }
    
    return template_data

def extract_resume_alt(file_path):
    """
    备用简历数据提取函数 - 只传入简历路径
    
    Args:
        file_path: 简历文件路径
    
    Returns:
        bool: 是否成功提取
    """
    try:
        # 获取base_dir，确保在打包环境中输出到正确位置
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
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
        
        # 创建原始提取数据结构
        raw_resume_data = {
            'BasicInfo': {
                'empl_ID': emp_no,
                'name': basic_info['name'],
                'work_years': basic_info['work_years'],
                'grad_date': basic_info['grad_date'],
                'grad_school': basic_info['grad_school'],
                'major': basic_info['major'],
                'high_Edu': basic_info['high_Edu'],
                'high_Degree': basic_info['high_Degree'],
                'department': basic_info['department'],
                'Jop_Title': basic_info['Jop_Title'],
                'Per_Profile': basic_info['Per_Profile']
            },
            'WorkExperience': [],
            'ProjectExperience': [],
            'WorkAbility': {
                'tech_skill': basic_info['tech_skill'],
                'credential': basic_info['credential'],
                'training': basic_info['training'],
                'Skill_tags': basic_info['Skill_tags']
            }
        }
        
        # 转换为模板格式（传入工号）
        template_formatted_data = convert_to_template_format(raw_resume_data, emp_no)
        
        # 获取人员姓名
        person_name = basic_info['name'] if basic_info['name'] != '/' else 'Unknown'
        
        # 输出结果
        print(f"【{person_name}的简历 - 原始提取数据】")
        print(json.dumps(raw_resume_data, ensure_ascii=False, indent=4))
        
        print(f"\n【{person_name}的简历 - 模板格式数据】")
        template_json = json.dumps(template_formatted_data, ensure_ascii=False, indent=4)
        
        # 创建输出目录
        original_dir = os.path.join(base_dir, "output", "original_json")
        modify_dir = os.path.join(base_dir, "output", "modify_json")
        os.makedirs(original_dir, exist_ok=True)
        os.makedirs(modify_dir, exist_ok=True)
        
        # 获取原始文件名（去掉扩展名）
        original_filename = os.path.splitext(os.path.basename(file_path))[0]
        
        # 保存原始提取结果，保持文件名不变
        raw_output_filename = os.path.join(original_dir, f"{original_filename}.json")
        with open(raw_output_filename, "w", encoding="utf-8") as f:
            json.dump(raw_resume_data, f, ensure_ascii=False, indent=4)
        print(f"[OK] 已保存{person_name}的简历原始提取数据到：{raw_output_filename}")
        
        # 保存模板格式数据，保持文件名不变
        modify_output_filename = os.path.join(modify_dir, f"{original_filename}.json")
        with open(modify_output_filename, "w", encoding="utf-8") as f:
            json.dump(template_formatted_data, f, ensure_ascii=False, indent=4)
        print(f"[OK] 已保存{person_name}的简历模板格式数据到：{modify_output_filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"提取简历数据时出错: {str(e)}", exc_info=True)
        return False


def test_functions():
    """
    测试函数的正确性
    """
    print("=== 开始测试函数 ===")
    
    # 测试clean_keyword函数
    test_keywords = [
        "姓名：",
        "工作 年限",
        "毕业\n时间",
        "业务与技术能力\t详述",
        ""
    ]
    
    print("\n测试clean_keyword函数:")
    for keyword in test_keywords:
        cleaned = clean_keyword(keyword)
        print(f"原始: '{keyword}' -> 清理后: '{cleaned}'")
    
    # 由于不再处理学历，移除dilopma2degree测试
    
    print("\n=== 测试完成 ===")


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