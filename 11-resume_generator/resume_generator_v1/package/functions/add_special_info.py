#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
在执行additionInfo更新动作之后，对modify_json数据进行特殊字段处理
功能：
1. 在modify_json中新增SpecialInfo对象
2. 实现学位(Degree)字段的转码逻辑
3. 实现参加工作时间字段的提取与格式化
"""

import os
import json
import re
from datetime import datetime
from typing import Dict, Optional

def process_special_info(json_file_path: str) -> bool:
    """
    处理JSON文件的特殊字段信息
    
    参数:
        json_file_path: JSON文件路径
    
    返回:
        bool: 处理是否成功
    """
    try:
        # 读取JSON文件
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 获取人员名称（假设JSON根对象只有一个键）
        if not data or not isinstance(data, dict):
            return False
        
        person_name = list(data.keys())[0]
        person_data = data[person_name]
        
        # 初始化SpecialInfo对象
        if "SpecialInfo" not in person_data:
            person_data["SpecialInfo"] = {}
        elif not isinstance(person_data["SpecialInfo"], dict):
            person_data["SpecialInfo"] = {}
        
        special_info = person_data["SpecialInfo"]
        
        # 处理学位(Degree)字段
        if "AdditionInfo" in person_data and person_data["AdditionInfo"]:
            highest_education = person_data["AdditionInfo"].get("HighestEducation", "")
            
            # 学位转码逻辑
            if highest_education == "大专":
                special_info["Degree"] = "/"
            elif highest_education == "专科":
                special_info["Degree"] = "/"
            elif highest_education == "本科":
                special_info["Degree"] = "学士"
            elif highest_education == "研究生":
                special_info["Degree"] = "硕士"
            elif highest_education:
                special_info["Degree"] = highest_education
        
        # 处理参加工作时间(StartWorkDate)字段
        if "WorkExperience" in person_data and isinstance(person_data["WorkExperience"], list):
            work_experiences = person_data["WorkExperience"]
            
            if work_experiences:
                # 获取最后一个工作经历的开始时间
                last_experience = work_experiences[-1]
                start_time = last_experience.get("StartTime", "")

                if start_time:
                    # 格式化时间为"YYYY年MM月"
                    formatted_date = format_work_date(start_time)
                    if formatted_date:
                        special_info["StartWorkDate"] = formatted_date
        
        # 处理学历信息 - 修改后的逻辑
        if "BasicInfo" in person_data and person_data["BasicInfo"]:
            # 初始化教育经历列表
            special_info["EducationList"] = []
            
            # 提取所有学历相关字段
            graduation_time = person_data["BasicInfo"].get("GraduationTime", "")
            graduation_school = person_data["BasicInfo"].get("GraduationSchool", "")
            major = person_data["BasicInfo"].get("Major", "")
            highest_education = person_data["BasicInfo"].get("HighestEducation", "")
            
            # 解析新的格式：最高学历:xxxx|第一学历:xxxx
            def parse_education_field(field_value):
                """解析教育相关字段，返回字典 {学历类型: 值}"""
                result = {}
                if field_value and field_value != '/':
                    # 检查是否是新的格式（包含"最高学历:"和"|"）
                    if '最高学历:' in field_value and '|' in field_value:
                        # 按竖线分割不同的学历类型
                        parts = field_value.split('|')
                        for part in parts:
                            # 按冒号分割类型和值
                            if ':' in part:
                                edu_type, edu_value = part.split(':', 1)
                                result[edu_type.strip()] = edu_value.strip()
                    else:
                        # 如果是单独的值，默认为最高学历
                        result["最高学历"] = field_value
                return result
            
            # 解析各个字段
            graduation_time_dict = parse_education_field(graduation_time)
            graduation_school_dict = parse_education_field(graduation_school)
            major_dict = parse_education_field(major)
            highest_education_dict = parse_education_field(highest_education)
            
            # 获取所有存在的学历类型
            all_edu_types = set()
            all_edu_types.update(graduation_time_dict.keys())
            all_edu_types.update(graduation_school_dict.keys())
            all_edu_types.update(major_dict.keys())
            all_edu_types.update(highest_education_dict.keys())
            
            # 如果没有检测到任何学历类型，默认使用最高学历
            if not all_edu_types:
                all_edu_types = ["最高学历"]
            
            # 为每种学历类型创建教育经历条目
            for edu_type in all_edu_types:
                special_info["EducationList"].append({
                    "DegreeType": edu_type,
                    "GraduationTime": graduation_time_dict.get(edu_type, ""),
                    "GraduationSchool": graduation_school_dict.get(edu_type, ""),
                    "Major": major_dict.get(edu_type, ""),
                    "HighestEducation": highest_education_dict.get(edu_type, "")
                })

        # 写回文件
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        # 验证SpecialInfo是否成功添加
        return validate_special_info(json_file_path)
        
    except Exception as e:
        print(f"处理特殊字段时出错: {str(e)}")
        return False

def format_work_date(date_str: str) -> Optional[str]:
    """
    格式化工作时间为"YYYY年MM月"格式
    
    参数:
        date_str: 原始日期字符串
    
    返回:
        Optional[str]: 格式化后的日期字符串，失败返回None
    """
    # 尝试多种日期格式解析
    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y年%m月%d日",
        "%Y-%m",
        "%Y/%m",
        "%Y.%m",
        "%Y年%m月",
        "%Y"
    ]
    
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            # 格式化为"YYYY年MM月"
            return f"{date_obj.year}年{date_obj.month:02d}月" if fmt != "%Y" else f"{date_obj.year}年01月"
        except ValueError:
            continue
    
    # 尝试正则表达式提取年份和月份
    year_pattern = r'(19|20)\d{2}'
    month_pattern = r'(0[1-9]|1[0-2])'
    
    year_match = re.search(year_pattern, date_str)
    month_match = re.search(month_pattern, date_str)
    
    if year_match:
        year = year_match.group(0)
        month = month_match.group(0) if month_match else "01"
        return f"{year}年{month}月"
    
    return None

def validate_special_info(json_file_path: str) -> bool:
    """
    验证SpecialInfo字段是否成功添加
    
    参数:
        json_file_path: JSON文件路径
    
    返回:
        bool: 验证是否通过
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data or not isinstance(data, dict):
            return False
        
        person_name = list(data.keys())[0]
        person_data = data[person_name]
        
        # 检查SpecialInfo是否存在且为字典
        if "SpecialInfo" not in person_data or not isinstance(person_data["SpecialInfo"], dict):
            return False
        
        return True
    except Exception:
        return False

def process_directory(json_dir: str, log_func=None) -> Dict:
    """
    批量处理目录中的所有JSON文件
    
    参数:
        json_dir: JSON文件所在目录
        log_func: 可选的日志函数
    
    返回:
        Dict: 处理结果统计
    """
    # 初始化统计信息
    stats = {
        "total_files": 0,
        "processed_count": 0,
        "failed_count": 0,
        "success_count": 0
    }
    
    # 默认日志函数
    def log(msg):
        if log_func:
            log_func(msg)
        else:
            print(msg)
    
    # 检查目录是否存在
    if not os.path.exists(json_dir):
        log(f"错误: 目录不存在: {json_dir}")
        return stats
    
    # 获取JSON文件列表
    json_files = [f for f in os.listdir(json_dir) if f.endswith(".json")]
    stats["total_files"] = len(json_files)
    log(f"找到 {len(json_files)} 个JSON文件需要处理")
    
    # 处理每个文件
    for json_file in json_files:
        file_path = os.path.join(json_dir, json_file)
        stats["processed_count"] += 1
        
        if process_special_info(file_path):
            stats["success_count"] += 1
            log(f"  成功: {json_file}")
        else:
            stats["failed_count"] += 1
            log(f"  失败: {json_file}")
    
    log(f"===== 特殊字段处理完成 =====")
    log(f"总计: {stats['total_files']} 个文件")
    log(f"成功: {stats['success_count']} 个文件")
    log(f"失败: {stats['failed_count']} 个文件")
    
    return stats

if __name__ == "__main__":
    # 示例用法
    import sys
    if len(sys.argv) >= 2:
        json_dir = sys.argv[1]
        process_directory(json_dir)
    else:
        print("用法: python add_special_info.py <json_directory>")