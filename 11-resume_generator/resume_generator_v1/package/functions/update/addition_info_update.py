#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
统一处理简历JSON文件的AdditionInfo字段更新
功能：提供标准的接口来更新、验证和维护JSON文件中的AdditionInfo信息
支持从info_json目录或Excel文件更新AdditionInfo字段
"""

import os
import json
import traceback
from typing import List, Dict, Tuple, Optional

# 获取项目根目录
current_file = os.path.abspath(__file__)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
# 将项目根目录添加到Python路径
sys.path.append(project_root)

# 尝试导入需要的模块
import sys

def get_excel_module():
    """获取Excel处理模块，处理可能的导入错误"""
    try:
        from package.functions.excel_2_info_json import convert_excel_to_json
        return convert_excel_to_json
    except ImportError:
        return None

def get_file_helper():
    """获取文件操作助手模块，处理可能的导入错误"""
    try:
        from package.utils.file_helper import read_file, write_file
        return read_file, write_file
    except ImportError:
        # 提供基本的文件读写函数作为备用
        def read_file(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception:
                return None
        
        def write_file(file_path, content):
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                return True
            except Exception:
                return False
        
        return read_file, write_file

def update_addition_info_from_excel(excel_file: str, modify_dir: str, 
                                  log_func: Optional[callable] = None) -> Dict:
    """
    从Excel文件更新JSON文件的AdditionInfo字段
    
    参数:
        excel_file: Excel文件路径
        modify_dir: JSON文件所在目录
        log_func: 可选，日志记录函数
    
    返回:
        包含操作结果的字典
    """
    # 获取日志函数
    log = log_func if log_func else lambda x: None
    
    # 初始化结果
    result = {
        "success": False,
        "updated_count": 0,
        "skipped_count": 0,
        "error": None
    }
    
    try:
        log("  - [INFO] 开始从Excel读取数据...")
        
        # 获取Excel处理模块
        convert_excel_to_json = get_excel_module()
        if not convert_excel_to_json:
            result["error"] = "无法导入Excel处理模块"
            log(f"  - [ERROR] {result['error']}")
            return result
        
        # 读取Excel数据
        excel_data = convert_excel_to_json(excel_file)
        if not excel_data:
            result["error"] = "未成功读取Excel数据"
            log(f"  - [ERROR] {result['error']}")
            return result
        
        log(f"  - [INFO] 成功读取Excel数据，共{len(excel_data)}条记录")
        
        # 创建工号到员工数据的映射
        emp_map = {emp.get("EmpNo", "").zfill(5): emp for emp in excel_data}
        log(f"  - [INFO] 构建了{len(emp_map)}个员工信息映射")
        
        # 获取文件操作函数
        read_file, write_file = get_file_helper()
        
        # 更新每个JSON文件的AdditionInfo
        for filename in os.listdir(modify_dir):
            if filename.endswith(".json"):
                # 尝试从文件名提取工号
                emp_no = filename.split('_')[0]
                if emp_no in emp_map:
                    file_path = os.path.join(modify_dir, filename)
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
                                if write_file(file_path, updated_content):
                                    result["updated_count"] += 1
                                else:
                                    result["skipped_count"] += 1
                                    log(f"  - [WARNING] 无法写回文件: {filename}")
                            else:
                                result["skipped_count"] += 1
                                log(f"  - [WARNING] 跳过空文件: {filename}")
                        else:
                            result["skipped_count"] += 1
                            log(f"  - [WARNING] 无法读取文件: {filename}")
                    except Exception as e:
                        result["skipped_count"] += 1
                        log(f"  - [ERROR] 更新文件 {filename} 时出错: {str(e)}")
                    
        # 添加验证步骤
        log("  - [INFO] 开始验证更新结果...")
        validation_success = 0
        for filename in os.listdir(modify_dir):
            if filename.endswith(".json"):
                emp_no = filename.split('_')[0]
                if emp_no in emp_map:
                    file_path = os.path.join(modify_dir, filename)
                    try:
                        file_content = read_file(file_path)
                        if file_content:
                            resume_data = json.loads(file_content)
                            if resume_data and isinstance(resume_data, dict) and resume_data:
                                person_name = list(resume_data.keys())[0]
                                if (person_name in resume_data and 
                                    "AdditionInfo" in resume_data[person_name] and
                                    resume_data[person_name]["AdditionInfo"]):
                                    validation_success += 1
                    except Exception:
                        pass
        
        log(f"  - [INFO] 验证结果: {validation_success}/{result['updated_count']}")
        result["success"] = True
        
    except Exception as e:
        result["error"] = str(e)
        log(f"  - [ERROR] 处理Excel更新时发生异常: {str(e)}")
        log(f"  - [ERROR] 详细错误: {traceback.format_exc()}")
    
    return result

def update_addition_info_for_files(json_dir: str, person_names: Optional[List[str]] = None, 
                                  log_func: Optional[callable] = None) -> Dict:
    """
    批量更新JSON文件的AdditionInfo字段
    
    参数:
        json_dir: JSON文件所在目录
        person_names: 可选，员工编号列表，None表示所有文件
        log_func: 可选，日志记录函数
    
    返回:
        包含统计信息的字典
    """
    # 尝试导入特殊字段处理模块
    try:
        # 使用相对导入
        from ..add_special_info import process_special_info
        has_special_info_module = True
    except ImportError:
        # 如果相对导入失败，尝试绝对导入
        try:
            from package.functions.add_special_info import process_special_info
            has_special_info_module = True
        except ImportError:
            process_special_info = None
            has_special_info_module = False
    
    # 初始化统计信息
    stats = {
        "total_files": 0,
        "updated_count": 0,
        "skipped_count": 0,
        "validation_success": 0,
        "validation_failed": 0,
        "special_info_processed": 0,
        "special_info_failed": 0,
        "errors": []
    }
    
    # 日志记录函数
    def log(msg):
        if log_func:
            log_func(msg)
        else:
            print(msg)
    
    # 验证目录是否存在
    if not os.path.exists(json_dir):
        log(f"错误: JSON文件目录不存在: {json_dir}")
        stats["errors"].append(f"JSON文件目录不存在: {json_dir}")
        return stats
    
    # 获取JSON文件列表
    json_files = get_matching_json_files(json_dir, person_names)
    stats["total_files"] = len(json_files)
    log(f"找到 {len(json_files)} 个JSON文件需要处理")
    
    # 遍历处理每个文件
    for json_file in json_files:
        json_path = os.path.join(json_dir, json_file)
        try:
            # 读取JSON文件
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查并更新AdditionInfo字段
            updated = False
            if "AdditionInfo" not in data:
                data["AdditionInfo"] = {}
                updated = True
            elif not isinstance(data["AdditionInfo"], dict):
                data["AdditionInfo"] = {}
                updated = True
            
            # 如果有更新，写回文件
            if updated:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                stats["updated_count"] += 1
                log(f"  更新文件: {json_file}")
                
                # 处理特殊字段信息
                if has_special_info_module and process_special_info:
                    try:
                        if process_special_info(json_path):
                            stats["special_info_processed"] += 1
                            log(f"  特殊字段处理: {json_file} - 成功")
                        else:
                            stats["special_info_failed"] += 1
                            log(f"  特殊字段处理: {json_file} - 失败")
                    except Exception as si_e:
                        stats["special_info_failed"] += 1
                        log(f"  特殊字段处理: {json_file} - 出错: {si_e}")
            else:
                stats["skipped_count"] += 1
            
            # 验证更新结果
            if validate_addition_info(json_path):
                stats["validation_success"] += 1
            else:
                stats["validation_failed"] += 1
                log(f"  验证失败: {json_file}")
                
        except Exception as e:
            stats["errors"].append(f"处理文件 {json_file} 时出错: {str(e)}")
            log(f"  处理错误: {json_file} - {str(e)}")
    
    log(f"===== AdditionInfo信息更新完成 =====")
    log(f"成功更新: {stats['updated_count']} 个文件")
    log(f"跳过: {stats['skipped_count']} 个文件")
    log(f"验证结果 - 成功: {stats['validation_success']}, 失败: {stats['validation_failed']}")
    
    # 记录特殊字段处理结果
    if has_special_info_module and process_special_info:
        log(f"特殊字段处理 - 成功: {stats['special_info_processed']}, 失败: {stats['special_info_failed']}")
    
    return stats

def validate_addition_info(json_path: str) -> bool:
    """
    验证JSON文件中的AdditionInfo字段是否正确
    
    参数:
        json_path: JSON文件路径
    
    返回:
        是否验证通过
    """
    try:
        # 重新读取文件以确保内容已写入
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 检查AdditionInfo字段是否存在且为字典类型
        if "AdditionInfo" not in data:
            return False
        
        if not isinstance(data["AdditionInfo"], dict):
            return False
        
        return True
    except Exception:
        return False

def get_matching_json_files(json_dir: str, person_names: Optional[List[str]] = None) -> List[str]:
    """
    获取匹配的JSON文件列表
    
    参数:
        json_dir: JSON文件目录
        person_names: 可选，员工编号列表，None表示所有文件
    
    返回:
        匹配的JSON文件列表
    """
    # 获取所有JSON文件
    all_json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    
    # 如果没有指定人员列表，则返回所有文件
    if person_names is None or person_names == "all":
        return all_json_files
    
    # 如果是列表，则过滤出匹配的文件
    if isinstance(person_names, list):
        # 使用集合操作优化过滤
        person_names_set = set(person_names)
        filtered_files = []
        
        for json_file in all_json_files:
            # 从文件名中提取工号（通常是文件名的第一部分）
            file_prefix = json_file.split('_')[0]
            # 检查是否包含在person_names中
            if file_prefix in person_names_set:
                filtered_files.append(json_file)
        
        return filtered_files
    
    return all_json_files

def update_specific_employee_info(json_dir: str, emp_no: str, 
                                log_func: Optional[callable] = None) -> bool:
    """
    更新特定员工的JSON文件的AdditionInfo字段
    
    参数:
        json_dir: JSON文件所在目录
        emp_no: 员工编号
        log_func: 可选，日志记录函数
    
    返回:
        是否更新成功
    """
    # 日志记录函数
    def log(msg):
        if log_func:
            log_func(msg)
        else:
            print(msg)
    
    # 格式化员工编号
    emp_no_padded = emp_no.zfill(5)
    
    # 查找匹配的JSON文件
    json_files = get_matching_json_files(json_dir, [emp_no_padded])
    
    if not json_files:
        log(f"未找到员工 {emp_no} 的JSON文件")
        return False
    
    # 处理找到的文件
    updated = False
    for json_file in json_files:
        json_path = os.path.join(json_dir, json_file)
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查并更新AdditionInfo字段
            if "AdditionInfo" not in data:
                data["AdditionInfo"] = {}
                updated = True
            elif not isinstance(data["AdditionInfo"], dict):
                data["AdditionInfo"] = {}
                updated = True
            
            if updated:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                log(f"成功更新员工 {emp_no} 的JSON文件")
            
            # 验证更新结果
            if validate_addition_info(json_path):
                return True
                
        except Exception as e:
            log(f"处理员工 {emp_no} 的JSON文件时出错: {str(e)}")
    
    return updated

if __name__ == "__main__":
    # 示例用法
    import sys
    if len(sys.argv) >= 2:
        json_dir = sys.argv[1]
        stats = update_addition_info_for_files(json_dir)
        print(f"更新完成，总计处理 {stats['total_files']} 个文件")
    else:
        print("用法: python addition_info_update.py <json_directory> [employee_number]")
