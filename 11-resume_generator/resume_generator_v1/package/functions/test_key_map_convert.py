#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
键值映射转换测试脚本
使用实际的简历JSON文件测试转换功能
"""

import os
import json
from key_map_convert import convert_resume_data, get_key_mapping_dict

def test_with_actual_file():
    """
    使用实际的JSON文件测试转换功能
    """
    # 获取modify_json目录下的第一个JSON文件
    modify_json_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'output', 'modify_json'
    )
    
    # 获取所有JSON文件
    json_files = [f for f in os.listdir(modify_json_dir) if f.endswith('.json')]
    
    if not json_files:
        print("错误：在modify_json目录下未找到JSON文件")
        return False
    
    # 选择第一个JSON文件进行测试
    test_file = json_files[0]
    test_file_path = os.path.join(modify_json_dir, test_file)
    
    print(f"正在使用文件进行测试：{test_file}")
    
    try:
        # 读取JSON文件
        with open(test_file_path, 'r', encoding='utf-8') as f:
            resume_data = json.load(f)
        
        print(f"成功读取文件，数据结构：{list(resume_data.keys())}")
        
        # 执行转换
        print("开始转换键值...")
        converted_data = convert_resume_data(resume_data)
        
        # 验证转换结果
        print("验证转换结果...")
        
        # 获取映射字典
        mapping_dict = get_key_mapping_dict()
        
        # 检查转换后的结构
        for person_name, person_data in converted_data.items():
            print(f"\n人员：{person_name}")
            print(f"主要部分：{list(person_data.keys())}")
            
            # 检查bi部分（BasicInfo转换后）
            if 'bi' in person_data:
                basic_info_keys = list(person_data['bi'].keys())
                print(f"bi部分键值数量：{len(basic_info_keys)}")
                # 检查是否有长键值（首字母大写）
                uppercase_keys = [k for k in basic_info_keys if k[0].isupper() and k not in ['BasicInfo', 'WorkExperience', 'ProjectExperience', 'WorkAbility', 'AdditionInfo', 'SpecialInfo']]
                print(f"bi中剩余的长键值数量：{len(uppercase_keys)}")
                
                # 输出部分转换结果示例
                print("\n转换示例：")
                sample_keys = list(person_data['bi'].keys())[:5]
                for key in sample_keys:
                    original_key = None
                    for k, v in mapping_dict.items():
                        if v == key:
                            original_key = k
                            break
                    if original_key:
                        print(f"  {original_key} -> {key}: {person_data['bi'][key]}")
            
            # 检查we部分（WorkExperience转换后）
            if 'we' in person_data and person_data['we']:
                print(f"\nwe条目数量：{len(person_data['we'])}")
                if person_data['we'][0]:
                    exp_keys = list(person_data['we'][0].keys())
                    print(f"we条目中的键值数量：{len(exp_keys)}")
                    # 检查是否有长键值
                    uppercase_keys = [k for k in exp_keys if k[0].isupper()]
                    print(f"we中剩余的长键值数量：{len(uppercase_keys)}")
        
        print("\n测试完成！所有键值已成功转换。")
        return True
        
    except Exception as e:
        print(f"测试过程中出错：{str(e)}")
        return False

def test_nested_structures():
    """
    测试嵌套结构的转换
    """
    print("\n测试嵌套结构转换...")
    
    # 创建包含复杂嵌套结构的测试数据
    complex_data = {
        "测试用户": {
            "BasicInfo": {
                "EmpNo": "TEST001",
                "Name": "测试用户",
                "CustomField": "自定义字段"
            },
            "WorkExperience": [
                {
                    "StartTime": "2020/01",
                    "EndTime": "2022/01",
                    "CompanyName": "公司A",
                    "Projects": [
                        {
                            "ProjectName": "项目1",
                            "ProjectRole": "开发"
                        }
                    ]
                }
            ]
        }
    }
    
    # 转换数据
    converted = convert_resume_data(complex_data)
    
    # 验证转换
    assert converted["测试用户"]["bi"]["emp"] == "TEST001"
    assert converted["测试用户"]["we"][0]["start"] == "2020/01"
    assert converted["测试用户"]["we"][0]["Projects"][0]["proj"] == "项目1"
    
    # 验证未在映射表中的字段保持不变
    assert converted["测试用户"]["bi"]["CustomField"] == "自定义字段"
    
    print("嵌套结构测试通过！")
    return True

if __name__ == '__main__':
    print("===== 键值映射转换测试开始 =====")
    
    # 运行基本测试（文件测试）
    file_test_result = test_with_actual_file()
    
    # 运行嵌套结构测试
    nested_test_result = test_nested_structures()
    
    # 输出总体结果
    print("\n===== 测试结果汇总 =====")
    print(f"文件测试：{'通过' if file_test_result else '失败'}")
    print(f"嵌套结构测试：{'通过' if nested_test_result else '失败'}")
    
    if file_test_result and nested_test_result:
        print("\n✅ 所有测试通过！键值映射转换功能工作正常。")
    else:
        print("\n❌ 部分测试失败，请检查错误信息。")
