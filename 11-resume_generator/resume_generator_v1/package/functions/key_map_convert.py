#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
键值映射转换模块
用于在渲染简历模板时将长键值映射到短键值
不修改原始JSON文件，只在内存中进行转换
"""

# 顶级键映射字典
TOP_LEVEL_MAPPINGS = {
    'BasicInfo': 'bi',           # 基本信息
    'WorkExperience': 'we',      # 工作经历
    'ProjectExperience': 'pe',   # 项目经验
    'WorkAbility': 'wa',         # 工作能力
    'AdditionInfo': 'ai',        # 附加信息
    'SpecialInfo': 'si'          # 特殊信息
}

# 详细键值映射字典（确保所有值长度小于5个字符）
KEY_MAPPINGS = {
    # BasicInfo (bi) 部分映射
    'EmpNo': 'emp',
    'Name': 'name',
    'WorkYears': 'work',
    'GraduationTime': 'grad',
    'GraduationSchool': 'school',
    'Major': 'major',
    'HighestEducation': 'edu',
    'Department': 'dept',
    'Title': 'title',
    'PersonalProfile': 'profile',
    
    # WorkExperience (we) 部分映射
    'StartTime': 'start',
    'EndTime': 'end',
    'CompanyName': 'comp',
    'Position': 'pos',
    'JobDescription': 'desc',
    'Duration': 'dur',
    
    # ProjectExperience (pe) 部分映射
    'ProjectName': 'proj',
    'ProjectRole': 'role',
    
    # WorkAbility (wa) 部分映射
    'BusinessAbility': 'skill',
    'Certification': 'cert',
    'Training': 'train',
    'SkillTag': 'tags',
    
    # AdditionInfo (ai) 部分映射
    'DepartmentLevel1': 'dept1',
    'DepartmentLevel2': 'dept2',
    'Position': 'pos',
    'JobCategory': 'job',
    'ProfessionalLevel': 'level',
    'CompanyEmail': 'email',
    'EmploymentStatus': 'status',
    'EntryDate': 'entry',
    'FirstEntryDate': 'first',
    'CompanyYears': 'years',
    'WorkYears': 'work',
    'PaymentCompany': 'pay',
    'BaseLocation': 'loc',
    'Gender': 'gender',
    'BirthDate': 'birth',
    'Age': 'age',
    'PoliticalStatus': 'pol',
    'IDNumber': 'id',
    'NativePlace': 'plc',
    'PhoneNumber': 'phone',
    'GraduationTime': 'grad',
    'GraduationSchool': 'school',
    'HighestEducation': 'edu',
    'Major': 'major',
    'ContractLegalPerson': 'legal',
    
    # SpecialInfo (si) 部分映射
    'Degree': 'degree',
    'StartWorkDate': 'workdt',
    'EducationList': 'edu',
    'DegreeType': 'type',
}


def convert_keys(data):
    """
    递归地将数据中的长键转换为短键
    
    Args:
        data: 输入数据，可以是字典、列表或其他类型
        
    Returns:
        转换后的数据，保持原有结构不变
    """
    if isinstance(data, dict):
        # 对字典进行键值转换
        converted = {}
        for key, value in data.items():
            # 首先检查是否是顶级键
            new_key = TOP_LEVEL_MAPPINGS.get(key, KEY_MAPPINGS.get(key, key))
            # 递归处理值
            converted[new_key] = convert_keys(value)
        return converted
    elif isinstance(data, list):
        # 对列表中的每个元素递归处理
        return [convert_keys(item) for item in data]
    else:
        # 对于其他类型，直接返回
        return data


def convert_resume_data(resume_data):
    """
    转换整个简历数据的键值
    
    Args:
        resume_data: 简历数据字典
        
    Returns:
        转换后的简历数据字典
    """
    if not isinstance(resume_data, dict):
        raise TypeError("简历数据必须是字典类型")
    
    # 创建新的字典来存储转换后的数据
    converted_data = {}
    
    # 处理简历数据中的每一个人员（通常只有一个）
    for person_name, person_data in resume_data.items():
        # 转换个人数据的所有键值
        converted_person_data = convert_keys(person_data)
        # 保留原始的人员名称作为键
        converted_data[person_name] = converted_person_data
    
    return converted_data


def get_key_mapping_dict():
    """
    获取键值映射字典，用于生成映射文档
    
    Returns:
        包含顶级键和详细键映射的字典
    """
    # 合并顶级键和详细键映射
    all_mappings = {**TOP_LEVEL_MAPPINGS, **KEY_MAPPINGS}
    return all_mappings.copy()

def get_reverse_mapping_dict():
    """
    获取反向映射字典（短键到长键）
    
    Returns:
        反向映射字典
    """
    # 合并顶级键和详细键映射并创建反向映射
    all_mappings = {**TOP_LEVEL_MAPPINGS, **KEY_MAPPINGS}
    return {v: k for k, v in all_mappings.items()}

def get_top_level_mapping_dict():
    """
    获取顶级键映射字典
    
    Returns:
        顶级键映射字典
    """
    return TOP_LEVEL_MAPPINGS.copy()


if __name__ == '__main__':
    # 简单的测试代码
    import json
    
    # 示例数据
    sample_data = {
        "张三": {
            "BasicInfo": {
                "EmpNo": "12345",
                "Name": "张三",
                "WorkYears": "5年"
            },
            "WorkExperience": [
                {
                    "StartTime": "2020/01",
                    "EndTime": "至今",
                    "CompanyName": "示例公司"
                }
            ],
            "ProjectExperience": [
                {
                    "ProjectName": "示例项目",
                    "ProjectRole": "负责人"
                }
            ]
        }
    }
    
    # 转换数据
    converted = convert_resume_data(sample_data)
    
    # 打印转换结果
    print("转换后的简历数据：")
    print(json.dumps(converted, ensure_ascii=False, indent=2))
