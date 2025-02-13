import pandas as pd
import json
from datetime import datetime

def clean_data(value):
    """
    清洗数据，去除空值和无效数据。
    """
    if pd.isna(value) or value == "nan" or value == "NaT":
        return None
    return value

def format_date(date_str):
    """
    格式化日期为YYYY/MM格式。
    """
    if not date_str or pd.isna(date_str):
        return None
    try:
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(date_obj):
            return None
        return date_obj.strftime("%Y/%m")
    except:
        return None

def extract_experience(data, prefix, max_rows=10):
    """
    提取工作经历或项目经历，处理超过10行的数据。
    """
    experience = []
    for i in range(1, max_rows + 1):
        start_time = data.get(f"{prefix}-开始时间.{i}" if i > 1 else f"{prefix}-开始时间")
        end_time = data.get(f"{prefix}-结束时间.{i}" if i > 1 else f"{prefix}-结束时间")
        name = data.get(f"{prefix}-项目名称.{i}" if i > 1 else f"{prefix}-项目名称")
        role = data.get(f"{prefix}-项目角色.{i}" if i > 1 else f"{prefix}-项目角色")
        description = data.get(f"{prefix}-项目职责说明.{i}" if i > 1 else f"{prefix}-项目职责说明")

        if start_time or end_time or name or role or description:
            experience.append({
                "StartTime": format_date(start_time),
                "EndTime": format_date(end_time),
                "Name": name,
                "Role": role,
                "Description": description
            })
    
    return experience

def convert_to_json(data):
    """
    将单行数据转换为JSON格式。
    """
    json_data = {
        "BasicInfo": {
            "Name": data.get("基本情况-姓名"),
            "WorkYears": data.get("基本情况-工作年限"),
            "HighestEducation": data.get("基本情况-最高学历"),
            "GraduationTime": data.get("基本情况-最高学历-毕业日期"),
            "GraduationSchool": data.get("基本情况-最高学历-毕业学校"),
            "Major": data.get("基本情况-最高学历-专业"),
            "Department": data.get("基本情况-部门"),
            "Title": data.get("基本情况-职称"),
            "PersonalProfile": data.get("基本情况-个人简介")
        },
        "WorkExperience": extract_experience(data, "工作经历"),
        "ProjectExperience": extract_experience(data, "项目经历"),
        "WorkAbility": {
            "BusinessAbility": data.get("业务与技术能力详述"),
            "Certification": data.get("资质认证"),
            "Training": data.get("参与培训"),
            "SkillTag": data.get("技能标签")
        }
    }
    return json_data

def process_excel_to_json(file_path, sheet_name="数据来源"):
    """
    处理Excel文件，将其转换为JSON格式。
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        headers = df.columns.tolist()
        rows = df.values.tolist()
        #print(rows)

        json_data = {}
        row_index = 0  # 用于跟踪每个人的简历行数
        while row_index < len(rows):
            row_data = rows[row_index]
            person_data = process_row(row_data, headers)

            # 获取当前人的最大行数
            max_rows = get_max_rows(person_data)
            
            # 收集当前人的简历数据
            person_name = person_data.get("基本情况-姓名")
            if person_name:
                json_data[person_name] = convert_to_json(person_data)
            
            # 跳过当前人的所有行
            row_index += max_rows
        
        return json_data

    except Exception as e:
        print(f"处理Excel文件时出错: {e}")
        return None

def process_row(row, headers):
    """
    处理单行数据，将其转换为字典格式。
    """
    data = {}
    for i, header in enumerate(headers):
        value = clean_data(row[i])
        if value is not None:
            data[header] = value
    return data

def get_max_rows(data):
    """
    获取一个人数据的最大行数，根据工作经历和项目经历的列来判断。
    """
    work_experience = data.get("工作经历-开始时间")
    project_experience = data.get("项目经历-开始时间")
    
    max_rows = 10
    if work_experience or project_experience:
        max_rows = 10
        for i in range(11, 41, 10):  # 检查11-20行，21-30行，31-40行
            if data.get(f"工作经历-开始时间.{i}"):
                max_rows = max(max_rows, i)
            if data.get(f"项目经历-开始时间.{i}"):
                max_rows = max(max_rows, i)

    return max_rows

# 示例调用
if __name__ == "__main__":
    file_path = r"D:\github\11-resume_generator\template\人员简历汇总_20241103.xlsx"
    json_data = process_excel_to_json(file_path)
    if json_data:
        print(json.dumps(json_data, ensure_ascii=False, indent=4))
