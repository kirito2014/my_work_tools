import pandas as pd
import json
from datetime import datetime

def clean_data(value):
    """清洗数据，去除空值和无效数据。"""
    if pd.isna(value) or value in ["nan", "NaT", "", None]:
        return None
    return str(value).strip() if isinstance(value, str) else value

def format_date(date_str):
    """格式化日期为YYYY/MM格式。"""
    if not date_str:
        return None
    try:
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(date_obj):
            return None
        return date_obj.strftime("%Y/%m")
    except:
        return None

def extract_experience(rows, prefix):
    """提取工作或项目经历，合并所有区块数据。"""
    experience = []
    block_num = 0
    while True:
        suffix = f".{block_num}" if block_num > 0 else ""
        if prefix == "工作经历":
            start_col = f"工作经历-开始时间{suffix}"
            end_col = f"工作经历-结束时间{suffix}"
            name_col = f"工作经历-公司名称{suffix}"
            role_col = f"工作经历-担任职务{suffix}"
            desc_col = f"工作经历-工作职责说明{suffix}"
        else:
            start_col = f"项目经历-开始时间{suffix}"
            end_col = f"项目经历-结束时间{suffix}"
            name_col = f"项目经历-项目名称{suffix}"
            role_col = f"项目经历-项目角色{suffix}"
            desc_col = f"项目经历-项目职责说明{suffix}"
        
        if start_col not in rows[0]:
            break
        
        for row in rows:
            start_time = row.get(start_col)
            end_time = row.get(end_col)
            name = row.get(name_col)
            role = row.get(role_col)
            desc = row.get(desc_col)
            
            if all(v is None for v in [start_time, end_time, name, role, desc]):
                continue
            
            # 根据经历类型生成不同的字段
            if prefix == "工作经历":
                entry = {
                    "StartTime": format_date(start_time),
                    "EndTime": format_date(end_time),
                    "CompanyName": clean_data(name),
                    "Position": clean_data(role),
                    "JobDescription": clean_data(desc)
                }
            else:
                entry = {
                    "StartTime": format_date(start_time),
                    "EndTime": format_date(end_time),
                    "ProjectName": clean_data(name),
                    "ProjectRole": clean_data(role),
                    "JobDescription": clean_data(desc)
                }
            experience.append(entry)
        block_num += 1
    return experience

def convert_to_json(person_data, rows):
    """转换为JSON结构。"""
    json_data = {
        "BasicInfo": {
            "Name": person_data.get("基本情况-姓名"),
            "WorkYears": person_data.get("基本情况-工作年限"),
            "HighestEducation": person_data.get("基本情况-最高学历"),
            "GraduationTime": format_date(person_data.get("基本情况-最高学历-毕业日期")),
            "GraduationSchool": person_data.get("基本情况-最高学历-毕业学校"),
            "Major": person_data.get("基本情况-最高学历-专业"),
            "Department": person_data.get("基本情况-部门"),
            "Title": person_data.get("基本情况-职称"),
            "PersonalProfile": person_data.get("基本情况-个人简介")
        },
        "WorkExperience": extract_experience(rows, "工作经历"),
        "ProjectExperience": extract_experience(rows, "项目经历"),
        "WorkAbility": {
            "BusinessAbility": person_data.get("业务与技术能力详述"),
            "Certification": person_data.get("资质认证"),
            "Training": person_data.get("参与培训"),
            "SkillTag": person_data.get("技能标签")
        }
    }
    return json_data

def process_excel_to_json(file_path, sheet_name="数据来源"):
    """处理Excel文件并转换为JSON。"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        df = df.where(pd.notnull(df), None)
        json_output = {}
        current_person = None
        person_data = {}
        person_rows = []
        
        for idx, row in df.iterrows():
            name = row.get("基本情况-姓名")
            if name and name != current_person:
                if current_person is not None:
                    # 合并之前人员的数据
                    merged_data = {**person_data, "rows": person_rows}
                    json_output[current_person] = convert_to_json(merged_data, person_rows)
                current_person = name
                person_data = row.to_dict()
                person_rows = [row.to_dict()]
            else:
                person_rows.append(row.to_dict())
        
        # 处理最后一个人员
        if current_person is not None:
            merged_data = {**person_data, "rows": person_rows}
            json_output[current_person] = convert_to_json(merged_data, person_rows)
        
        return json_output
    except Exception as e:
        print(f"处理出错: {e}")
        return None

# 示例调用
if __name__ == "__main__":
    file_path = r"D:\github\11-resume_generator\template\人员简历汇总_20241103.xlsx"
    result = process_excel_to_json(file_path)
    if result:
        print(json.dumps(result, ensure_ascii=False, indent=4))