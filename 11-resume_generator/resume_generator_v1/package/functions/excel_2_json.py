import pandas as pd
import json,re
from datetime import datetime
#import render_from_excel as tj
from dateutil.relativedelta import relativedelta

def clean_data(value):
    """清洗数据，去除空值和无效数据。"""
    if pd.isna(value) or value in ["nan", "NaT", "", None]:
        return None
    return str(value).strip() if isinstance(value, str) else value
def remove_english_characters(input_string):
    # 使用正则表达式去除英文字符
    result_string = re.sub(r'[a-zA-Z]', '', input_string)
    
    # 去除末尾的数字和点号
    result_string = re.sub(r'[\d.]$', '', result_string)

    result_string = result_string.replace('.','')
    
    return result_string
def format_date(date_str):
    """格式化日期为YYYY/MM格式，处理'至今'情况。"""
    if not date_str:
        return None
    # 去除首尾空格并处理"至今"的情况
    cleaned_str = str(date_str).strip()
    if cleaned_str == "至今":
        return cleaned_str
    
    try:
        date_obj = pd.to_datetime(cleaned_str, errors="coerce")
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
    def sort_key(entry):
        if entry["StartTime"] == "至今":
            # 将"至今"视为当前日期+100年（确保排序在最前）
            return datetime.now() + relativedelta(years=100)
        try:
            return datetime.strptime(entry["StartTime"], "%Y/%m")
        except:
            return datetime.min
        
    experience.sort(key=sort_key, reverse=True)

    # 按StartTime降序排序（时间最近的在前）
    #experience.sort(key=lambda x: datetime.strptime(x["StartTime"], "%Y/%m") if x["StartTime"] else datetime.min, reverse=True)
    
    return experience

def convert_to_json(person_data, rows):
    """转换为JSON结构"""
    def format_education(prefix):
        """提取单个学历信息"""
        date = person_data.get(f"{prefix}-毕业日期")  # 基本信息的日期保留 yyyy年xx月 的格式
        school = clean_data(person_data.get(f"{prefix}-毕业学校"))
        major = clean_data(person_data.get(f"{prefix}-专业"))
        return (date, school, major)

    # 提取各学历层级数据
    edu_data = {
        "最高学历": format_education("基本情况-最高学历"),
        "第一学历": format_education("基本情况-第一学历"),
        "第二学历": format_education("基本情况-第2学历"),
        "第三学历": format_education("基本情况-第3学历")
    }

    # 处理每个字段的格式化逻辑
    def process_field(field_type):
        """通用字段处理逻辑"""
        highest_value = edu_data["最高学历"][["date", "school", "major"].index(field_type)]
        results = []
        
        # 处理最高学历
        if highest_value:
            results.append(f"【最高学历】{highest_value}")
        else:
            results.append(f"【最高学历】未填写")
        
        # 处理其他学历
        for level in ["第一学历", "第二学历", "第三学历"]:
            value = edu_data[level][["date", "school", "major"].index(field_type)]
            date = edu_data[level][0]  # 当前学历的毕业时间
            
            # 如果当前学历的毕业时间与最高学历不同，则视为多学历
            if date and date != edu_data["最高学历"][0]:
                if value:  # 如果当前字段有值，则添加到结果中
                    results.append(f"【{level}】{value}")
                else:
                    results.append(f"【{level}】未填写")
        
        return "\n".join(results) if results else None

    # 构建最终字段
    graduation_time = process_field("date")
    graduation_school = process_field("school")
    major = process_field("major")

    return {
        "BasicInfo": {
            "Name": person_data.get("基本情况-姓名"),
            "WorkYears": person_data.get("基本情况-工作年限").replace("年",""),
            "HighestEducation": clean_data(person_data.get("基本情况-最高学历")),
            "GraduationTime": graduation_time,
            "GraduationSchool": graduation_school,
            "Major": major,
            "Department": remove_english_characters(person_data.get("基本情况-部门")),
            "Title": person_data.get("基本情况-职称"),
            "PersonalProfile": person_data.get("基本情况-个人简介")
        },
        "WorkExperience": extract_experience(rows, "工作经历"),
        "ProjectExperience": extract_experience(rows, "项目经历"),
        "WorkAbility": {
            "BusinessAbility": person_data.get("业务与技术能力详述",""),
            "Certification": person_data.get("资质认证",""),
            "Training":  person_data.get("参与培训",""),
            "SkillTag":  person_data.get("技能标签","") 
        }
    }

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
    file_path = "人员简历汇总_20241103.xlsx"
    # 模板文件路径
    template_path = "人员简历_模板.docx"
    result_json = "result.json"

    # 输出文件夹路径
    output_folder = "output_resumes"
    result = process_excel_to_json(file_path)

    if result:
        # 将结果写入 result.json
        with open('result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        print("结果已保存至 result.json")
    else:
        print("未生成有效数据")