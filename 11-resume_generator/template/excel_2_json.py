import pandas as pd
import json
from datetime import datetime

def clean_data(value):
    """
    清洗数据，去除空值和无效数据。

    :param value: 原始数据。
    :return: 清洗后的数据。
    """
    if pd.isna(value) or value == "nan" or value == "NaT":
        return None
    return value

def format_date(date_str):
    """
    格式化日期为YYYY/MM格式。

    :param date_str: 原始日期字符串。
    :return: 格式化后的日期字符串。
    """
    if not date_str or pd.isna(date_str):
        return None
    try:
        # 尝试解析日期
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(date_obj):
            return None
        return date_obj.strftime("%Y/%m")
    except:
        return None

def process_row(row, headers):
    """
    处理单行数据，将其转换为字典格式。

    :param row: 单行数据。
    :param headers: 标题行。
    :return: 处理后的字典。
    """
    data = {}
    for i, header in enumerate(headers):
        value = clean_data(row[i])
        if value is not None:
            data[header] = value
    return data
def extract_work_experience(data, max_rows=10):
    """
    提取工作经历。

    :param data: 单行数据字典。
    :param max_rows: 最大行数，默认为10。
    :return: 工作经历列表。
    """
    work_experience = []
    for i in range(1, max_rows + 1):
        start_time = data.get(f"工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-开始时间.{i}" if i > 1 else "工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-开始时间")
        end_time = data.get(f"工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-结束时间.{i}" if i > 1 else "工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-结束时间")
        company_name = data.get(f"工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-公司名称.{i}" if i > 1 else "工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-公司名称")
        position = data.get(f"工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-担任职务.{i}" if i > 1 else "工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-担任职务")
        job_description = data.get(f"工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-工作职责说明.{i}" if i > 1 else "工作经历（由近至远）如超过10段工作经历，剩余未填写的经历请移步到序号22继续填写-工作职责说明")

        if start_time or end_time or company_name or position or job_description:
            work_experience.append({
                "StartTime": format_date(start_time),
                "EndTime": format_date(end_time),
                "CompanyName": company_name,
                "Position": position,
                "JobDescription": job_description
            })
    return work_experience

def extract_project_experience(data, max_rows=10):
    """
    提取项目经历。

    :param data: 单行数据字典。
    :param max_rows: 最大行数，默认为10。
    :return: 项目经历列表。
    """
    project_experience = []
    for i in range(1, max_rows + 1):
        start_time = data.get(f"项目经历（由近至远）-开始时间.{i}" if i > 1 else "项目经历（由近至远）-开始时间")
        end_time = data.get(f"项目经历（由近至远）-结束时间.{i}" if i > 1 else "项目经历（由近至远）-结束时间")
        project_name = data.get(f"项目经历（由近至远）-项目名称.{i}" if i > 1 else "项目经历（由近至远）-项目名称")
        project_role = data.get(f"项目经历（由近至远）-项目角色.{i}" if i > 1 else "项目经历（由近至远）-项目角色")
        job_description = data.get(f"项目经历（由近至远）-项目职责说明.{i}" if i > 1 else "项目经历（由近至远）-项目职责说明")

        if start_time or end_time or project_name or project_role or job_description:
            project_experience.append({
                "StartTime": format_date(start_time),
                "EndTime": format_date(end_time),
                "ProjectName": project_name,
                "ProjectRole": project_role,
                "JobDescription": job_description
            })
    return project_experience

def convert_to_json(data):
    """
    将单行数据转换为JSON格式。

    :param data: 单行数据字典。
    :return: JSON格式的数据。
    """
    json_data = {
        "BasicInfo": {
            "Name": data.get("基本情况-姓名（必填）"),
            "WorkYears": data.get("基本情况-工作年限（必填）"),
            "HighestEducation": data.get("基本情况-最高学历（必填）"),
            "GraduationTime": format_date(data.get("基本情况-最高学历-毕业日期（必填）")),
            "GraduationSchool": data.get("基本情况-最高学历-毕业学校（必填）"),
            "Major": data.get("基本情况-最高学历-专业（必填）"),
            "Department": data.get("基本情况-部门（必填）"),
            "Title": data.get("基本情况-职称（必填）"),
            "PersonalProfile": data.get("个人简介（必填）")
        },
        "WorkExperience": extract_work_experience(data),
        "ProjectExperience": extract_project_experience(data),
        "WorkAbility": {
            "BusinessAbility": data.get("业务与技术能力详述（必填）"),
            "Certification": data.get("资质认证"),
            "Training": data.get("参与培训"),
            "SkillTag": data.get("技能标签（必填）")
        }
    }
    return json_data

def process_excel_to_json(file_path, sheet_name="数据来源"):
    """
    处理Excel文件，将其转换为JSON格式。

    :param file_path: Excel文件路径。
    :param sheet_name: 工作表名称，默认为"数据来源"。
    :return: JSON格式的数据。
    """
    try:
        # 读取Excel文件
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # 获取标题行和数据行
        headers = df.columns.tolist()
        rows = df.values.tolist()

        # 处理每一行数据
        json_data = {}
        for row in rows:
            row_data = process_row(row, headers)
            person_name = row_data.get("基本情况-姓名（必填）")
            if person_name:
                json_data[person_name] = convert_to_json(row_data)

        return json_data

    except Exception as e:
        print(f"处理Excel文件时出错: {e}")
        return None

# 示例调用
if __name__ == "__main__":
    # 示例文件路径
    file_path = r"D:\github\11-resume_generator\template\人员简历汇总_20241103.xlsm"

    # 处理Excel文件并生成JSON
    json_data = process_excel_to_json(file_path)

    # 打印JSON数据
    if json_data:
        print(json.dumps(json_data, ensure_ascii=False, indent=4))
