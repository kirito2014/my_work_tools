import pandas as pd
import json
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta

def get_column_letter(col_idx):
    """将列索引转换为Excel列字母（例如，0 -> A, 25 -> Z, 26 -> AA）"""
    letters = []
    while col_idx >= 0:
        letters.append(chr(col_idx % 26 + ord('A')))
        col_idx = col_idx // 26 - 1
        if col_idx < 0:
            break
    return ''.join(reversed(letters))

def parse_date(date_str, is_end=False):
    """解析日期字符串，处理'至今'并返回日期对象"""
    if not date_str:
        return None
    if is_end and date_str.strip() == "至今":
        return datetime.now()
    try:
        if re.match(r"\d{4}年\d{2}月", date_str):
            return datetime.strptime(date_str, "%Y年%m月")
        elif re.match(r"\d{4}/\d{2}", date_str):
            return datetime.strptime(date_str, "%Y/%m")
        else:
            return None
    except:
        return None

def check_work_experience_dates(data, person_blocks, df_columns):
    """校验工作经历时间顺序及有效性"""
    errors = []
    for idx, exp in enumerate(data.get("WorkExperience", [])):
        start = exp.get("StartTime")
        end = exp.get("EndTime")
        start_date = parse_date(start)
        end_date = parse_date(end, is_end=True)
        
        # 查找对应的Excel列和行号
        block_num = idx
        prefix = "工作经历-开始时间"
        col_name = f"{prefix}.{block_num}" if block_num > 0 else prefix
        if col_name not in df_columns:
            continue
        col_idx = df_columns.get_loc(col_name)
        col_letter = get_column_letter(col_idx)
        row_num = person_blocks['work'].get(block_num, '未知行')
        
        if not start_date:
            errors.append(f"【一般】第 {col_letter} 列 {row_num} 行，工作经历开始时间格式错误：'{start}'")
        if end != "至今" and not end_date:
            errors.append(f"【一般】第 {col_letter} 列 {row_num} 行，工作经历结束时间格式错误：'{end}'")
        if start_date and end_date and start_date > end_date:
            errors.append(f"【重要】第 {col_letter} 列 {row_num} 行，工作经历时间顺序错误：{exp.get('CompanyName')}")

    return errors

def check_project_experience_dates(data, person_blocks, df_columns):
    """校验项目经历时间顺序及有效性"""
    errors = []
    for idx, project in enumerate(data.get("ProjectExperience", [])):
        start = project.get("StartTime")
        end = project.get("EndTime")
        start_date = parse_date(start)
        end_date = parse_date(end, is_end=True)
        
        # 查找对应的Excel列和行号
        block_num = idx
        prefix = "项目经历-开始时间"
        col_name = f"{prefix}.{block_num}" if block_num > 0 else prefix
        if col_name not in df_columns:
            continue
        col_idx = df_columns.get_loc(col_name)
        col_letter = get_column_letter(col_idx)
        row_num = person_blocks['project'].get(block_num, '未知行')
        
        if not start_date:
            errors.append(f"【一般】第 {col_letter} 列 {row_num} 行，项目开始时间格式错误：'{start}'")
        if end != "至今" and not end_date:
            errors.append(f"【一般】第 {col_letter} 列 {row_num} 行，项目结束时间格式错误：'{end}'")
        if start_date and end_date and start_date > end_date:
            errors.append(f"【重要】第 {col_letter} 列 {row_num} 行，项目时间顺序错误：{project.get('ProjectName')}")

    return errors

def check_education_overlap(data):
    """校验学历时间重叠"""
    educations = []
    levels = ["最高学历", "第一学历", "第二学历", "第三学历"]
    for level in levels:
        date_str = data["BasicInfo"].get("GraduationTime", "").split(f"【{level}】")
        if len(date_str) < 2:
            continue
        date_str = date_str[1].split("\n")[0].strip()
        date = parse_date(date_str)
        if date:
            educations.append((level, date))
    
    overlaps = []
    for i in range(len(educations)):
        for j in range(i+1, len(educations)):
            if educations[i][1] == educations[j][1]:
                overlaps.append(f"{educations[i][0]} 和 {educations[j][0]} 时间重叠")
    return [f"【重要】学历时间重叠：{ov}" for ov in overlaps] if overlaps else []

def check_work_years(data):
    """校验工作年限合理性"""
    work_years = data["BasicInfo"].get("WorkYears")
    if not work_years:
        return []
    
    try:
        reported_years = int(re.search(r"\d+", work_years).group())
    except:
        return []
    
    first_job_date = None
    for exp in data.get("WorkExperience", []):
        start = parse_date(exp.get("StartTime"))
        if start and (not first_job_date or start < first_job_date):
            first_job_date = start
    
    if not first_job_date:
        return []
    
    actual_years = relativedelta(datetime.now(), first_job_date).years
    if abs(actual_years - reported_years) > 1:
        return [f"【重要】工作年限不匹配：申报{reported_years}年，实际约{actual_years}年"]
    return []

def check_project_name_format(data, person_blocks, df_columns):
    """校验项目名称格式"""
    errors = []
    pattern = re.compile(r"^.+(公司|联社|银行)\s*.+项目$")
    for idx, project in enumerate(data.get("ProjectExperience", [])):
        name = project.get("ProjectName", "")
        if not name:
            continue
        
        block_num = idx
        prefix = "项目经历-项目名称"
        col_name = f"{prefix}.{block_num}" if block_num > 0 else prefix
        if col_name not in df_columns:
            continue
        col_idx = df_columns.get_loc(col_name)
        col_letter = get_column_letter(col_idx)
        row_num = person_blocks['project'].get(block_num, '未知行')
        
        if not pattern.match(name):
            errors.append(f"【一般】第 {col_letter} 列 {row_num} 行，项目名称格式不符：'{name}'")
    return errors

def main():
    # 读取JSON简历数据
    with open("result.json", "r", encoding="utf-8") as f:
        resumes = json.load(f)
    
    # 读取原始Excel文件
    df = pd.read_excel("人员简历汇总_20241103.xlsx", sheet_name="数据来源")
    df = df.where(pd.notnull(df), None)
    
    # 预处理人员块映射
    person_block_map = {}
    for name in resumes.keys():
        person_rows = df[df["基本情况-姓名"] == name]
        work_blocks = {}
        project_blocks = {}
        
        for row_idx, row in person_rows.iterrows():
            for col in row.index:
                if "工作经历-开始时间" in col:
                    match = re.match(r"工作经历-开始时间(?:\.(\d+))?", col)
                    if match:
                        block_num = int(match.group(1)) if match.group(1) else 0
                        work_blocks[block_num] = row_idx + 2  # Excel行号
                elif "项目经历-开始时间" in col:
                    match = re.match(r"项目经历-开始时间(?:\.(\d+))?", col)
                    if match:
                        block_num = int(match.group(1)) if match.group(1) else 0
                        project_blocks[block_num] = row_idx + 2
        
        person_block_map[name] = {
            "work": work_blocks,
            "project": project_blocks
        }
    
    # 执行校验
    results = []
    for name, data in resumes.items():
        errors = []
        blocks = person_block_map.get(name, {"work": {}, "project": {}})
        
        # 各校验项
        errors.extend(check_work_experience_dates(data, blocks, df.columns))
        errors.extend(check_project_experience_dates(data, blocks, df.columns))
        errors.extend(check_education_overlap(data))
        errors.extend(check_work_years(data))
        errors.extend(check_project_name_format(data, blocks, df.columns))
        
        if errors:
            results.append({
                "校验序号": len(results) + 1,
                "人员名称": name,
                "校验结果": "\n".join(errors)
            })
    
    # 输出结果
    if results:
        output_df = pd.DataFrame(results)
        with pd.ExcelWriter("check_result.xlsx") as writer:
            output_df.to_excel(writer, sheet_name="校验结果", index=False)
        print("校验结果已保存至 check_result.xlsx")
    else:
        print("未发现校验错误")

if __name__ == "__main__":
    main()