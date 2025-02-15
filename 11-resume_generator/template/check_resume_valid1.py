# check_resume_valid.py
import pandas as pd
import os,sys,re
import json
from datetime import datetime
import argparse
from dateutil.relativedelta import relativedelta
import excel_2_json as ej

def parse_date(date_str, is_graduation=False):
    """解析日期字符串为datetime对象，处理特殊格式"""
    if date_str is None:
        return None
    # 保留原始字符串用于后续校验
    cleaned_str = str(date_str).strip() if date_str else ""
    
    # 处理结束时间为"至今"的情况
    if cleaned_str == "至今":
        return datetime.now()
    
    try:
        if is_graduation:
            return datetime.strptime(cleaned_str, "%Y年%m月")
        return datetime.strptime(cleaned_str, "%Y/%m")
    except:
        return None

def validate_work_experience(work_exp):
    """校验工作经历"""
    errors = []
    prev_date = None
    for idx, exp in enumerate(work_exp, 1):
        # 校验开始时间不能为"至今"
        if str(exp["StartTime"]).strip() == "至今":
            errors.append(f"【重要】工作经历第{idx}段开始时间不能为'至今'")
        
        # 日期格式校验（仅当不是"至今"时）
        start = parse_date(exp["StartTime"])
        end = parse_date(exp["EndTime"])
        
        # 格式有效性校验
        if not start and str(exp["StartTime"]).strip() not in ["", "至今"]:
            errors.append(f"【一般】工作经历第{idx}段开始时间格式错误：{exp['StartTime']}")
        
        # 时间顺序校验（当两个日期都有效时）
        if start and end and start > end:
            errors.append(f"【重要】工作经历第{idx}段开始时间晚于结束时间")
    
    return errors

def validate_project_experience(proj_exp, work_exp):
    """校验项目经历"""
    errors = []
    project_map = {}
    
    for idx, proj in enumerate(proj_exp, 1):
        # 校验开始时间不能为"至今"
        if str(proj["StartTime"]).strip() == "至今":
            errors.append(f"【重要】项目经历第{idx}段开始时间不能为'至今'")
        
        # 日期格式校验
        start = parse_date(proj["StartTime"])
        end = parse_date(proj["EndTime"])
        
        # 格式有效性校验
        if not start and str(proj["StartTime"]).strip() not in ["", "至今"]:
            errors.append(f"【一般】项目经历第{idx}段开始时间格式错误：{proj['StartTime']}")
        
        # 时间顺序校验
        if start and end and start > end:
            errors.append(f"【重要】项目经历第{idx}段开始时间晚于结束时间")
        
        # 跨公司项目校验
        if proj["ProjectName"] in project_map:
            for company, s, e in project_map[proj["ProjectName"]]:
                if (start < e) and (end > s):
                    errors.append(f"【重要】跨公司项目时间冲突：'{proj['ProjectName']}'在{company}的时间重叠")
        project_map.setdefault(proj["ProjectName"], []).append(
            (proj.get("CompanyName","未知公司"), start, end)
        )
    
    return errors

def validate_education(person_data):
    """学历信息校验"""
    errors = []
    edu_levels = ["最高学历", "第一学历", "第二学历", "第三学历"]
    edu_data = {}
    
    for level in edu_levels:
        grad_date = parse_date(person_data["BasicInfo"].get("GraduationTime"), True)
        school = person_data["BasicInfo"].get("GraduationSchool")
        # 学历信息完整性校验
        if grad_date and not school:
            errors.append(f"【重要】{level}填写了毕业时间但未填写学校")
    
    return errors
def validate_work_years(person_data, work_exp):
    """工作年限校验（改进版）"""
    def extract_highest_education_date(grad_time_str):
        """提取最高学历日期"""
        if not grad_time_str:
            return None
        for entry in grad_time_str.split("\n"):
            if entry.startswith("【最高学历】"):
                date_str = entry.replace("【最高学历】", "").strip()
                return parse_date(date_str, is_graduation=True)
        return None

    try:
        # 工作年限转换
        work_years = float(re.search(r"\d+\.?\d*", person_data["BasicInfo"]["WorkYears"]).group())
        #print(work_years)
    except (KeyError, AttributeError, ValueError):
        return ["【重要】工作年限格式错误"]

    #工作年限 = 当前日期 - 最早开始工作日期 
    #获取当前日期yyyy/mm/dd
    current_date = datetime.now()
    # 获取最高学历日期
    grad_date = extract_highest_education_date(person_data["BasicInfo"].get("GraduationTime", ""))
    #print(grad_date)
    if not grad_date:
        return ["【重要】最高学历日期解析失败"]

    # 获取有效工作开始日期
    valid_work_dates = [parse_date(exp["StartTime"]) for exp in work_exp]
    valid_work_dates = [d for d in valid_work_dates if d is not None]
    #print(valid_work_dates)
    if not valid_work_dates:
        return ["【重要】缺少有效工作开始日期"]
    
    earliest_work = min(valid_work_dates)
    #print(earliest_work)

    # 逻辑校验
    errors = []
    if grad_date > earliest_work:
        errors.append("【重要】最早工作日期早于毕业时间")
    
    # 计算年限差 当前-最早工作日期
    delta = relativedelta(current_date, earliest_work).years + \
           relativedelta(current_date, earliest_work).months / 12
    if abs(work_years - delta) > 1:
        errors.append(f"【重要】工作年限({work_years:.0f}年)与最早工作日期推算({delta:.0f}年)不符")
    
    return errors
# def validate_work_years(person_data, work_exp):
#     """工作年限校验"""
#     try:
#         # 将工作年限中的"年"去掉，并转换为浮点数
#         work_years = float(person_data["BasicInfo"]["WorkYears"].replace("年",""))
#     except:
#         # 如果转换失败，返回错误信息
#         return ["【重要】工作年限格式错误"]
    
#     #获取当前日期yyyy/mm/dd

#     current_date = datetime.now()
#     #print(ej.format_date(current_date))
#     # 获取最早的工作日期
#     earliest_work = min([parse_date(exp["StartTime"]) for exp in work_exp if parse_date(exp["StartTime"])])
#     #print(ej.format_date(earliest_work))
#     # 获取毕业时间 将yyyy年月转换成datetime对象
#     grad_date = parse_date(person_data["BasicInfo"]["GraduationTime"], True)
#     #print(grad_date)

#     # 计算工作年限
#     work_years_calculated = (current_date - earliest_work).days / 365
#     print(work_years_calculated)
#     print(work_years)
    
#     # 如果毕业时间和最早工作日期都存在
#     if work_years and earliest_work:
#         print(1111)
#         # 如果毕业时间晚于最早工作日期，返回错误信息
#         if grad_date > earliest_work:
#             return ["【重要】最早工作日期早于毕业时间"]
        
#         # 计算毕业时间和最早工作日期之间的年数差
#         delta = relativedelta(earliest_work, grad_date).years
#         # 如果工作年限和毕业时间推算的年数差超过1年，返回错误信息
#         if abs(work_years - work_years_calculated) > 1:
#             return [f"【重要】工作年限({work_years}年)与毕业时间推算({work_years_calculated}年)不符"]
    
#     # 如果没有错误，返回空列表
#     return []

def generate_check_results(data_source):
    """生成校验结果"""
    results = []
    
    for person_name, person_data in data_source.items():
        errors = []
        
        # 基本信息校验
        errors += validate_education(person_data)
        #errors += validate_work_years(person_data, person_data["WorkExperience"])
        errors += validate_work_years(person_data, person_data["WorkExperience"])
        
        # 工作经历校验
        errors += validate_work_experience(person_data["WorkExperience"])
        
        # 项目经历校验
        errors += validate_project_experience(
            person_data["ProjectExperience"],
            person_data["WorkExperience"]
        )
        
        # 去重并生成结果
        if errors:
            results.append({
                "人员名称": person_name,
                "校验结果": "\n".join(list(set(errors)))
            })
    
    return pd.DataFrame(results)

def main():
    # 创建一个ArgumentParser对象
    parser = argparse.ArgumentParser()
    # 添加一个参数，用于指定输入的JSON文件路径
    parser.add_argument("--json", help="输入的JSON文件路径")
    # 添加一个参数，用于指定输入的Excel文件路径
    parser.add_argument("--excel", help="输入的Excel文件路径")
    # 解析参数
    args = parser.parse_args()
    
    # 如果指定了--json参数
    if args.json:
        # 打开JSON文件，并读取数据
        with open(args.json, 'r', encoding='utf-8') as f:
            data = json.load(f)
    # 如果指定了--excel参数
    elif args.excel:
        # 调用ej.process_excel_to_json函数，将Excel文件转换为JSON数据
        data = ej.process_excel_to_json(args.excel)
    # 如果没有指定--json或--excel参数
    else:
        # 打印提示信息
        print("必须指定--json或--excel参数")
        # 返回
        return
    
    # 调用generate_check_results函数，生成校验结果
    df = generate_check_results(data)
    
    #文件存在检测，没有写入，有则删除后写入
    if os.path.exists("check_result.xlsx"):
        os.remove("check_result.xlsx")

    # 将校验结果保存到Excel文件中
    with pd.ExcelWriter("check_result.xlsx") as writer:
        # 将DataFrame写入Excel文件，指定sheet名称和索引列名称
        df.to_excel(writer, sheet_name="校验结果", index_label="校验序号")
    
    # 打印提示信息
    print(f"校验结果已保存至 check_result.xlsx")

if __name__ == "__main__":
    main()
