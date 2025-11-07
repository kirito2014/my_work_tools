# check_resume_valid.py
import pandas as pd
import os,sys,re
import json
from datetime import datetime,timedelta
import argparse
from dateutil.relativedelta import relativedelta


#设置根目录为项目根目录

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(base_dir)
import excel_2_json as ej

#print(base_dir)
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
        # 解析为当月第一天
        #date_obj = datetime.strptime(date_str, "%Y/%m")
        # 计算当月最后一天
        #next_month = date_obj.replace(day=28) + timedelta(days=4)
        #return next_month - timedelta(days=next_month.day)

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

    # 工作经历日期重叠校验
    errors += validate_work_period_overlap(work_exp)
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
        
        # 精确化时间处理
        start = parse_date(proj["StartTime"])
        end = parse_date(proj["EndTime"]) or datetime.now()
        current_company = proj.get("CompanyName", "未知公司").strip()
        
        # 标准化项目名称（去除空格和特殊字符）
        project_name = re.sub(r'\W+', '', proj["ProjectName"].lower().strip())
        
        if project_name in project_map:
            for record in project_map[project_name]:
                company, s, e = record
                e = e or datetime.now()  # 处理历史记录的"至今"
                
                # 精确时间重叠判断（包含边界）
                overlap_condition = (
                    (start <= e) and  # 当前开始 <= 历史结束
                    (end >= s)       # 当前结束 >= 历史开始
                ) and current_company != company.strip()
                
                if overlap_condition:
                    # 格式化时间显示（精确到月）
                    fmt = lambda d: d.strftime("%Y/%m") if isinstance(d, datetime) else str(d)
                    error_details = (
                        f"【重要】跨公司项目时间冲突检测：\n"
                        f"项目名称：{proj['ProjectName']}\n"
                        f"当前记录：{current_company} ({fmt(start)} ~ {fmt(end)})\n"
                        f"冲突记录：{company} ({fmt(s)} ~ {fmt(e)})\n"
                        f"重叠时段：{max(start, s).strftime('%Y/%m')} ~ {min(end, e).strftime('%Y/%m')}"
                    )
                    errors.append(error_details)

        # 存储标准化后的项目信息
        project_map.setdefault(project_name, []).append((
            current_company,
            start,
            end
        ))
        # 新增跨工作经历校验
    errors += validate_cross_work_projects(work_exp, proj_exp)
    errors += validate_proj_period_overlap(proj_exp)

    return errors
def validate_cross_work_projects(work_exp, proj_exp):
    """改进的跨工作时间校验（精确边界判断及跨公司检测）"""
    errors = []
    
    # 构建工作时间轴并排序
    work_periods = []
    for work in work_exp:
        start = parse_date(work["StartTime"])
        end = parse_date(work["EndTime"]) or datetime.now()
        if not start or not end:
            continue
        work_periods.append({
            "company": work["CompanyName"].strip(),
            "start": (start.year, start.month),
            "end": (end.year, end.month) if end != datetime.now() else (9999, 12)
        })
    
    # 按开始时间升序排列工作经历
    work_periods.sort(key=lambda x: x['start'])
    
    # 校验每个项目
    for proj in proj_exp:
        proj_start = parse_date(proj["StartTime"])
        proj_end = parse_date(proj["EndTime"]) or datetime.now()
        company = proj.get("CompanyName", "").strip()
        if not proj_start or not proj_end:
            continue
        
        proj_start_tuple = (proj_start.year, proj_start.month)
        proj_end_tuple = (proj_end.year, proj_end.month) if proj_end != datetime.now() else (9999, 12)
        
        # 查找项目开始时间所属的工作经历（使用左闭右开区间）
        matched_work = None
        for work in work_periods:
            if work['start'] <= proj_start_tuple < work['end']:  # 修改为左闭右开区间
                matched_work = work
                break
        
        if not matched_work:
            errors.append(f"【警告】游离项目：{proj['ProjectName']} 未匹配任何工作经历")
            continue
        
        # 检查时间越界或跨公司
        error_parts = []
        if proj_start_tuple < matched_work['start']:
            error_parts.append(f"开始时间早于工作经历 {matched_work['company']} 的开始")
        
        # 结束时间越界判断
        if proj_end_tuple > matched_work['end']:
            # 查找是否有其他公司包含结束时间
            cross_work = None
            for work in work_periods:
                if work is matched_work:
                    continue
                if work['start'] <= proj_end_tuple < work['end']:  # 同样使用左闭右开区间
                    cross_work = work
                    break
            if cross_work:
                error_msg = (
                    f"【重要】项目跨公司：{proj['ProjectName']}\n"
                    f"项目时间：{format_date_tuple(proj_start_tuple)}~{format_date_tuple(proj_end_tuple)}\n"
                    f"开始于：{matched_work['company']} ({format_date_tuple(matched_work['start'])}~{format_date_tuple(matched_work['end'])})\n"
                    f"结束于：{cross_work['company']} ({format_date_tuple(cross_work['start'])}~{format_date_tuple(cross_work['end'])})"
                )
                errors.append(error_msg)
            else:
                error_msg = (
                    f"【重要】项目时间越界：{proj['ProjectName']}\n"
                    f"项目时间：{format_date_tuple(proj_start_tuple)}~{format_date_tuple(proj_end_tuple)}\n"
                    f"所属工作：{matched_work['company']} ({format_date_tuple(matched_work['start'])}~{format_date_tuple(matched_work['end'])})\n"
                    f"违规类型：结束时间晚于工作经历 {matched_work['company']} 的结束且无后续工作经历"
                )
                errors.append(error_msg)
        elif proj_end_tuple < matched_work['start']:
            error_parts.append(f"结束时间早于工作经历 {matched_work['company']} 的开始")
        
        if error_parts:
            error_msg = (
                f"【重要】项目时间异常：{proj['ProjectName']}\n"
                f"项目时间：{format_date_tuple(proj_start_tuple)}~{format_date_tuple(proj_end_tuple)}\n"
                f"所属工作：{matched_work['company']} ({format_date_tuple(matched_work['start'])}~{format_date_tuple(matched_work['end'])})\n"
                f"异常原因：{'，'.join(error_parts)}"
            )
            errors.append(error_msg)
    
    return errors

def format_date_tuple(date_tuple):
    """格式化月份元组"""
    if date_tuple[0] == 9999:
        return "至今"
    return f"{date_tuple[0]}/{date_tuple[1]:02d}"

# '//////////////////////////项目日期时间校验是否有跨公司日期/////////////////////////////////////////////////////////////'
# '示例：如A公司任职时间为 2020/10-2021/06 项目时间应该在此时间段中出现如2020/10-2021/09 这样的数据即为跨公司
# '逻辑实现在每段公司任职时间判断循环中，判断每个项目的开始和结尾是否有交叉

# ' 公司任职时间       |___________|________________________________________|_____________|
# '                2020/10 A  2021/09                   B              2022/06    C    2022/09
# '项目实施时间        |___________|___________________________________________|__________|
# '                2020/10 A1 2021/09                  B1                    2022/07  C1 2022/09

def validate_work_period_overlap(work_exp):
    """改进的工作经历时间段重叠校验（排除衔接情况）"""
    errors = []
    periods = []
    
    # 转换时间段为日期对象
    for idx, exp in enumerate(work_exp, 1):
        start = parse_date(exp["StartTime"])
        end = parse_date(exp["EndTime"]) or datetime.now()
        
        if not start or not end:
            continue
        
        periods.append((
            idx,
            start,
            end,
            exp["CompanyName"]
        ))
    
    # 双重循环检测实际重叠
    for i in range(len(periods)):
        for j in range(i+1, len(periods)):
            idx_a, s_a, e_a, name_a = periods[i]
            idx_b, s_b, e_b, name_b = periods[j]
            
            # 排除边界衔接的情况（前一段的结束=后一段的开始）
            if e_a == s_b or e_b == s_a:
                continue
                
            # 判断核心重叠逻辑（直接使用日期对象）
            if has_overlap(s_a, e_a, s_b, e_b):
                # 转换回可读格式
                fmt = lambda d: d.strftime("%Y/%m") if d != datetime.now() else "至今"
                overlap_start = max(s_a, s_b)
                overlap_end = min(e_a, e_b)
                
                error_msg = (
                    f"【重要】工作经历时间段冲突：\n"
                    f"➤ 第{idx_a}段 {name_a} ({fmt(s_a)}~{fmt(e_a)})\n"
                    f"➤ 第{idx_b}段 {name_b} ({fmt(s_b)}~{fmt(e_b)})\n"
                    f"重叠时段：{fmt(overlap_start)}～{fmt(overlap_end)}"
                )
                errors.append(error_msg)
    
    return errors

def validate_proj_period_overlap(prj_exp):
    """项目经历时间段重叠校验（排除衔接情况）"""
    errors = []
    periods = []
    
    # 转换时间段为日期对象
    for idx, exp in enumerate(prj_exp, 1):
        start = parse_date(exp["StartTime"])
        end = parse_date(exp["EndTime"]) or datetime.now()
        
        if not start or not end:
            continue
        
        periods.append((
            idx,
            start,
            end,
            exp["ProjectName"]
        ))
    
    # 双重循环检测实际重叠
    for i in range(len(periods)):
        for j in range(i+1, len(periods)):
            idx_a, s_a, e_a, name_a = periods[i]
            idx_b, s_b, e_b, name_b = periods[j]
            
            # 排除边界衔接的情况（前一段的结束=后一段的开始）
            if e_a == s_b or e_b == s_a:
                continue
                
            # 判断核心重叠逻辑（直接使用日期对象）
            if has_overlap(s_a, e_a, s_b, e_b):
                # 转换回可读格式
                fmt = lambda d: d.strftime("%Y/%m") if d != datetime.now() else "至今"
                overlap_start = max(s_a, s_b)
                overlap_end = min(e_a, e_b)
                
                error_msg = (
                    f"【重要】项目经历时间段冲突：\n"
                    f"➤ 第{idx_a}段 {name_a} ({fmt(s_a)}~{fmt(e_a)})\n"
                    f"➤ 第{idx_b}段 {name_b} ({fmt(s_b)}~{fmt(e_b)})\n"
                    f"重叠时段：{fmt(overlap_start)}～{fmt(overlap_end)}"
                )
                errors.append(error_msg)
    
    return errors

def has_overlap(start1, end1, start2, end2):
    """判断两个时间段是否重叠（精确到天）"""
    return (start1 < end2) and (start2 < end1)

def validate_education(person_data):
    """学历信息校验"""
    errors = []
    edu_levels = ["最高学历", "第一学历", "第二学历", "第三学历"]
    
    # 提取毕业时间、学校和专业字段
    grad_time = person_data["BasicInfo"].get("GraduationTime", "")
    grad_school = person_data["BasicInfo"].get("GraduationSchool", "")
    major = person_data["BasicInfo"].get("Major", "")

    # 检查每个学历层级的完整性
    for level in edu_levels:
        # 提取当前学历的毕业时间、学校和专业
        level_grad_time = extract_field_by_level(grad_time, level)
        level_grad_school = extract_field_by_level(grad_school, level)
        level_major = extract_field_by_level(major, level)

        # 如果当前学历存在（即毕业时间、学校或专业有值），则检查是否未填写
        if level_grad_time is not None or level_grad_school is not None or level_major is not None:
            if level_grad_time == "未填写":
                errors.append(f"【重要】{level}未填写毕业时间")
            if level_grad_school == "未填写":
                errors.append(f"【重要】{level}未填写学校")
            if level_major == "未填写":
                errors.append(f"【重要】{level}未填写专业")

    return errors

def extract_field_by_level(field, level):
    """从字段中提取指定学历层级的值"""
    if not field:
        return None
    # 按行分割字段
    lines = field.split("\n")
    for line in lines:
        if f"【{level}】" in line:
            return line.replace(f"【{level}】", "").strip()
    return None
    
    return errors
def validate_work_years(person_data, work_exp, proj_exp):
    """工作年限校验（改进版）"""
    def extract_earliest_graduation_date(grad_time_str):
        """提取最早毕业日期"""
        if not grad_time_str:
            return None, "【重要】毕业日期为空"
        
        dates = []
        for entry in grad_time_str.split("\n"):
            #print(entry)
            if entry.startswith("【"):
                date_str = entry.split("】")[1].strip()
                #print(date_str)
                # 检查日期格式是否为 yyyy年mm月
                if not re.match(r"\d{4}年\d{1,2}月", date_str):
                    return None, f"【重要】毕业日期格式错误：{date_str}"
                date = parse_date(date_str, is_graduation=True)
                #print(date)
                if date:
                    dates.append(date)
        
        if not dates:
            return None, "【重要】未找到有效毕业日期"
        return min(dates), None

    try:
        # 工作年限转换
        work_years_str = person_data["BasicInfo"]["WorkYears"]
        # 检查工作年限字段是否包含 "年"
        if "年" in work_years_str:
            return ["【重要】工作年限字段中存在 '年' 字样，请使用纯数字"]
        work_years = float(re.search(r"\d+\.?\d*", work_years_str).group())
    except (KeyError, AttributeError, ValueError):
        return ["【重要】工作年限格式错误"]

    # 获取当前日期
    current_date = datetime.now()

    # 获取最早毕业日期
    grad_date, grad_error = extract_earliest_graduation_date(person_data["BasicInfo"].get("GraduationTime", ""))
    #print(grad_date)
    if grad_error:
        return [grad_error]

    # 获取有效工作开始日期
    valid_work_dates = [parse_date(exp["StartTime"]) for exp in work_exp]
    valid_work_dates = [d for d in valid_work_dates if d is not None]
    if not valid_work_dates:
        return ["【重要】缺少有效工作开始日期"]
    earliest_work = min(valid_work_dates)

    # 获取有效项目开始日期
    valid_proj_dates = [parse_date(exp["StartTime"]) for exp in proj_exp]
    valid_proj_dates = [d for d in valid_proj_dates if d is not None]
    if not valid_proj_dates:
        return ["【重要】缺少有效项目开始日期"]
    earliest_proj = min(valid_proj_dates)

    # 逻辑校验
    errors = []

    # 工作日期早于毕业时间
    if grad_date > earliest_work:
        errors.append("【重要】最早工作日期早于毕业时间")

    # 项目开始日期早于最早工作开始日期
    if earliest_proj > earliest_work:
        errors.append("【重要】最早项目开始日期早于最早工作开始日期")

    # 年限不符
    # 计算年限差 当前-最早工作日期
    delta = relativedelta(current_date, earliest_work).years + \
           relativedelta(current_date, earliest_work).months / 12
    if abs(work_years - delta) > 1:
        errors.append(f"【重要】工作年限({work_years:.0f}年)与最早工作日期推算({delta:.0f}年)不符")

    return errors


def generate_check_results(data_source):
    """生成校验结果"""
    results = []
    
    for person_name, person_data in data_source.items():
        errors = []
        
        # 基本信息校验
        errors += validate_education(person_data)
        #errors += validate_work_years(person_data, person_data["WorkExperience"])
        errors += validate_work_years(person_data, person_data["WorkExperience"], person_data["ProjectExperience"])
        
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
    # 添加一个参数，用于指定输出的Excel文件路径
    parser.add_argument("--output", help="输出的Excel文件路径")
    # 添加一个参数，用于指定是否追加模式
    parser.add_argument("--append", action="store_true", help="是否以追加模式写入Excel文件")
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
    
    # 确定输出文件路径
    if args.output:
        output_file = args.output
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
    else:
        # 默认输出路径为项目根目录的output\checkExcel
        output_dir = os.path.join(base_dir, "..", "output", "checkExcel")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_file = os.path.join(output_dir, "check_result.xlsx")
    
    # 检查是否需要追加模式
    if args.append and os.path.exists(output_file):
        # 读取现有文件
        existing_df = pd.read_excel(output_file)
        # 合并数据
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        # 重新索引
        combined_df.index = combined_df.index + 1
        # 保存合并后的数据
        with pd.ExcelWriter(output_file) as writer:
            combined_df.to_excel(writer, sheet_name="校验结果", index_label="校验序号")
    else:
        # 首次创建或覆盖模式
        with pd.ExcelWriter(output_file) as writer:
            df.to_excel(writer, sheet_name="校验结果", index_label="校验序号")
    
    # 打印提示信息
    print(f"校验结果已保存至 {output_file}")

if __name__ == "__main__":
    main()
