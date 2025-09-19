import json
import re
import sys
from docx import Document
from typing import Dict, List, Any

def extract_resume_universal(doc_path: str) -> Dict:
    """
    通用简历提取函数：通过关键词自动定位模块，适配不同行数的工作/项目经历
    :param doc_path: 简历文件路径
    :return: 结构化简历字典
    """
    doc = Document(doc_path)
    table = doc.tables[0]  # 简历核心内容在第1个表格
    resume_data = {}
    total_rows = len(table.rows)

    # -------------------------- 1. 辅助函数：通过关键词找模块起始行 --------------------------
    def find_module_start_row(keyword: str) -> int:
        """根据关键词（如"工作经历"）找到模块表头所在行号，未找到返回-1"""
        for row_idx in range(total_rows):
            row_text = "".join([cell.text.strip() for cell in table.rows[row_idx].cells])
            if keyword in row_text:
                return row_idx
        return -1

    # -------------------------- 2. 提取基本情况（固定在"工作经历"模块之前） --------------------------
    work_start_row = find_module_start_row("工作经历")
    if work_start_row == -1:
        work_start_row = find_module_start_row("工作经历（由近至远）")
    
    basic_info = {}
    # 基本情况：从第1行（跳过表头行）提取到"工作经历"之前
    for row_idx in range(1, work_start_row):
        row = table.rows[row_idx]
        cells = [cell.text.strip().replace("\n", " ") for cell in row.cells]
        
        # 调试输出，查看每行的单元格内容
        print(f"行 {row_idx}: {cells}")
        
        # 处理第0列和第1列：第0列作为键，第1列作为值
        if len(cells) >= 2:
            key1 = cells[0].replace(":", "").replace("：", "").strip()
            value1 = cells[1].strip()
            
            # 只有当键不为空且键值不相等时才添加到字典
            if key1 and value1 and key1 != value1:
                basic_info[key1] = value1
                print(f"  添加键值对: '{key1}' -> '{value1}'")
        
        # 处理第3列和第5列：第3列作为键，第5列作为值（针对表格格式问题）
        if len(cells) >= 6:
            key2 = cells[3].replace(":", "").replace("：", "").strip()
            value2 = cells[5].strip()
            
            # 只有当键不为空且键值不相等时才添加到字典
            if key2 and value2 and key2 != value2:
                basic_info[key2] = value2
                print(f"  添加键值对: '{key2}' -> '{value2}'")
    
    resume_data["基本情况"] = basic_info

    # -------------------------- 3. 自动提取工作经历（表头→数据行→下模块前） --------------------------
    work_exp: List[Dict] = []
    if work_start_row != -1:
        # 工作经历表头行：模块名行的下一行（含"开始时间""结束时间"等列名）
        work_header_row = work_start_row + 1
        # 工作经历结束行：下一个模块（项目经历）的起始行 - 1
        project_start_row = find_module_start_row("项目经历")
        if project_start_row == -1:
            project_start_row = find_module_start_row("项目经历（由近至远）")
        work_end_row = project_start_row - 1 if project_start_row != -1 else total_rows - 1

        # 提取工作经历列名
        header_cells = [cell.text.strip() for cell in table.rows[work_header_row].cells if cell.text.strip()]
        # 提取数据行（从表头下一行到工作经历结束行）
        for row_idx in range(work_header_row + 1, work_end_row + 1):
            row = table.rows[row_idx]
            cells = [cell.text.strip().replace("\n", " ") for cell in row.cells if cell.text.strip()]
            if cells:
                exp_dict = {header_cells[i]: cells[i] if i < len(cells) else "" for i in range(len(header_cells))}
                work_exp.append(exp_dict)
    resume_data["工作经历"] = work_exp

    # -------------------------- 4. 自动提取项目经历（表头→数据行→下模块前） --------------------------
    project_exp: List[Dict] = []
    project_start_row = find_module_start_row("项目经历")
    if project_start_row == -1:
        project_start_row = find_module_start_row("项目经历（由近至远）")
    
    if project_start_row != -1:
        # 项目经历表头行：模块名行的下一行
        project_header_row = project_start_row + 1
        # 项目经历结束行：下一个模块的起始行 - 1
        ability_start_row = find_module_start_row("能力与资质")
        if ability_start_row == -1:
            ability_start_row = find_module_start_row("技能")
        project_end_row = ability_start_row - 1 if ability_start_row != -1 else total_rows - 1

        # 提取项目经历列名
        header_cells = [cell.text.strip() for cell in table.rows[project_header_row].cells if cell.text.strip()]
        # 提取数据行
        for row_idx in range(project_header_row + 1, project_end_row + 1):
            row = table.rows[row_idx]
            cells = [cell.text.strip().replace("\n", " ") for cell in row.cells if cell.text.strip()]
            if cells:
                # 处理"项目名称为空"的情况
                if len(cells) >= 3 and cells[2] in ["", "/", "无"]:
                    cells[2] = "未标注项目名称"
                exp_dict = {header_cells[i]: cells[i] if i < len(cells) else "" for i in range(len(header_cells))}
                project_exp.append(exp_dict)
    resume_data["项目经历"] = project_exp

    # -------------------------- 5. 提取能力与资质（固定在项目经历之后） --------------------------
    ability_data = {}
    ability_start_row = find_module_start_row("能力与资质")
    if ability_start_row == -1:
        ability_start_row = find_module_start_row("技能")
        ability_keys = ["技能详述", "资质认证", "培训经历", "技能标签"]
    else:
        ability_keys = ["业务与技术能力详述", "资质认证", "参与培训", "技能标签"]
    
    if ability_start_row != -1:
        key_idx = 0
        for row_idx in range(ability_start_row + 1, total_rows):
            row = table.rows[row_idx]
            cell_content = "".join([cell.text.strip().replace("\n", " ") for cell in row.cells if cell.text.strip()])
            if cell_content and key_idx < len(ability_keys):
                ability_data[ability_keys[key_idx]] = cell_content
                key_idx += 1
    resume_data["能力与资质"] = ability_data

    return resume_data

def extract_person_name(basic_info: Dict) -> str:
    """
    从基本信息中提取人员姓名
    :param basic_info: 基本信息字典
    :return: 人员姓名
    """
    # 尝试不同的键名来获取姓名
    name_keys = ["姓 名", "姓名", "名字", "名称", "Name", "name", "姓    名"]
    for key in name_keys:
        if key in basic_info:
            name = basic_info[key].strip()
            # 清理姓名中的特殊字符
            name = re.sub(r'[【】（）()]', '', name)
            return name
    
    return "未知人员"

def convert_to_template_format(raw_data: Dict) -> Dict:
    """
    将原始提取的数据转换为模板JSON格式
    :param raw_data: extract_resume_universal函数提取的原始数据
    :return: 符合模板格式的字典
    """
    # 提取基本信息
    basic_info = raw_data.get("基本情况", {})
    
    # 自动提取人员姓名
    person_name = extract_person_name(basic_info)
    
    # 构建符合模板格式的数据
    template_data = {
        person_name: {
            "BasicInfo": {
                "Name": basic_info.get("姓 名", basic_info.get("姓名", basic_info.get("姓    名", person_name))),
                "WorkYears": basic_info.get("工作年限", basic_info.get("经验年限", "")),
                "GraduationTime": basic_info.get("毕业时间", basic_info.get("毕业年份", "")),
                "GraduationSchool": basic_info.get("毕业学校", basic_info.get("学校", "")),
                "Major": basic_info.get("专    业", basic_info.get("专业", "")),
                "HighestEducation": basic_info.get("最高学历", basic_info.get("学历", "")),
                "Department": basic_info.get("所在部门", basic_info.get("部门", "")),
                "Title": basic_info.get("职    称", basic_info.get("职位", basic_info.get("职务", ""))),
                "PersonalProfile": basic_info.get("个人简介", basic_info.get("简介", ""))
            },
            "WorkExperience": [],
            "ProjectExperience": [],
            "WorkAbility": {
                "BusinessAbility": raw_data.get("能力与资质", {}).get("业务与技术能力详述", 
                                    raw_data.get("能力与资质", {}).get("技能详述", "")),
                "Certification": raw_data.get("能力与资质", {}).get("资质认证", ""),
                "Training": raw_data.get("能力与资质", {}).get("参与培训", 
                                raw_data.get("能力与资质", {}).get("培训经历", "")),
                "SkillTag": raw_data.get("能力与资质", {}).get("技能标签", "")
            }
        }
    }
    
    # 处理工作经历
    work_experiences = raw_data.get("工作经历", [])
    for work in work_experiences:
        template_data[person_name]["WorkExperience"].append({
            "StartTime": work.get("开始时间", work.get("起始时间", "")),
            "EndTime": work.get("结束时间", work.get("截止时间", "至今")),
            "CompanyName": work.get("公司名称", work.get("公司", "")),
            "Position": work.get("担任职务", work.get("职位", work.get("职务", ""))),
            "JobDescription": work.get("工作职责说明", work.get("工作内容", work.get("职责", "")))
        })
    
    # 处理项目经历
    project_experiences = raw_data.get("项目经历", [])
    for project in project_experiences:
        template_data[person_name]["ProjectExperience"].append({
            "StartTime": project.get("开始时间", project.get("起始时间", "")),
            "EndTime": project.get("结束时间", project.get("截止时间", "")),
            "ProjectName": project.get("项目名称", project.get("项目", "")),
            "ProjectRole": project.get("项目角色", project.get("角色", project.get("职位", ""))),
            "JobDescription": project.get("项目职责说明", project.get("项目描述", project.get("职责", "")))
        })
    
    return template_data

# -------------------------- 执行提取和转换 --------------------------
if __name__ == "__main__":
    # 可以传入不同的简历文件
    # 参数传入
    if len(sys.argv) < 2:
        print("请提供简历文件路径作为参数")
        sys.exit(1)
    
    doc_path = sys.argv[1]
    
    try:
        # 提取原始数据
        raw_resume_data = extract_resume_universal(doc_path)
        
        # 转换为模板格式
        template_formatted_data = convert_to_template_format(raw_resume_data)
        
        # 获取人员姓名（用于文件名）
        person_name = list(template_formatted_data.keys())[0]
        
        # 输出结果与保存文件
        print(f"【{person_name}的简历 - 原始提取数据】")
        print(json.dumps(raw_resume_data, ensure_ascii=False, indent=2))
        
        print(f"\n【{person_name}的简历 - 模板格式数据】")
        template_json = json.dumps(template_formatted_data, ensure_ascii=False, indent=2)
        print(template_json)

        # 保存为JSON文件
        output_filename = f"{person_name}_简历_模板格式.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(template_json)
        print(f"\n✅ 已保存{person_name}的简历模板格式数据到：{output_filename}")
        
        # 同时保存原始提取结果
        raw_output_filename = f"{person_name}_简历_原始提取结果.json"
        with open(raw_output_filename, "w", encoding="utf-8") as f:
            json.dump(raw_resume_data, f, ensure_ascii=False, indent=2)
        print(f"✅ 已保存{person_name}的简历原始提取数据到：{raw_output_filename}")
        
    except Exception as e:
        print(f"处理简历时发生错误: {e}")
        import traceback
        traceback.print_exc()
        print("请检查文件路径和文档格式")