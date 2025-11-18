import re
import sys
import json
import os
import re
from docx import Document
from typing import Dict, List, Any  

# 处理PyInstaller打包后的路径问题
if getattr(sys, 'frozen', False):
    # 打包后的环境
    base_dir = os.path.dirname(sys.executable)
    # 确保工作目录设置为当前目录（exe所在目录）
    os.chdir(base_dir)
else:
    # 开发环境
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 添加项目根目录到Python路径，以便能够导入package模块
sys.path.append(base_dir)

# 导入doc转docx转换器
try:
    from . import doc_converter as dc
except ImportError:
    try:
        import doc_converter as dc
    except ImportError:
        # 尝试动态加载
        import importlib.util
        import sys
        dc_file_path = os.path.join(os.path.dirname(__file__), "doc_converter.py")
        if os.path.exists(dc_file_path):
            spec = importlib.util.spec_from_file_location("doc_converter", dc_file_path)
            dc = importlib.util.module_from_spec(spec)
            sys.modules["doc_converter"] = dc
            spec.loader.exec_module(dc)
        else:
            print(f"错误: 未找到 doc_converter.py 文件在路径: {dc_file_path}")
            sys.exit(1)

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
        #print(f"行 {row_idx}: {cells}")
        
        # 简化的键值对提取方法
        # 1. 建立常见键名列表（包括可能的变体）
        common_keys_patterns = {
            "姓名": ["姓名", "姓    名", "姓名："],
            "工作年限": ["工作年限", "工作年限："],
            "毕业时间": ["毕业时间", "毕业时间："],
            "毕业学校": ["毕业学校", "毕业学校："],
            "专业": ["专业", "专业："],
            "最高学历": ["最高学历", "最高学历："],
            "所在部门": ["所在部门", "所在部门："],
            "职称": ["职称", "职称：", "职    称"],
            "个人简介": ["个人简介", "个人简介："]
        }
        
        # 2. 遍历所有单元格对，寻找匹配的键值对
        i = 0
        while i < len(cells) - 1:
            current_cell = cells[i].strip()
            next_cell = cells[i+1].strip()
            
            # 检查当前单元格是否匹配任何键名模式
            matched_key = None
            for standard_key, patterns in common_keys_patterns.items():
                for pattern in patterns:
                    if pattern in current_cell or current_cell.replace(" ", "").replace(":", "").replace("：", "") == pattern.replace(" ", "").replace(":", "").replace("：", ""):
                        matched_key = standard_key
                        break
                if matched_key:
                    break
            
            # 如果找到匹配的键，且下一个单元格不是空且不是另一个键名
            if matched_key and next_cell:
                # 检查下一个单元格是否也是键名
                is_next_cell_key = False
                for patterns in common_keys_patterns.values():
                    for pattern in patterns:
                        if pattern in next_cell or next_cell.replace(" ", "").replace(":", "").replace("：", "") == pattern.replace(" ", "").replace(":", "").replace("：", ""):
                            is_next_cell_key = True
                            break
                    if is_next_cell_key:
                        break
                
                # 只有当下一个单元格不是键名时，才作为值
                if not is_next_cell_key:
                    # 检查是否已经存在这个键
                    if matched_key not in basic_info:
                        basic_info[matched_key] = next_cell
                        #print(f"  添加键值对: '{matched_key}' -> '{next_cell}'")
            
            i += 1
            
        # 3. 对于每行最后一个单元格，如果它匹配一个键，但还没有值，尝试从上一行找值
        if cells and len(cells) > 0:
            last_cell = cells[-1].strip()
            # 检查最后一个单元格是否匹配任何键名模式
            matched_key = None
            for standard_key, patterns in common_keys_patterns.items():
                for pattern in patterns:
                    if pattern in last_cell or last_cell.replace(" ", "").replace(":", "").replace("：", "") == pattern.replace(" ", "").replace(":", "").replace("：", ""):
                        matched_key = standard_key
                        break
                if matched_key:
                    break
            
            # 如果找到匹配的键，但还没有值，尝试找前一个非键单元格作为值
            if matched_key and matched_key not in basic_info:
                # 往前找非键值
                for j in range(len(cells)-2, -1, -1):
                    potential_value = cells[j].strip()
                    is_potential_key = False
                    for patterns in common_keys_patterns.values():
                        for pattern in patterns:
                            if pattern in potential_value or potential_value.replace(" ", "").replace(":", "").replace("：", "") == pattern.replace(" ", "").replace(":", "").replace("：", ""):
                                is_potential_key = True
                                break
                        if is_potential_key:
                            break
                    if not is_potential_key and potential_value:
                        basic_info[matched_key] = potential_value
                        #print(f"  添加键值对: '{matched_key}' -> '{potential_value}'")
                        break
    
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
        print(header_cells)
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
    
    if ability_start_row != -1:
        # 能力与资质部分有特定的表格结构，需要按行提取
        # 从能力与资质模块开始，逐行处理

        
        for row_idx in range(ability_start_row + 1, total_rows):
            row = table.rows[row_idx]
            cells = [cell.text.strip().replace("\n", " ") for cell in row.cells if cell.text.strip()]
            #print(f"行 {row_idx}: {cells}")
            if not cells:
                continue
                
            # 处理第0列和第1列：第0列作为键，第1列作为值
            if len(cells) >= 2:
                key1 = cells[0].replace(":", "").replace("：", "").strip().replace("\n", " ").replace(" ", "")

                value1 = cells[1].strip()
                if value1 == "None":
                    value1 = "无"
                
                # 只有当键不为空且键值不相等时才添加到字典
                if key1 and value1 and key1 != value1:
                    ability_data[key1] = value1


                    #print(f"  添加键值对: '{key1}' -> '{value1}'")


    
    resume_data["能力与资质"] = ability_data
    #print("能力与资质提取结果:", ability_data)

    return resume_data

def extract_person_name(basic_info: Dict) -> str:
    """
    从基本信息中提取人员姓名
    :param basic_info: 基本信息字典
    :return: 人员姓名
    """
    # 尝试不同的键名来获取姓名
    name_keys = ["姓    名", "姓名", "名字", "名称", "Name", "name", "姓    名"]
    for key in name_keys:
        if key in basic_info:
            name = basic_info[key].strip()
            # 清理姓名中的特殊字符
            name = re.sub(r'[【】（）()]', '', name)
            # 去掉姓名中的数字
            name = re.sub(r'\d+', '', name)
            return name
    
    return "未知人员"

def extract_emp_no_from_filename(file_path: str) -> str:
    """
    从文件名中提取工号（假设格式为：工号+姓名+工作简历.docx）
    :param file_path: 文件路径
    :return: 工号字符串
    """
    # 获取文件名（不包含路径）
    file_name = os.path.basename(file_path)
    
    # 使用正则表达式匹配文件名中的工号部分（假设工号由数字组成，后面跟+号）
    match = re.match(r'^(\d+)\+', file_name)
    if match:
        return match.group(1)
    
    # 如果文件名格式不符合预期，返回空字符串或默认值
    return ""

def convert_to_template_format(raw_data: Dict, emp_no: str = "") -> Dict:
    """
    将原始提取的数据转换为模板JSON格式
    :param raw_data: extract_resume_universal函数提取的原始数据
    :param emp_no: 工号（可选）
    :return: 符合模板格式的字典
    """
    # 提取基本信息
    basic_info = raw_data.get("基本情况", {})
    
    # 自动提取人员姓名 去除特殊符号后的姓名
    person_name = extract_person_name(basic_info)
    
    # 提取能力与资质内容
    ability_data = raw_data.get("能力与资质", {})
    
    # 构建符合模板格式的数据
    template_data = {
        basic_info.get("姓名"): {
            "BasicInfo": {
                "EmpNo": emp_no,  # 工号字段，放在Name前面
                "Name":  person_name,  # 使用处理过的姓名（去掉数字）
                "WorkYears": basic_info.get("工作年限", basic_info.get("经验年限", "")),
                "GraduationTime": basic_info.get("毕业时间", basic_info.get("毕业年份", "")),
                "GraduationSchool": basic_info.get("毕业学校", basic_info.get("学校", "")),
                "Major": basic_info.get("专    业", basic_info.get("专业", "")),
                "HighestEducation": basic_info.get("最高学历", basic_info.get("学历", "")),
                "Department": basic_info.get("所在部门", basic_info.get("部门", "")),
                "Title": basic_info.get("职称", basic_info.get("职位", basic_info.get("职务", ""))),
                "PersonalProfile": basic_info.get("个人简介", basic_info.get("简介", ""))
            },
            "WorkExperience": [],
            "ProjectExperience": [],
            "WorkAbility": {
                #匹配去除换行符后的键值
                "BusinessAbility": ability_data.get("业务与技术能力详述", ""),
                "Certification": ability_data.get("资质认证", ""),
                "Training": ability_data.get("参与培训", ""),
                "SkillTag": ability_data.get("技能标签", "")
            }
        }
    }
    
    # 处理工作经历
    work_experiences = raw_data.get("工作经历", [])
    for work in work_experiences:
        template_data[basic_info.get("姓名")]["WorkExperience"].append({
            "StartTime": work.get("开始时间", work.get("起始时间", "")),
            "EndTime": work.get("结束时间", work.get("截止时间", "至今")),
            "CompanyName": work.get("公司名称", work.get("公司", "")),
            "Position": work.get("担任职务", work.get("职位", work.get("职务", ""))),
            "JobDescription": work.get("工作职责说明", work.get("工作内容", work.get("职责", "")))
        })
    
    # 处理项目经历
    project_experiences = raw_data.get("项目经历", [])
    for project in project_experiences:
        template_data[basic_info.get("姓名")]["ProjectExperience"].append({
            "StartTime": project.get("开始时间", project.get("起始时间", "")),
            "EndTime": project.get("结束时间", project.get("截止时间", "")),
            "ProjectName": project.get("项目名称", project.get("项目", "")),
            "ProjectRole": project.get("项目角色", project.get("角色", project.get("职位", ""))),
            "JobDescription": project.get("项目职责说明", project.get("项目描述", project.get("职责", project.get("项目职责", ""))))
        })
    
    # 处理None值
    work_ability = template_data[basic_info.get("姓名")]["WorkAbility"]
    for key in work_ability:
        if work_ability[key] is None:
            work_ability[key] = ""
        elif work_ability[key] == "None":
            work_ability[key] = None
    
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
        # 检查文件类型，如果是doc格式则先转换为docx
        if doc_path.lower().endswith('.doc'):
            print(f"检测到doc格式文件: {doc_path}")
            # 创建临时目录存储转换后的文件
            # 使用base_dir或当前文件目录作为临时目录的基础
            temp_dir = os.path.join(base_dir, "temp_converted")
            os.makedirs(temp_dir, exist_ok=True)
            # 转换doc到docx
            doc_path = dc.convert_doc_to_docx(doc_path, temp_dir)
        elif not doc_path.lower().endswith('.docx'):
            raise ValueError(f"不支持的文件格式: {doc_path}。仅支持.doc和.docx格式。")
        
        # 提取原始数据
        raw_resume_data = extract_resume_universal(doc_path)
        
        # 从文件名中提取工号
        emp_no = extract_emp_no_from_filename(doc_path)
        
        # 转换为模板格式（传入工号）
        template_formatted_data = convert_to_template_format(raw_resume_data, emp_no)
        
        # 获取人员姓名（用于文件名）
        person_name = list(template_formatted_data.keys())[0]
        
        # 输出结果与保存文件
        print(f"【{person_name}的简历 - 原始提取数据】")
        print(json.dumps(raw_resume_data, ensure_ascii=False, indent=2))
        
        print(f"\n【{person_name}的简历 - 模板格式数据】")
        template_json = json.dumps(template_formatted_data, ensure_ascii=False, indent=2)
        #print(template_json)

        # 创建输出目录
        # 使用base_dir确保在打包环境中输出到正确位置
        original_dir = os.path.join(base_dir, "output", "original_json")
        modify_dir = os.path.join(base_dir, "output", "modify_json")
        os.makedirs(original_dir, exist_ok=True)
        os.makedirs(modify_dir, exist_ok=True)

        # 保存原始提取结果（文件名格式：工号_姓名_人员简历.json）
        raw_output_filename = os.path.join(original_dir, f"{emp_no}_{person_name}_人员简历.json")
        with open(raw_output_filename, "w", encoding="utf-8") as f:
            json.dump(raw_resume_data, f, ensure_ascii=False, indent=2)
        print(f"[OK] 已保存{person_name}的简历原始提取数据到：{raw_output_filename}")
        
        # 保存模板格式数据（文件名格式：工号_姓名_人员简历.json）
        modify_output_filename = os.path.join(modify_dir, f"{emp_no}_{person_name}_人员简历.json")
        with open(modify_output_filename, "w", encoding="utf-8") as f:
            f.write(template_json)
        print(f"[OK] 已保存{person_name}的简历模板格式数据到：{modify_output_filename}")
        
    except Exception as e:
        print(f"处理简历时发生错误: {e}")
        import traceback
        traceback.print_exc()
        print("请检查文件路径和文档格式")