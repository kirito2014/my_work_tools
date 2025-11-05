import os
import json
import sys

# 导入pandas
try:
    import pandas as pd
except ImportError:
    print("错误: 未找到pandas模块，请使用 pip install pandas 安装")
    sys.exit(1)

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 导入ExcelReader类
from package.utils.excel_reader import ExcelReader

def translate_headers(excel_headers):
    """
    将Excel表头翻译为标准键名
    """
    # 定义表头映射关系
    header_mapping = {
        "员工编号": "EmpNo",
        "工作名": "Name",
        "管理关系一级部门": "DepartmentLevel1",
        "管理关系二级部门": "DepartmentLevel2",
        "岗位": "Position",
        "专业级别": "ProfessionalLevel",
        "公司邮箱": "CompanyEmail",
        "入职日期": "EntryDate",
        "初次入职日期": "FirstEntryDate",
        "司龄": "CompanyYears",
        "工龄": "WorkYears",
        "base地": "BaseLocation",
        "性别": "Gender",
        "出生日期": "BirthDate",
        "年龄": "Age",
        "政治面貌": "PoliticalStatus",
        "证件号码": "IDNumber",
        "手机号码": "PhoneNumber",
        "毕业日期": "GraduationTime",
        "毕业院校": "GraduationSchool",
        "学历": "HighestEducation",
        "专业": "Major",
        "合同签订法人": "ContractLegalPerson"
    }
    
    # 返回翻译后的键名列表
    return [header_mapping.get(header, header) for header in excel_headers]

def convert_excel_to_json(excel_file_path, sheet_name="数据-人事花名册导出"):
    """
    读取Excel文件并转换为JSON格式
    """
    try:
        # 创建ExcelReader实例
        reader = ExcelReader(excel_file_path)
        
        # 读取Excel数据
        data = reader.read_excel(sheet_name)
        headers = data["headers"]
        rows = data["data"]
        
        # 翻译表头为标准键名
        translated_headers = translate_headers(headers)
        
        # 转换每行数据为字典
        result_list = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                if i < len(translated_headers):
                    # 处理空值
                    if pd.isna(value):
                        value = ""
                    elif isinstance(value, str):
                        value = value.strip()
                    value_str = str(value)
                    # 如果是EmpNo字段且是数字，格式化为5位数
                    if translated_headers[i] == "EmpNo" and value_str.strip().isdigit():
                        value_str = value_str.strip().zfill(5)
                    # 保留原始Name值（包含数字）用于文件名生成
                    row_dict[translated_headers[i]] = value_str
            result_list.append(row_dict)
        
        return result_list
    
    except Exception as e:
        print(f"读取Excel文件并转换为JSON时出错: {e}")
        import traceback
        traceback.print_exc()
        return []

def create_modify_json_format(employee_data):
    """
    创建符合modify_json格式的数据
    """
    modify_data = {}
    
    for emp in employee_data:
        emp_no = emp.get("EmpNo", "")
        # 使用原始姓名作为键名（包含数字）
        original_name = emp.get("Name", "未知")
        
        # 构建AdditionInfo字典，确保其中的Name字段不包含数字
        addition_info = {}
        for key, value in emp.items():
            if key == "Name":
                # 去除Name字段中的数字
                value = ''.join([char for char in str(value) if not char.isdigit()])
            addition_info[key] = value
        
        # 按照modify_json格式组织数据
        modify_data[original_name] = {
            "AddtionInfo": addition_info
        }
    
    return modify_data

def save_json_files(modify_data, output_dir):
    """
    保存JSON文件到指定目录
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    for name, data in modify_data.items():
        # 从AddtionInfo中获取工号和姓名，并将工号格式化为5位数（补齐前导零）
        emp_no = data["AddtionInfo"].get("EmpNo", "未知工号")
        # 如果是数字工号，格式化为5位数
        if emp_no != "未知工号" and emp_no.isdigit():
            emp_no = emp_no.zfill(5)
        # 构建文件名
        filename = os.path.join(output_dir, f"{emp_no}_{name}_人员信息.json")
        
        # 保存JSON文件
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"✅ 已保存信息JSON: {emp_no}_{name}_人员信息.json")
        except Exception as e:
            print(f"❌ 保存 {emp_no}_{name}_人员信息.json 时出错: {e}")

def main():
    # 设置默认的Excel文件路径
    default_excel_path = os.path.join("input", "技术人员名单-8月（删减版）.xlsx")
    
    # 允许用户通过命令行参数指定Excel文件路径
    if len(sys.argv) > 1:
        excel_file_path = sys.argv[1]
    else:
        excel_file_path = default_excel_path
        print(f"未指定Excel文件路径，使用默认路径: {excel_file_path}")
    
    # 确保Excel文件存在
    if not os.path.exists(excel_file_path):
        print(f"错误: Excel文件不存在: {excel_file_path}")
        sys.exit(1)
    
    # 设置输出目录（与modify_json同级）
    output_dir = os.path.join("output", "info_json")
    
    print("开始处理Excel文件...")
    
    # 转换Excel数据为JSON
    employee_data = convert_excel_to_json(excel_file_path)
    
    if not employee_data:
        print("没有成功转换任何数据，程序退出")
        sys.exit(1)
    
    print(f"成功转换 {len(employee_data)} 条员工数据")
    
    # 创建符合modify_json格式的数据
    modify_data = create_modify_json_format(employee_data)
    
    # 保存JSON文件
    save_json_files(modify_data, output_dir)
    
    print(f"\n处理完成！所有JSON文件已保存到: {output_dir}")

if __name__ == "__main__":
    main()