import os
import json
from datetime import datetime
from docxtpl import DocxTemplate

def generate_resume_from_json(json_data, template_path, output_folder):
    """
    根据JSON数据和Word模板生成简历文档。

    :param json_data: 包含人员信息的JSON字符串。
    :param template_path: Word模板文件路径。
    :param output_folder: 输出文件夹路径。
    :return: 生成的简历文件路径。
    """
    try:
        # 解析JSON数据
        data = json.loads(json_data)
        person_name = list(data.keys())[1]  # 获取人员姓名
        print(len(list(data.keys()))) #这里获取传入的json的长度
        person_data = data[person_name]  # 获取人员数据

        # 加载Word模板
        doc = DocxTemplate(template_path)

        # 渲染模板
        doc.render(person_data)

        # 生成文件名
        current_date = datetime.now().strftime("%Y%m%d")
        output_filename = f"人员简历_{person_name}_{current_date}.docx"
        output_path = os.path.join(output_folder, output_filename)

        # 如果文件已存在，则删除
        # 如果文件夹不存在则创建文件夹
        create_output_folder(output_folder)

        if os.path.exists(output_path):
            os.remove(output_path)

        # 保存生成的文档
        doc.save(output_path)
        print(f"{person_name} 简历已生成,保存到: {output_path}")
        return output_path

    except Exception as e:
        print(f"{person_name}生成简历时出错: {e}")
        return None

def create_output_folder(output_folder):
    """
    创建输出文件夹。

    :param output_folder: 输出文件夹路径。
    :return: None
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"输出文件夹已创建: {output_folder}")
    else:
        print(f"输出文件夹已存在: {output_folder}")

# 示例调用
if __name__ == "__main__":
    # 示例JSON数据
    json_data = """
    {
        "张三": {
            "BasicInfo": {
                "Name": "张三",
                "WorkYears": "5年",
                "GraduationTime": "2018/06",
                "GraduationSchool": "清华大学",
                "Major": "计算机科学与技术",
                "HighestEducation": "本科",
                "Department": "技术部",
                "Title": "高级工程师",
                "PersonalProfile": "热爱编程，擅长Python和Java。"
            },
            "WorkExperience": [
                {
                    "StartTime": "2023/01/01",
                    "EndTime": "至今",
                    "CompanyName": "xxx1公司",
                    "Position": "高级开发工程师",
                    "JobDescription": "负责核心模块开发。"
                },
                {
                    "StartTime": "2022/01/01",
                    "EndTime": "2022/12/31",
                    "CompanyName": "xxx2公司",
                    "Position": "开发工程师",
                    "JobDescription": "参与项目开发与维护。"
                }
            ],
            "ProjectExperience": [
                {
                    "StartTime": "2023/01/01",
                    "EndTime": "至今",
                    "ProjectName": "xxx1项目",
                    "ProjectRole": "项目经理",
                    "JobDescription": "负责项目整体规划与实施。"
                },
                {
                    "StartTime": "2022/01/01",
                    "EndTime": "2022/12/31",
                    "ProjectName": "xxx2项目",
                    "ProjectRole": "开发工程师",
                    "JobDescription": "参与项目开发与测试。"
                }
            ],
            "WorkAbility": {
                "BusinessAbility": "熟练掌握Python、Java等编程语言。",
                "Certification": "PMP认证",
                "Training": "敏捷开发培训",
                "SkillTag":"【测试】"
            }
        },
        "李四": {
            "BasicInfo": {
                "Name": "李四",
                "WorkYears": "5年",
                "GraduationTime": "2018/06",
                "GraduationSchool": "清华大学",
                "Major": "计算机科学与技术",
                "HighestEducation": "本科",
                "Department": "技术部",
                "Title": "高级工程师",
                "PersonalProfile": "热爱编程，擅长Python和Java。"
            },
            "WorkExperience": [
                {
                    "StartTime": "2023/01/01",
                    "EndTime": "至今",
                    "CompanyName": "xxx1公司",
                    "Position": "高级开发工程师",
                    "JobDescription": "负责核心模块开发。"
                },
                {
                    "StartTime": "2022/01/01",
                    "EndTime": "2022/12/31",
                    "CompanyName": "xxx2公司",
                    "Position": "开发工程师",
                    "JobDescription": "参与项目开发与维护。"
                }
            ],
            "ProjectExperience": [
                {
                    "StartTime": "2023/01/01",
                    "EndTime": "至今",
                    "ProjectName": "xxx1项目",
                    "ProjectRole": "项目经理",
                    "JobDescription": "负责项目整体规划与实施。"
                },
                {
                    "StartTime": "2022/01/01",
                    "EndTime": "2022/12/31",
                    "ProjectName": "xxx2项目",
                    "ProjectRole": "开发工程师",
                    "JobDescription": "参与项目开发与测试。"
                }
            ],
            "WorkAbility": {
                "BusinessAbility": "熟练掌握Python、Java等编程语言。",
                "Certification": "PMP认证",
                "Training": "敏捷开发培训",
                "SkillTag":"【测试】"
            }
        }
    }
    """

    # 模板文件路径
    template_path = "人员简历_模板.docx"

    # 输出文件夹路径
    output_folder = "output_resumes"

    # 调用函数生成简历
    generate_resume_from_json(json_data, template_path, output_folder)
