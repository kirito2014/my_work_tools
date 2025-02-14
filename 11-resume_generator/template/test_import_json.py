import os
import json
from datetime import datetime
from docxtpl import DocxTemplate

def generate_resume_from_json(person_data, template_path, output_folder, person_name):
    """
    根据JSON数据和Word模板生成简历文档。

    :param person_data: 单个人员的数据（字典格式）。
    :param template_path: Word模板文件路径。
    :param output_folder: 输出文件夹路径。
    :param person_name: 人员姓名。
    :return: 生成的简历文件路径。
    """
    try:
        # 加载Word模板
        doc = DocxTemplate(template_path)

        # 渲染模板
        doc.render(person_data)

        # 生成文件名
        current_date = datetime.now().strftime("%Y%m%d")
        output_filename = f"人员简历_{person_name}_{current_date}.docx"
        output_path = os.path.join(output_folder, output_filename)

        # 如果文件已存在，则删除
        if os.path.exists(output_path):
            os.remove(output_path)

        # 保存生成的文档
        doc.save(output_path)
        print(f"{person_name} 简历已生成,保存到: {output_path}")
        return output_path

    except Exception as e:
        print(f"{person_name} 生成简历时出错: {e}")
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

def process_json_data(json_data, template_path, output_folder, person_names="all"):
    """
    处理JSON数据，生成简历文档。

    :param json_data: 包含人员信息的JSON字符串。
    :param template_path: Word模板文件路径。
    :param output_folder: 输出文件夹路径。
    :param person_names: 需要处理的人员名称列表或"all"。
    :return: None
    """
    try:
        # 解析JSON数据
        with open(json_data, 'r',encoding='utf-8') as f:
            data = json.load(f)

            # 如果JSON为空，打印错误并退出
            if not data:
                print("错误: JSON数据为空。")
                return

            # 如果传入"all"，处理所有人员
            if person_names == "all":
                person_names = list(data.keys())

            # 遍历每个人员
            for person_name in person_names:
                if person_name in data:
                    person_data = data[person_name]
                    generate_resume_from_json(person_data, template_path, output_folder, person_name)
                else:
                    print(f"错误: 未找到人员 '{person_name}' 的数据。")

    except json.JSONDecodeError:
        print("错误: JSON数据格式不正确。")
    except Exception as e:
        print(f"处理JSON数据时出错: {e}")

# 示例调用
if __name__ == "__main__":
    # 示例JSON数据
    json_data = "result.json"

    # 模板文件路径
    template_path = "人员简历_模板.docx"

    # 输出文件夹路径
    output_folder = "output_resumes"

    # 调用函数生成简历
    # 示例1: 处理所有人员
    #process_json_data(json_data, template_path, output_folder, person_names="all")

    # 示例2: 处理指定人员
    process_json_data(json_data, template_path, output_folder, person_names="all")
