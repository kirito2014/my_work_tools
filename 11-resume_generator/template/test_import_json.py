import os,sys
import json
from datetime import datetime
from docxtpl import DocxTemplate

# 导入 Excel 转 JSON 的模块（需确保两个文件在同一目录）
try:
    import excel_2_json as ej
except ImportError:
    print("错误: 未找到 excel_2_json.py 文件")
    sys.exit(1)

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

def process_json_data(json_data, template_path, input_file,output_folder, person_names="all"):
    """
    修改后的处理函数：
    1. 自动检测 JSON 文件是否存在
    2. 若不存在则调用 Excel 转 JSON 逻辑
    3. 新增错误处理和进度提示
    """
    try:
        # 第一步：检查 JSON 文件是否存在
        if not os.path.exists(json_data):
            print(f"检测到 {json_data} 不存在，开始自动生成...")
            
            # 第二步：调用 Excel 转 JSON 逻辑
            
            if not os.path.exists(input_file):
                print(f"错误: 未找到Excel源文件 {input_file}")
                return
            
            print(f"正在处理Excel文件: {input_file}")
            result = ej.process_excel_to_json(input_file)
            
            if not result:
                print("错误: Excel 转换 JSON 失败")
                return
                
            # 第三步：保存生成的 JSON
            with open(json_data, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            print(f"已生成 JSON 文件: {json_data}")

        # 第四步：读取 JSON 数据
        with open(json_data, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if not data:
                print("错误: JSON 数据为空")
                return

            # 第五步：处理人员名单逻辑
            if person_names == "all":
                person_names = list(data.keys())
                
            # 第六步：创建输出文件夹
            create_output_folder(output_folder)

            # 第七步：遍历生成简历
            success_count = 0
            for person_name in person_names:
                if person_name in data:
                    output_path = generate_resume_from_json(
                        data[person_name],
                        template_path,
                        output_folder,
                        person_name
                    )
                    if output_path: success_count += 1
                else:
                    print(f"警告: 跳过未找到的人员 - {person_name}")

            # 最终统计
            print("\n处理完成！")
            print(f"成功生成 {success_count}/{len(person_names)} 份简历")
            print(f"输出目录: {os.path.abspath(output_folder)}")

    except Exception as e:
        print(f"处理过程中发生严重错误: {str(e)}")
        sys.exit(1)

# 示例调用
if __name__ == "__main__":
    # 配置文件路径
    config = {
        "json_file": "result.json",          # JSON 文件路径
        "template_file": "人员简历_模板.docx",  # 模板文件路径
        "output_dir": "output_resumes",       # 输出文件夹
        "input_file": "人员简历汇总_20241103.xlsx"    # Excel 文件路径
    }

    # 自动处理流程
    process_json_data(
        json_data=config["json_file"],
        template_path=config["template_file"],
        input_file=config["input_file"],
        output_folder=config["output_dir"],
        person_names="all"  # 可改为 ["张三", "李四"] 指定特定人员
    )
