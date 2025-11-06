import os,sys
import json
from datetime import datetime
from docxtpl import DocxTemplate

# 导入项目根目录以便导入其他模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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

def process_json_data(json_data, template_path, input_file, output_folder, person_names="all"):
    """
    修改后的处理函数：
    1. 强制更新机制：始终删除已存在的JSON并重新生成
    2. 前置文件检查：先验证Excel和模板文件存在性
    3. 增强错误处理链
    """
    try:
        # ========== 前置检查阶段 ========== 
        # 验证Excel文件存在性
        if not os.path.isfile(input_file):
            print(f"[ERROR] 关键错误：Excel源文件不存在 {os.path.abspath(input_file)}")
            return

        # 验证模板文件存在性
        if not os.path.isfile(template_path):
            print(f"[ERROR] 关键错误：Word模板文件不存在 {os.path.abspath(template_path)}")
            return

        # ========== 数据准备阶段 ==========
        # 强制删除已存在的JSON文件
        if os.path.exists(json_data):
            try:
                os.remove(json_data)
                print(f"[DELETE] 已清除旧版JSON文件：{json_data}")
            except Exception as e:
                print(f"[ERROR] 删除旧JSON文件失败：{str(e)}")
                return

        # 处理Excel生成新JSON
        print("\n[PROCESS] 正在转换Excel数据...")
        result = ej.process_excel_to_json(input_file)
        
        if not result:
            print("[ERROR] Excel转换JSON失败，请检查Excel数据格式")
            return
            
        # 保存新版JSON文件
        try:
            with open(json_data, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            print(f"[OK] 已生成新版JSON文件：{json_data}")
        except Exception as e:
            print(f"[ERROR] JSON文件保存失败：{str(e)}")
            return

        # ========== 简历生成阶段 ==========
        print("\n[INFO] 开始生成简历文档...")
        with open(json_data, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            if not data:
                print("[ERROR] JSON数据为空，终止流程")
                return

            # 动态获取处理人员名单
            valid_names = []
            if person_names == "all":
                valid_names = list(data.keys())
                print(f"[INFO] 检测到需处理全部 {len(valid_names)} 位人员")
            else:
                valid_names = [name for name in person_names if name in data]
                print(f"[INFO] 指定处理 {len(valid_names)} 位人员，过滤无效名称 {len(person_names)-len(valid_names)} 个")

            # 创建输出目录（自动处理已存在情况）
            create_output_folder(output_folder)

            # 批量生成文档
            success_count = 0
            for person_name in valid_names:
                output_path = generate_resume_from_json(
                    data[person_name],
                    template_path,
                    output_folder,
                    person_name
                )
                if output_path: 
                    success_count += 1

            # 生成统计报告
            print(f"\n" + "="*40)
            print(f"[INFO] 处理完成！成功率 {success_count}/{len(valid_names)}")
            print(f"[INFO] 输出路径：{os.path.abspath(output_folder)}")
            if len(valid_names) > success_count:
                print("[WARNING] 失败详情请查看上方错误提示")

    except Exception as e:
        print(f"\n[ERROR] 全局异常：{str(e)}")
        sys.exit(1)

# 配置文件（示例）
if __name__ == "__main__":
    config = {
        "json_file": "template/result.json",
        "template_file": "template/人员简历_模板_01.docx",
        "output_dir": "output/output_resumes/",
        "input_file": "template/人员简历汇总_20241103.xlsx"
    }

    process_json_data(
        json_data=config["json_file"],
        template_path=config["template_file"],
        input_file=config["input_file"],
        output_folder=config["output_dir"],
        person_names="all"  # 可改为["张三", "李四"]指定人员
    )