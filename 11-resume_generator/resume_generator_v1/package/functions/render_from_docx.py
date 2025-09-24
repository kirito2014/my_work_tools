import os,sys
import json
from datetime import datetime
from docxtpl import DocxTemplate

# 导入项目根目录以便导入其他模块
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
parent_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(parent_dir)
sys.path.append(project_root)
# print(f"当前文件路径: {current_file}")
# print(f"当前目录: {current_dir}")
# print(f"父级目录: {parent_dir}")
# print(f"项目根目录: {project_root}")
# print(f"Python搜索路径: {sys.path}")

# 导入 Doc 转 JSON 的模块
dj = None
try:
    # 尝试直接导入
    import doc_2_json as dj
except ImportError:
    try:
        # 尝试相对导入
        from . import doc_2_json as dj
    except ImportError:
        try:
            # 尝试绝对导入
            import package.functions.doc_2_json as dj
        except ImportError:
            # 最后的尝试：动态加载文件
            import importlib.util
            import sys
            dj_file_path = os.path.join(current_dir, "doc_2_json.py")
            if os.path.exists(dj_file_path):
                spec = importlib.util.spec_from_file_location("doc_2_json", dj_file_path)
                dj = importlib.util.module_from_spec(spec)
                sys.modules["doc_2_json"] = dj
                spec.loader.exec_module(dj)
                print(f"通过动态加载成功导入 doc_2_json.py 文件: {dj_file_path}")
            else:
                print(f"错误: 未找到 doc_2_json.py 文件在路径: {dj_file_path}")
                sys.exit(1)

if dj is None:
    print("错误: 未能导入 doc_2_json 模块")
    sys.exit(1)


def generate_resume_from_json(person_data, template_path, output_folder, person_name, bankname=None):
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
        output_filename = f"{bankname}人员简历_{person_name}_{current_date}.docx"
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


def process_json_data(json_data, template_path, input_file, output_folder, person_names="all", bankname=None):
    """
    修改后的处理函数：
    1. 强制更新机制：始终删除已存在的JSON并重新生成
    2. 前置文件检查：先验证Doc/Docx和模板文件存在性
    3. 增强错误处理链
    """
    try:
        # 确保JSON文件所在目录存在
        json_dir = os.path.dirname(json_data)
        if not os.path.exists(json_dir):
            os.makedirs(json_dir)
            print(f"📁 已创建JSON目录：{json_dir}")
        
        # ========== 前置检查阶段 ========== 
        # 验证Doc/Docx文件存在性
        if not os.path.isfile(input_file):
            print(f"❌ 关键错误：Doc/Docx源文件不存在 {os.path.abspath(input_file)}")
            return

        # 验证模板文件存在性
        if not os.path.isfile(template_path):
            print(f"❌ 关键错误：Word模板文件不存在 {os.path.abspath(template_path)}")
            return

        # ========== 数据准备阶段 ========== 
        # 强制删除已存在的JSON文件
        if os.path.exists(json_data):
            try:
                os.remove(json_data)
                print(f"🗑️ 已清除旧版JSON文件：{json_data}")
            except Exception as e:
                print(f"❌ 删除旧JSON文件失败：{str(e)}")
                return

        # 处理Doc/Docx生成新JSON
        print("\n🔨 正在转换Doc/Docx数据...")
        
        # 检查文件类型，如果是doc格式则先转换为docx
        processed_doc_path = input_file
        if input_file.lower().endswith('.doc'):
            print(f"检测到doc格式文件: {input_file}")
            # 创建临时目录存储转换后的文件
            temp_dir = os.path.join(os.path.dirname(input_file), "temp_converted")
            os.makedirs(temp_dir, exist_ok=True)
            # 转换doc到docx
            try:
                from . import doc_converter as dc
            except ImportError:
                try:
                    import doc_converter as dc
                except ImportError:
                    print("错误: 未找到 doc_converter.py 文件")
                    sys.exit(1)
            processed_doc_path = dc.convert_doc_to_docx(input_file, temp_dir)
        elif not input_file.lower().endswith('.docx'):
            print(f"❌ 不支持的文件格式: {input_file}。仅支持.doc和.docx格式。")
            return

        # 提取原始数据
        raw_resume_data = dj.extract_resume_universal(processed_doc_path)
        if not raw_resume_data:
            print("❌ Doc/Docx提取数据失败，请检查文档格式")
            return

        # 从文件名中提取工号
        emp_no = dj.extract_emp_no_from_filename(input_file)
        
        # 转换为模板格式
        result = dj.convert_to_template_format(raw_resume_data, emp_no)
        if not result:
            print("❌ Doc/Docx转换JSON失败，请检查文档数据格式")
            return

        # 保存新版JSON文件
        try:
            with open(json_data, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=4)
            print(f"✅ 已生成新版JSON文件：{json_data}")
        except Exception as e:
            print(f"❌ JSON文件保存失败：{str(e)}")
            return

        # ========== 简历生成阶段 ========== 
        print("\n📑 开始生成简历文档...")
        with open(json_data, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            if not data:
                print("❌ JSON数据为空，终止流程")
                return

            # 动态获取处理人员名单
            valid_names = []
            if person_names == "all":
                valid_names = list(data.keys())
                print(f"🔍 检测到需处理全部 {len(valid_names)} 位人员")
            else:
                valid_names = [name for name in person_names if name in data]
                print(f"🔍 指定处理 {len(valid_names)} 位人员，过滤无效名称 {len(person_names)-len(valid_names)} 个")

            # 创建输出目录（自动处理已存在情况）
            create_output_folder(output_folder)

            # 批量生成文档
            success_count = 0
            for person_name in valid_names:
                output_path = generate_resume_from_json(
                    data[person_name],
                    template_path,
                    output_folder,
                    person_name,
                    bankname,
                )
                if output_path: 
                    success_count += 1

            # 生成统计报告
            print("\n" + "="*40)
            print(f"🏁 处理完成！成功率 {success_count}/{len(valid_names)}")
            print(f"📁 输出路径：{os.path.abspath(output_folder)}")
            if len(valid_names) > success_count:
                print("⚠️  失败详情请查看上方错误提示")

    except Exception as e:
        print(f"\n❌ 全局异常：{str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# 配置文件（示例）
if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) < 4:
        print("用法: python render_from_docx.py <docx文件路径> <模板文件路径> <银行名称>")
        sys.exit(1)
        
    docx_file = sys.argv[1]
    template_file = sys.argv[2]
    bankname = sys.argv[3]
    
    # 根据bankname设置输出目录
    output_dir = os.path.join("output", bankname)
    
    # 从文件名提取工号和姓名，生成不带temp标识的JSON文件名
    base_name = os.path.splitext(os.path.basename(docx_file))[0]
    # 假设文件名格式为"工号+姓名+工作简历"，提取工号和姓名
    parts = base_name.split('+')
    if len(parts) >= 2:
        emp_no = parts[0]
        name = parts[1]
        # 生成不带temp标识的JSON文件名，格式为"工号_姓名_人员简历.json"
        json_filename = f"{emp_no}_{name}_人员简历.json"
    else:
        # 如果文件名格式不符合预期，则使用原文件名但移除temp标识
        json_filename = f"{base_name}.json"
    # 设置JSON文件路径为output/modify_json目录
    json_file = os.path.join("output", "modify_json", json_filename)

    process_json_data(
        json_data=json_file,
        template_path=template_file,
        input_file=docx_file,
        output_folder=output_dir,
        person_names="all",  # 可改为["张三", "李四"]指定人员
        bankname=bankname,
    )