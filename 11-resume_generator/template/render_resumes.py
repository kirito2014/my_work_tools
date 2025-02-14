import json
import test_import_json as tj

def main():
    # 读取生成的 JSON 文件
    with open("result.json", "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    # 设置参数
    template_path = "人员简历汇总_20241103.xlsx"
    output_folder = "output_resumes"
    person_names = "all"  # 指定要渲染的人员名单
    
    # 调用渲染函数
    tj.process_json_data(
        json_data=json_data,
        template_path=template_path,
        output_folder=output_folder,
        person_names=person_names
    )

if __name__ == "__main__":
    main()