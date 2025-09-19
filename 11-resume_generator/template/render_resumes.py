import json
import test_import_json as tj

def main():
    # 读取生成的 JSON 文件
    # with open("result.json", "r", encoding="utf-8") as f:
    #     json_data = json.load(f)

    config = {
        "json_file": "result.json",
        "template_file": "人员简历_模板.docx",
        "output_dir": "output_resumes",
        "input_file": "人员简历汇总_20241103.xlsx"
    }
    # 设置参数
    # with open(config["json_file"], "r", encoding="utf-8") as f:
    #     json_data = json.load(f)
    json_data=config["json_file"]
    input_file = config["input_file"]
    template_path = config["template_file"]  # 人员简历_模板.docx
    output_folder = config["output_dir"]
    person_names = "all"  # 指定要渲染的人员名单
    
    # 调用渲染函数
    tj.process_json_data(
        json_data=json_data,
        template_path=template_path,
        input_file=input_file,
        output_folder=output_folder,
        person_names=person_names
    )

if __name__ == "__main__":
    main()