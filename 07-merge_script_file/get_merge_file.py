import os
import sys

def read_table_names(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        table_names = [line.strip() for line in f if line.strip()]
    return table_names

def merge_scripts(folder_path, table_names_file, output_file):
    table_names = read_table_names(table_names_file)
    scripts_content = []

    # 生成涉及表名和表数量部分
    header_content = f"-- 本次脚本涉及的表名: {', '.join(table_names)}\n"
    header_content += f"-- 本次共涉及 {len(table_names)} 个表\n\n"
    scripts_content.append(header_content)

    for table_name in table_names:
        script_file = os.path.join(folder_path, f"{table_name}_PC.hql")
        if os.path.exists(script_file):
            with open(script_file, 'r', encoding='utf-8') as f:
                scripts_content.append(f.read())
            scripts_content.append('\n\n')  # 添加两个空行

    # 移除最后两个空行
    if scripts_content[-1] == '\n\n':
        scripts_content.pop()

    with open(output_file, 'w', encoding='utf-8', newline='\n') as f:
        f.write(''.join(scripts_content))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("用法: python script.py <文件夹路径> <表名列表文件路径>")
        sys.exit(1)

    folder_path = sys.argv[1]
    table_names_file = sys.argv[2]
    output_file = "dqc_check_script_pc.hql"

    merge_scripts(folder_path, table_names_file, output_file)
    print(f"所有脚本已合并并保存到 {output_file}")
