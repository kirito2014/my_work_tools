import os
import time
import sys
def read_table_names(file_path):
    with open(file_path, 'r' , encoding='utf-8') as f:
        table_names = [line.strip() for line in f if line.strip()]
    return table_names

def merge_scripts(folder_path,table_names_file,output_file):
    table_names = read_table_names(table_names_file)
    scripts_content = []

    header_content = f" -- 本次合并涉及的表名：{', '.join(table_names)}\n\n"
    header_content += f" -- 本次合并共涉及 {len(table_names)} 张表\n\n "
    scripts_content.append(header_content)
    error_list = []
    for table_name in table_names:
        print(f"合并表 <{table_name}> 的检查文件")
        script_file = os.path.join(folder_path,f"{table_name.lower()}_chk.hql")
        #print(script_file)
        if os.path.exists(script_file):
            with open(script_file,'r',encoding='utf-8') as f:
                scripts_content.append(f.read())
            scripts_content.append('\n\n')
        else:
            print(f'表 <{table_name}> 的检查文件不存在请检查')
            error_list.append(table_name)
    if scripts_content[-1] == '\n\n':
        scripts_content.pop()



    with open(output_file,'w',encoding='utf-8',newline='\n') as f:
        f.write('\n'.join(scripts_content))
    return error_list

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python gen_dqc_script.py <file_path> <table_names_file>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    table_names_file = sys.argv[2]
    output_file = 'dqc_check_script_pc.hql'

    error_table_list = merge_scripts(folder_path,table_names_file,output_file)
    print(f"\n-- 所有列表中的文件已被合并到<{os.path.basename(output_file)}> --\n")
    if error_table_list:
        print('错误表名单:')
        for table_name in error_table_list:
            print(f"\t |{table_name}")
