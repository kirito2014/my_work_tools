"""
文件名(File Name)               :get_doc_file.py
作者(Author)                    :wangmujun@Sunline
编写时间(CreateTime)            :2024-10-01
版本号(Version)                 :V1.1.0
使用方法(Usage)                 :python get_doc_files.py <上线包路径> <上线清单>
功能描述(Descriptions)          :
    本脚本主要用于实现以下功能:
    1、根据提供的清单文件自动拾取对应的上线文件
    2、
依赖库(Dependences):
    - 
修改历史(Histories):
    v1.0.0 - 2024-10-01 - 初始版本
    v1.1.0 - 2024-10-01 - 增加日志记录;修改清单模板,增加配置文件中特殊处理的操作
    
"""


import os
import shutil
import sys
import fnmatch
import logging

# 配置logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 读取配置文件
def read_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    lines = [line.strip() for line in lines if line.strip()]
    unique_lines = list(set(lines))
    table_job_list = []
    for line in unique_lines:
        if '|' in line:
            parts = line.split('|')
            if len(parts) == 4:
                table_job_list.append((parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()))
            else:
                logger.error(f"配置读取错误: {line} 格式错误")
        else:
            logger.error(f"配置读取错误: {line} 中没有'|' 分隔符")
    return table_job_list

# 复制目录结构
def copy_directory_structure(src, dest, exclude_subdir=None):
    for root, dirs, files in os.walk(src):
        if exclude_subdir and exclude_subdir in root:
            continue
        structure = os.path.join(dest, os.path.relpath(root, src))
        if not os.path.isdir(structure):
            os.makedirs(structure)

def find_createtable_files(dir_path, table_name):
    patterns = [
        f"{table_name.upper()}.hql",
        f"createtable_{table_name.lower()}.hql"
    ]
    matches = []
    for pattern in patterns:
        matches.extend(fnmatch.filter(os.listdir(dir_path), pattern))
    return matches

def copy_all_files(base_dir, backup_dir, table_job_list):
    logger.info('----------------- 复制所有文件 -------------')
    error_log = {'config': [], 'createtable': [], 'hql': []}

    for idx, (table_name, job_number, job_person, special_flag) in enumerate(table_job_list):
        job_prefix = job_number.split('_')[0].lower()
        job_dir = os.path.join(base_dir, job_prefix)
        config_file = os.path.join(job_dir, 'config', f'{job_number.upper()}.csv')
        create_table_dir = os.path.join(job_dir, 'createtable')
        hql_dir = os.path.join(job_dir, 'pgm', 'hql', job_number.upper())
        hql_file_pattern = f'{table_name.lower()}_pc.hql'

        backup_job_dir = os.path.join(backup_dir, job_prefix)
        backup_config_file = os.path.join(backup_job_dir, 'config', f'{job_number.upper()}.csv')
        backup_create_table_dir = os.path.join(backup_job_dir, 'createtable')
        backup_hql_dir = os.path.join(backup_job_dir, 'pgm', 'hql', job_number.upper())
        backup_hql_file = os.path.join(backup_hql_dir, hql_file_pattern)

        # 复制配置文件
        if not os.path.exists(config_file):
            error_log['config'].append(f'{idx}:{job_number} 配置文件不存在, 开发人员{job_person}.')
        else:
            if not os.path.exists(os.path.dirname(backup_config_file)):
                os.makedirs(os.path.dirname(backup_config_file))
            shutil.copy2(config_file, backup_config_file)

        # 复制建表文件
        if not os.path.exists(create_table_dir):
            error_log['createtable'].append(f'{idx}:{create_table_dir} 建表语句目录不存在, 请检查.')
        else:
            create_table_files = find_createtable_files(create_table_dir, table_name)
            if len(create_table_files) == 0:
                error_log['createtable'].append(f'{idx}:{table_name} 对应的建表语句不存在, 开发人员{job_person}.')
            elif len(create_table_files) > 1:
                error_log['createtable'].append(f'{idx}:{table_name} 有多个建表语句存在, 开发人员{job_person}.')
            else:
                create_table_file = os.path.join(create_table_dir, create_table_files[0])
                backup_create_table_file = os.path.join(backup_create_table_dir, create_table_files[0])
                if not os.path.exists(os.path.dirname(backup_create_table_file)):
                    os.makedirs(os.path.dirname(backup_create_table_file))
                shutil.copy2(create_table_file, backup_create_table_file)

        # 复制跑批文件
        if not os.path.exists(hql_dir):
            error_log['hql'].append(f'{idx}:{job_number} 跑批语句目录不存在, 开发人员{job_person}.')
        else:
            hql_files = fnmatch.filter(os.listdir(hql_dir), hql_file_pattern)
            if len(hql_files) == 0:
                error_log['hql'].append(f'{idx}:{table_name} 在{job_number}文件夹下对应的跑批语句不存在, 开发人员{job_person}.')
            elif len(hql_files) > 1:
                error_log['hql'].append(f'{idx}:{table_name} 在{job_number}文件夹下有多个跑批语句存在, 开发人员{job_person}.')
            else:
                if not os.path.exists(os.path.dirname(backup_hql_file)):
                    os.makedirs(os.path.dirname(backup_hql_file))
                shutil.copy2(os.path.join(hql_dir, hql_files[0]), backup_hql_file)

    return error_log

def delete_special_files(backup_dir, table_job_list):
    logger.info('----------------- 删除特殊文件 -------------')
    special_dir = os.path.join(os.path.dirname(backup_dir), '建表语句备份')
    if not os.path.exists(special_dir):
        os.makedirs(special_dir)

    for idx, (table_name, job_number, job_person, special_flag) in enumerate(table_job_list):
        if special_flag == 'Y':
            job_prefix = job_number.split('_')[0].lower()
            backup_create_table_dir = os.path.join(backup_dir, job_prefix, 'createtable')
            create_table_files = find_createtable_files(backup_create_table_dir, table_name)

            if len(create_table_files) == 1:
                create_table_file = os.path.join(backup_create_table_dir, create_table_files[0])
                special_table_file = os.path.join(special_dir, create_table_files[0])
                shutil.copy2(create_table_file, special_table_file)  # 备份建表语句
                os.remove(create_table_file)  # 删除 agl-package-new 中的文件
                logger.info(f"特殊处理: {table_name} 的建表文件已备份到 {special_table_file} 并删除 {create_table_file}.")
            elif len(create_table_files) > 1:
                logger.error(f"{idx}:{table_name} 在{backup_create_table_dir}中有多个建表语句存在, 开发人员{job_person}.")

# 创建备份文件夹
def main(list_file, base_dir):
    logger.info('----------------- 创建文件目录 ---------------')
    backup_dir = base_dir + '-new'
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
    os.makedirs(backup_dir)

    copy_directory_structure(base_dir, backup_dir, exclude_subdir='hql')

    table_job_list = read_file(list_file)
    error_log = copy_all_files(base_dir, backup_dir, table_job_list)
    delete_special_files(backup_dir, table_job_list)

    for key in error_log:
        if error_log[key]:
            logger.error(f"{key} errors")
            for error in error_log[key]:
                logger.error(error)
            logger.error("")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("参数错误, 请使用以下命令: python get_doc_files.py <上线包路径> <上线清单> ")
        logger.error("执行前请检查上线清单是否配置正确 以 英文表名|作业编号|开发人员|是否特殊处理(Y/N) 的格式填写")
        sys.exit(1)
    base_dir = sys.argv[1]
    list_file = sys.argv[2]
    logger.info(f"================= 处理上线包 =================")
    main(list_file, base_dir)
    logger.info(f"================= 处理完成 ===================")
