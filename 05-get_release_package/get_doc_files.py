import os
import shutil
import fnmatch

def read_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    lines = [line.strip() for line in lines if line.strip()]
    unique_lines = list(set(lines))
    table_job_list = []
    for line in unique_lines:
        if '|' in line:
            parts = line.split('|')
            if len(parts) == 2:
                table_job_list.append((parts[0], parts[1]))
                
            else:
                print(f"Warning: Incorrect format in line '{line}'")
        else:
            print(f"Warning: Missing '|' in line '{line}'")
    return table_job_list

def copy_directory_structure(src, dest, exclude_subdir=None):
    for root, dirs, files in os.walk(src):
        if exclude_subdir and exclude_subdir in root:
            continue
        structure = os.path.join(dest, os.path.relpath(root, src))
        if not os.path.isdir(structure):
            os.makedirs(structure)

def find_createtable_files(dir_path, table_name):
    patterns = [
        f"*{table_name.lower()}*.hql",
        f"createtable_{table_name.lower()}.hql",
        f"{table_name.upper()}.hql"
    ]
    matches = []
    for pattern in patterns:
        matches.extend(fnmatch.filter(os.listdir(dir_path), pattern))
    return matches

def check_and_copy_files(base_dir, backup_dir, table_job_list):
    error_log = {'config': [], 'createtable': [], 'hql': []}
    for idx, (table_name, job_number) in enumerate(table_job_list):
        job_prefix = job_number.split('_')[0].lower()
        job_dir = os.path.join(base_dir, job_prefix)
        config_file = os.path.join(job_dir, 'config', f'{job_number}.csv')
        create_table_dir = os.path.join(job_dir, 'createtable')
        hql_dir = os.path.join(job_dir, 'pgm', 'hql', job_number)
        hql_file_pattern = f"{table_name.lower()}_pc.hql"

        backup_job_dir = os.path.join(backup_dir, job_prefix)
        backup_config_file = os.path.join(backup_job_dir, 'config', f'{job_number}.csv')
        backup_create_table_dir = os.path.join(backup_job_dir, 'createtable')
        backup_hql_dir = os.path.join(backup_job_dir, 'pgm', 'hql')

        # Check config file
        if not os.path.exists(config_file):
            error_log['config'].append(f'{idx+1}: {config_file} not found')
        else:
            if not os.path.exists(os.path.dirname(backup_config_file)):
                os.makedirs(os.path.dirname(backup_config_file))
            shutil.copy2(config_file, backup_config_file)

        # Check createtable files
        if not os.path.exists(create_table_dir):
            error_log['createtable'].append(f'{idx+1}: {create_table_dir} not found')
        else:
            create_table_files = find_createtable_files(create_table_dir, table_name)
            if len(create_table_files) == 0:
                error_log['createtable'].append(f'{idx+1}: No createtable file found for {table_name}')
            elif len(create_table_files) > 1:
                error_log['createtable'].append(f'{idx+1}: Multiple createtable files found for {table_name}')
            else:
                create_table_file = os.path.join(create_table_dir, create_table_files[0])
                if not os.path.exists(os.path.dirname(os.path.join(backup_create_table_dir, create_table_files[0]))):
                    os.makedirs(os.path.dirname(os.path.join(backup_create_table_dir, create_table_files[0])))
                shutil.copy2(create_table_file, os.path.join(backup_create_table_dir, create_table_files[0]))

        # Check hql files
        if not os.path.exists(hql_dir):
            error_log['hql'].append(f'{idx+1}: {hql_dir} not found')
        else:
            hql_files = fnmatch.filter(os.listdir(hql_dir), hql_file_pattern)
            if len(hql_files) == 0:
                error_log['hql'].append(f'{idx+1}: No hql file found for {table_name}')
            elif len(hql_files) > 1:
                error_log['hql'].append(f'{idx+1}: Multiple hql files found for {table_name}')
            else:
                if not os.path.exists(backup_hql_dir):
                    os.makedirs(backup_hql_dir)
                shutil.copy2(os.path.join(hql_dir, hql_files[0]), os.path.join(backup_hql_dir, hql_files[0]))

    return error_log

def main(list_file, base_dir):
    backup_dir = base_dir + '-bak'
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
    os.makedirs(backup_dir)

    copy_directory_structure(base_dir, backup_dir, exclude_subdir='hql')
    
    table_job_list = read_file(list_file)
    error_log = check_and_copy_files(base_dir, backup_dir, table_job_list)

    for key in error_log:
        if error_log[key]:
            print(f"{key} errors:")
            for error in error_log[key]:
                print(error)
            print()

if __name__ == "__main__":
    list_file = 'path/to/your/list.txt'
    base_dir = 'path/to/your/agl-package'
    main(list_file, base_dir)
