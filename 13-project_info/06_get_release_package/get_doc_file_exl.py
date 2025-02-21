#根据 上线清单选取上线包对应的信息并标记不符合上线内容文件
import os,re
import shutil
import sys
import fnmatch

#读取配置文件
def read_file(file_path):
	with open(file_path,'r',encoding='utf-8') as file:
		lines = file.readlines()
	lines = [line.strip() for line in lines if line.strip()]
	unique_lines = list(set(lines))
	table_job_list = []
	#print(unique_lines)
	for line in unique_lines:
		if '|' in line:
			parts = line.split('|')
			if len(parts) == 3:
				table_job_list.append((parts[0].strip(),parts[1].strip(),parts[2].strip()))
			else:
				print(f"配置读取错误:{line}格式错误")
		else:
			print(f"配置读取错误:{line} 中没有'|' 分隔符")
	return table_job_list

#复制目录结构
def copy_directory_structure(src,dest,exclude_subdir=None):
	for root, dirs ,files in os.walk(src):
		if exclude_subdir and exclude_subdir in root:
			continue
		structure = os.path.join(dest,os.path.relpath(root,src))
		if not os.path.isdir(structure):
			os.makedirs(structure)

def find_createtable_files(dir_path,table_name):
	table_name = table_name[8:]
	patterns = [
		f"{table_name.upper()}.hql",
		f"createtable_{table_name.lower()}.hql"
	]
	matches=[]
	for pattern in patterns:
		matches.extend(fnmatch.filter(os.listdir(dir_path),pattern))
	return matches

def check_and_copy_file(base_dir,backup_dir,table_job_list):
	print('----------------- 检查并复制文件 -------------')
	error_log = {'config':[],'createtable':[],'hql':[]}
	for idx,(table_name,job_number,job_person) in enumerate(table_job_list):
		#获取领域名称
		#print(f"----------------- 处理表{table_name}及作业号{job_number}的文件  -------------")
		job_prefix = job_number.split('_')[0].lower()
		job_dir = os.path.join(base_dir,job_prefix)
		#config
		config_file = os.path.join(job_dir,'config',f'{job_number.upper()}.csv')
		#print(config_file)
		#createtable
		create_table_dir = os.path.join(job_dir,'createtable')
		#hql
		hql_dir = os.path.join(job_dir,'pgm','hql',job_number.upper())
		hql_file_pattern = f'{table_name.lower()}_pc.hql'

		backup_job_dir = os.path.join(backup_dir,job_prefix)
		#config
		backup_config_file = os.path.join(backup_job_dir,'config',f'{job_number.upper()}.csv')
		#createtable
		backup_create_table_dir = os.path.join(backup_job_dir,'createtable')
		#hql
		backup_hql_dir = os.path.join(backup_job_dir,'pgm','hql',job_number.upper())
		backup_hql_file = os.path.join(backup_hql_dir,hql_file_pattern)
		#检查文件及目录是否存在
		if not os.path.exists(config_file):
			error_log['config'].append(f'{idx}:{job_number} 配置文件不存在,开发人员{job_person}.')
		else:
			if not os.path.exists(os.path.dirname(backup_config_file)):
				os.makedirs(os.path.dirname(backup_config_file))
			shutil.copy2(config_file,backup_config_file)

		if not os.path.exists(create_table_dir):
			error_log['createtable'].append(f'{idx}:{create_table_dir} 建表语句目录不存在,请检查.')
		else:
			create_table_files = find_createtable_files(create_table_dir,table_name)
			if len(create_table_files) == 0:
				error_log['createtable'].append(f'{idx}:{table_name} 对应的建表语句不存在,开发人员{job_person}.')
			elif len(create_table_files) > 1:
				error_log['createtable'].append(f'{idx}:{table_name} 有多个建表语句存在,开发人员{job_person}.')
			else:
				create_table_file = os.path.join(create_table_dir,create_table_files[0])
				if not os.path.exists(os.path.dirname(os.path.join(backup_create_table_dir,create_table_files[0]))):
					os.makedirs(os.path.dirname(os.path.join(backup_create_table_dir,create_table_files[0])))
				shutil.copy2(create_table_file,os.path.join(backup_create_table_dir,create_table_files[0]))

		if not os.path.exists(hql_dir):
			error_log['hql'].append(f'{idx}:{job_number} 跑批语句目录不存在,开发人员{job_person}.')
		else:
			hql_files = fnmatch.filter(os.listdir(hql_dir),hql_file_pattern)
			if len(hql_files) == 0:
				error_log['hql'].append(f'{idx}:{table_name} 在{job_number}文件夹下对应的跑批语句不存在,开发人员{job_person}.')
			elif len(hql_files) > 1:
				error_log['hql'].append(f'{idx}:{table_name} 在{job_number}文件夹下有多个跑批语句存在,开发人员{job_person}.')
			else:
				if not os.path.exists(os.path.dirname(backup_hql_file)):
					os.makedirs(os.path.dirname(backup_hql_file))
				shutil.copy2(os.path.join(hql_dir,hql_files[0]),backup_hql_file)
	return error_log

#创建备份文件夹
def main(list_file,base_dir):
	print('----------------- 创建文件目录 ---------------')
	backup_dir = base_dir + '-new'
	if  os.path.exists(backup_dir):
		shutil.rmtree(backup_dir)
	os.makedirs(backup_dir)

	copy_directory_structure(base_dir,backup_dir,exclude_subdir='hql')

	table_job_list = read_file(list_file)
	error_log = check_and_copy_file(base_dir,backup_dir,table_job_list)

	for key in error_log:
		if error_log[key]:
			print(f"{key} errors")
			for error in error_log[key]:
				print(error)
			print()

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: python get_doc_files.py <上线包路径> <上线清单>")
		sys.exit(1)
	base_dir = sys.argv[1]
	list_file = sys.argv[2]
	print(f"================= 处理上线包 =================")
	main(list_file,base_dir)
	print(f"================= 处理完成 ===================")

	#list_file = f"D:\\sunline_etl_tool\\get_doc_file\\上线清单.txt"
	#base_dir = f"D:\\agl-package-0703-2\\agl-package"
