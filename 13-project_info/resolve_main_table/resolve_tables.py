import os
import re
from typing import List, Dict, Tuple

def process_hql_files(folder_path: str):
    """
    处理指定文件夹中以agl_开头_pc.hql结尾的文件
    """
    for filename in os.listdir(folder_path):
        if filename.startswith("agl_") and filename.endswith("_pc.hql"):
            file_path = os.path.join(folder_path, filename)
            table_en_name = filename[:-7].upper()  # 去掉'_pc.hql'并大写
            print(f"\nProcessing file: {filename}")
            print(f"Table EN Name: {table_en_name}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 替换${version_num}为空
            content = content.replace("${version_num}", "")
            
            # 分析主表
            main_tables = analyze_main_tables(content, table_en_name)
            
            if main_tables:
                print("Main tables found:")
                for block, tables in main_tables.items():
                    print(f"  Code block {block}: {', '.join(tables)}")
            else:
                print("No main tables found.")

def analyze_main_tables(content: str, table_en_name: str) -> Dict[int, List[str]]:
    """
    分析HQL内容中的主表
    """
    # 获取代码区域 (从"get today data"的下一行到"drop target table's today partition"的上一行)
    start_pattern = re.compile(r"get today data", re.IGNORECASE)
    end_pattern = re.compile(r"drop target table's today partition", re.IGNORECASE)
    
    start_match = start_pattern.search(content)
    end_match = end_pattern.search(content)
    
    if not start_match or not end_match:
        return {}
    
    # 获取开始行(下一行)和结束行(上一行)
    start_pos = content.find('\n', start_match.end()) + 1
    #print(start_pos)
    end_pos = content.rfind('\n', 0, end_match.start())
    #print(end_pos)
    
    if start_pos == -1 or end_pos == -1 or start_pos >= end_pos:
        return {}
    
    code_section = content[start_pos:end_pos].strip()
    #print(code_section)
    
    # 按INSERT INTO分割代码块
    insert_blocks = []
    current_pos = 0
    insert_pattern = re.compile(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;]+)", re.IGNORECASE)
    
    while True:
        match = insert_pattern.search(code_section, current_pos)
        if not match:
            break
            
        # 查找下一个INSERT INTO或代码块结束
        next_match = insert_pattern.search(code_section, match.end())
        if next_match:
            block_content = code_section[match.end():next_match.start()].strip()
        else:
            block_content = code_section[match.end():].strip()
        
        insert_blocks.append((match.group(0), block_content))
        #print(insert_blocks)
        current_pos = match.end()
    
    main_tables = {}
    current_block = 0
    
    for insert_line, block_content in insert_blocks:
        # 检查是否是目标表 (table_en_name_TM)
        target_table_match = re.search(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;]+)", insert_line, re.IGNORECASE)
        if not target_table_match:
            continue
            
        target_table = target_table_match.group(1).upper().replace("(", "")
        print(target_table)
        if not target_table.endswith(f"{table_en_name}_TM"):
            continue
            
        # 处理代码块内容
        tables_in_block = find_main_tables_in_block(block_content)
        
        # 检查是否需要递归查找AGL表
        final_tables = []
        for table in tables_in_block:
            resolved_tables = resolve_agl_tables(table, insert_blocks, table_en_name)
            final_tables.extend(resolved_tables)
        
        if final_tables:
            main_tables[current_block] = list(set(final_tables))  # 去重
            current_block += 1
    
    return main_tables

def find_main_tables_in_block(block_content: str) -> List[str]:
    """
    在代码块中查找主表，忽略子查询中的表
    """
    tables = []
    
    # 标准化代码块，去除注释和换行
    cleaned_content = re.sub(r"--.*?$", "", block_content, flags=re.MULTILINE)  # 去除行注释
    cleaned_content = re.sub(r"/\*.*?\*/", "", cleaned_content, flags=re.DOTALL)  # 去除块注释
    cleaned_content = re.sub(r"\s+", " ", cleaned_content).strip()  # 标准化空格
    
    # 查找FROM后的第一个表（不在子查询中）
    from_pos = 0
    while True:
        from_match = re.search(r"\bFROM\b", cleaned_content[from_pos:], re.IGNORECASE)
        if not from_match:
            break
            
        from_pos += from_match.end()
        
        # 检查是否在子查询中
        subquery_count = cleaned_content[:from_pos].count("(") - cleaned_content[:from_pos].count(")")
        if subquery_count > 0:
            continue
            
        # 获取FROM后的表
        table_match = re.search(r"([^\s,(;]+)", cleaned_content[from_pos:])
        if table_match:
            table = table_match.group(1).upper()
            if "." in table:  # 确保是schema.table格式
                tables.append(table)
    
    # 查找INNER JOIN后的表（不在子查询中）
    join_pos = 0
    while True:
        join_match = re.search(r"\bINNER\s+JOIN\b", cleaned_content[join_pos:], re.IGNORECASE)
        if not join_match:
            break
            
        join_pos += join_match.end()
        
        # 检查是否在子查询中
        subquery_count = cleaned_content[:join_pos].count("(") - cleaned_content[:join_pos].count(")")
        if subquery_count > 0:
            continue
            
        # 获取INNER JOIN后的表
        table_match = re.search(r"([^\s,(;]+)", cleaned_content[join_pos:])
        if table_match:
            table = table_match.group(1).upper()
            if "." in table:  # 确保是schema.table格式
                tables.append(table)
    
    return tables

def resolve_agl_tables(table: str, all_blocks: List[Tuple[str, str]], table_en_name: str) -> List[str]:
    """
    递归解析AGL开头的表，找到最终的主表
    """
    if not table.startswith("AGL_"):
        return [table]
    
    # 查找包含这个AGL表的INSERT INTO块
    for insert_line, block_content in all_blocks:
        target_table_match = re.search(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;]+)", insert_line, re.IGNORECASE)
        if not target_table_match:
            continue
            
        target_table = target_table_match.group(1).upper()
        if target_table == table:
            # 在这个块中查找主表
            new_tables = find_main_tables_in_block(block_content)
            
            # 递归解析
            resolved_tables = []
            for new_table in new_tables:
                resolved_tables.extend(resolve_agl_tables(new_table, all_blocks, table_en_name))
            
            return resolved_tables
    
    return []

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <folder_path>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} is not a valid directory")
        sys.exit(1)
    
    process_hql_files(folder_path)