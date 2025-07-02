import os
import re
from typing import List, Dict, Tuple
import pandas as pd
from openpyxl import load_workbook

def process_hql_files(folder_path: str, output_file: str = "resolve_main_table.xlsx"):
    """
    处理指定文件夹中以agl_开头_pc.hql结尾的文件，并将结果保存到Excel
    """
    # 检查并删除已存在的输出文件
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"已删除已存在的输出文件: {output_file}")
    
    # 准备结果DataFrame
    results = []
    
    for filename in os.listdir(folder_path):
        if filename.startswith("agl_") and filename.endswith("_pc.hql"):
            file_path = os.path.join(folder_path, filename)
            table_en_name = filename[:-7].upper()  # 去掉'_pc.hql'并大写
            print("\n=============================================")
            print(f"\n处理文件: {filename}")
            print(f"表英文名: {table_en_name}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 替换${version_num}为空
            content = content.replace("${version_num}", "")
            
            # 分析主表
            main_tables = analyze_main_tables(content, table_en_name)
            
            if main_tables:
                print("已解析到主表:")
                for block, tables in main_tables.items():
                    print(f"  第{block+1}组: {', '.join(tables)}")
                    # 将结果添加到列表中
                    for table in tables:
                        results.append({
                            "库名": "AGL",  # 固定为AGL
                            "聚合表名": table_en_name,
                            "加工组别": f"第{block+1}组",  # 加工组别
                            "加工主表": table
                        })
            else:
                print("未解析到任何主表.")
    
    # 将结果保存到Excel
    if results:
        df = pd.DataFrame(results)
        # 调整列顺序（不排序）
        df = df[["库名", "聚合表名", "加工组别", "加工主表"]]
        # 保存到Excel
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"\n结果已保存到: {output_file}")
    else:
        print("\n没有找到任何主表信息，未生成输出文件。")

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
    end_pos = content.rfind('\n', 0, end_match.start())
    
    if start_pos == -1 or end_pos == -1 or start_pos >= end_pos:
        return {}
    
    code_section = content[start_pos:end_pos].strip()
    
    # 按INSERT INTO分割代码块
    insert_blocks = []
    current_pos = 0
    insert_pattern = re.compile(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;(]+)", re.IGNORECASE)
    
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
        current_pos = match.end()
    
    main_tables = {}
    current_block = 0
    
    for insert_line, block_content in insert_blocks:
        # 只处理目标临时表 (AGL.AGLXXXXX_TM)
        target_table_match = re.search(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;(]+)", insert_line, re.IGNORECASE)
        if not target_table_match:
            continue
            
        target_table = target_table_match.group(1).upper()
        if not target_table.endswith(f"{table_en_name}_TM"):
            continue
            
        # 处理代码块内容
        tables_in_block = find_main_tables_in_block(block_content)
        
        # 递归解析主表
        final_tables = []
        for table in tables_in_block:
            if table.startswith("AGL."):
                # 递归查找AGL表的主表
                resolved_tables = resolve_agl_tables(table, insert_blocks, table_en_name)
                final_tables.extend(resolved_tables)
            else:
                # 非AGL表直接计入
                final_tables.append(table)
        
        if final_tables:
            # 去重并确保只保留最终主表（非AGL表）
            filtered_tables = [t for t in set(final_tables) if not t.startswith("AGL.")]
            if filtered_tables:
                main_tables[current_block] = filtered_tables
                current_block += 1
            else:
                # 如果所有解析后的表仍然是AGL表，则保留最初的AGL表
                main_tables[current_block] = list(set(final_tables))
                current_block += 1
    
    return main_tables

def find_main_tables_in_block(block_content: str) -> List[str]:
    """
    在代码块中查找主表，处理子查询情况，忽略LEFT JOIN的表
    """
    tables = []
    
    # 标准化代码块，去除注释但保留原始结构
    cleaned_content = re.sub(r"--.*?$", "", block_content, flags=re.MULTILINE)
    cleaned_content = re.sub(r"/\*.*?\*/", "", cleaned_content, flags=re.DOTALL)
    
    # 查找FROM后的内容（可能直接是表或子查询）
    from_match = re.search(r"\bFROM\b", cleaned_content, re.IGNORECASE)
    if not from_match:
        return tables
    
    from_pos = from_match.end()
    remaining_content = cleaned_content[from_pos:]
    
    # 检查是否在子查询中（FROM后是括号）
    if remaining_content.lstrip().startswith("("):
        # 找到子查询的结束括号
        subquery = extract_balanced_parentheses(remaining_content.lstrip())
        if subquery:
            # 从子查询中提取主表
            subquery_tables = extract_tables_from_subquery(subquery[1:-1])  # 去掉外层的括号
            tables.extend(subquery_tables)
    else:
        # 直接是表的情况
        table_match = re.search(r"([^\s,(;]+)", remaining_content)
        if table_match:
            table = table_match.group(1).upper()
            if "." in table:
                tables.append(table)
    
    return list(set(tables))  # 去重

def extract_balanced_parentheses(content: str) -> str:
    """
    提取完整的括号内容，包括嵌套括号
    """
    if not content.startswith("("):
        return ""
    
    balance = 1
    end_pos = 1
    while balance > 0 and end_pos < len(content):
        if content[end_pos] == "(":
            balance += 1
        elif content[end_pos] == ")":
            balance -= 1
        end_pos += 1
    
    return content[:end_pos] if balance == 0 else ""

def extract_tables_from_subquery(subquery: str) -> List[str]:
    """
    从子查询中提取FROM后的主表
    """
    tables = []
    # 去除换行和多余空格
    cleaned_subquery = re.sub(r"\s+", " ", subquery).strip()
    
    # 查找子查询中的第一个FROM
    from_match = re.search(r"\bFROM\b", cleaned_subquery, re.IGNORECASE)
    if not from_match:
        return tables
    
    from_pos = from_match.end()
    remaining = cleaned_subquery[from_pos:]
    
    # 检查子查询中的FROM后是否是括号
    if remaining.lstrip().startswith("("):
        nested_subquery = extract_balanced_parentheses(remaining.lstrip())
        if nested_subquery:
            return extract_tables_from_subquery(nested_subquery[1:-1])
    
    # 获取FROM后的表名（直到下一个空格或逗号等分隔符）
    table_match = re.search(r"([^\s,(;]+)", remaining)
    if table_match:
        table = table_match.group(1).upper()
        if "." in table:
            tables.append(table)
    
    return tables


def find_join_tables(content: str) -> List[str]:
    """
    查找INNER JOIN的表（不在子查询中）
    """
    tables = []
    pos = 0
    while True:
        join_match = re.search(r"\bINNER\s+JOIN\b", content[pos:], re.IGNORECASE)
        if not join_match:
            break
            
        pos += join_match.end()
        remaining = content[pos:]
        
        # 检查是否在子查询中
        if remaining.lstrip().startswith("("):
            continue
            
        # 获取JOIN后的表名
        table_match = re.search(r"([^\s,(;]+)", remaining)
        if table_match:
            table = table_match.group(1).upper()
            if "." in table:
                tables.append(table)
    
    return tables

def resolve_agl_tables(table: str, all_blocks: List[Tuple[str, str]], table_en_name: str) -> List[str]:
    """
    递归解析AGL开头的表，找到最终的主表
    """
    resolved_tables = []
    
    # 查找包含这个AGL表的INSERT INTO块
    for insert_line, block_content in all_blocks:
        target_table_match = re.search(r"INSERT\s+INTO\s+(?:TABLE\s+)?([^\s;(]+)", insert_line, re.IGNORECASE)
        if not target_table_match:
            continue
            
        target_table = target_table_match.group(1).upper()
        if target_table == table:
            # 在这个块中查找主表
            new_tables = find_main_tables_in_block(block_content)
            
            # 递归解析
            for new_table in new_tables:
                if new_table.startswith("AGL."):
                    resolved_tables.extend(resolve_agl_tables(new_table, all_blocks, table_en_name))
                else:
                    resolved_tables.append(new_table)
            
            return resolved_tables if resolved_tables else [table]  # 如果找不到非AGL表，返回原表
    
    return [table]  # 如果找不到对应的INSERT块，返回原表

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("用法: python resolve_tables.py <脚本路径>")
        sys.exit(1)
    
    folder_path = sys.argv[1]
    if not os.path.isdir(folder_path):
        print(f"Error: {folder_path} 不是一个有效目录.")
        sys.exit(1)
    
    print("=================处理开始====================")
    process_hql_files(folder_path)
    print("\n=================处理结束====================")