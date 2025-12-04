import pandas as pd
import numpy as np
import random
from collections import defaultdict

def generate_test_data_a():
    """生成测试数据A - 更复杂的依赖关系"""
    
    # 定义基础数据库前缀
    db_prefixes = ['P_CZ', 'P_BJ', 'P_SH', 'P_GZ', 'P_SZ']
    db_middles = ['DWS', 'STG', 'ODS', 'DIM', 'FACT', 'TMP']
    db_suffixes = ['PRD', 'DEV', 'TEST', 'UAT', 'SIT']
    
    # 生成数据库名称
    def generate_db_name():
        return f"{random.choice(db_prefixes)}_{random.choice(db_middles)}_{random.choice(db_suffixes)}"
    
    # 生成表名
    base_table_names = [
        'CUSTOMER', 'ORDER', 'PRODUCT', 'SALES', 'LOGISTICS',
        'PAYMENT', 'INVENTORY', 'USER', 'ACCOUNT', 'TRANSACTION',
        'MARKETING', 'FINANCE', 'HR', 'WAREHOUSE', 'SUPPLY_CHAIN'
    ]
    
    table_suffixes = ['INFO', 'DATA', 'DETAIL', 'MASTER', 'HISTORY', 'SUMMARY', 'REPORT']
    
    def generate_table_name():
        if random.random() < 0.3:
            # 生成有层次结构的表名
            prefix = random.choice(['DWS', 'ODS', 'DIM', 'FACT', 'STG'])
            base = random.choice(base_table_names)
            suffix = random.choice(table_suffixes)
            return f"{prefix}_{base}_{suffix}"
        else:
            # 生成简单表名
            base = random.choice(base_table_names)
            suffix = random.choice(table_suffixes)
            return f"{base}_{suffix}"
    
    # 生成节点名（类似P_CZ_DWS_PRD格式）
    def generate_node_name():
        prefix = random.choice(['P_CZ', 'P_BJ', 'P_SH'])
        middle = random.choice(['DWS', 'ODS', 'STG', 'DIM'])
        suffix = random.choice(['PRD', 'UAT', 'DEV'])
        num = random.randint(1, 99)
        return f"{prefix}_{middle}_{suffix}_{num:02d}"
    
    # 生成10个BLOCK
    data = []
    node_pool = []  # 用于存储生成的节点，供后续使用
    
    print("正在生成复杂依赖关系的测试数据...")
    
    for block_num in range(1, 11):
        # 每个BLOCK有1-3个不同的NODE_NAME
        block_nodes = []
        for _ in range(random.randint(1, 3)):
            node_name = generate_node_name()
            block_nodes.append(node_name)
            node_pool.append(node_name)
        
        # 每个BLOCK有2-6个STATEMENT
        for stmt_num in range(1, random.randint(3, 7)):
            # 选择当前语句的节点
            current_node = random.choice(block_nodes)
            
            # 确定这个语句有多少个来源表 (1-4个)
            num_sources = random.randint(1, 4)
            
            for source_idx in range(num_sources):
                # 决定来源表类型
                source_type = random.choice(['self', 'other_node', 'independent'])
                
                if source_type == 'self':
                    # 来源表与目标表相同（用于DML语句）
                    source_table = current_node
                    source_db = generate_db_name()
                    
                elif source_type == 'other_node':
                    # 来源表是其他节点（测试迭代推导）
                    if node_pool and random.random() < 0.7:
                        # 70%概率选择已存在的节点
                        source_table = random.choice(node_pool)
                    else:
                        source_table = generate_node_name()
                        node_pool.append(source_table)
                    source_db = generate_db_name()
                    
                else:
                    # 独立来源表
                    source_table = generate_table_name()
                    source_db = generate_db_name()
                
                # 构建数据行
                data.append({
                    'NODE_NAME': current_node,
                    'FILE_ID': 10000 + block_num * 100,
                    'BLOCK_NUM': block_num,
                    'STATMENT_NUM': stmt_num,
                    'TARGET_DB_NAME': generate_db_name(),
                    'TARGET_TABLE_NAME': current_node,
                    'SOURCE_TABLE': f'{source_db}.{source_table}',
                    'SOURCE_DB_NAME': source_db,
                    'SOURCE_TABLE_NAME': source_table
                })
    
    # 创建一些特定的测试用例
    
    print("正在创建特定测试用例...")
    
    # 用例1: 直接匹配用例
    # NODE_NAME在B表中，应该直接标记为1
    direct_match_node = "P_CZ_DWS_PRD_01"
    for block_num in range(11, 13):
        for stmt_num in range(1, random.randint(2, 4)):
            data.append({
                'NODE_NAME': direct_match_node,
                'FILE_ID': 20001,
                'BLOCK_NUM': block_num,
                'STATMENT_NUM': stmt_num,
                'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
                'TARGET_TABLE_NAME': direct_match_node,
                'SOURCE_TABLE': 'P_CZ_STG_DEV.STG_DATA_01',
                'SOURCE_DB_NAME': 'P_CZ_STG_DEV',
                'SOURCE_TABLE_NAME': 'STG_DATA_01'
            })
    
    # 用例2: 简单依赖链
    # A -> B -> C -> D，其中B在B表中，测试迭代推导
    chain_nodes = ['CHAIN_A', 'CHAIN_B', 'CHAIN_C', 'CHAIN_D']
    for i, node in enumerate(chain_nodes):
        if i < len(chain_nodes) - 1:
            data.append({
                'NODE_NAME': node,
                'FILE_ID': 20002,
                'BLOCK_NUM': 13,
                'STATMENT_NUM': i+1,
                'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
                'TARGET_TABLE_NAME': node,
                'SOURCE_TABLE': f'P_CZ_ODS_DEV.{chain_nodes[i+1]}',
                'SOURCE_DB_NAME': 'P_CZ_ODS_DEV',
                'SOURCE_TABLE_NAME': chain_nodes[i+1]
            })
    
    # 用例3: 分支依赖
    # 父节点依赖多个子节点
    parent_node = 'PARENT_NODE'
    child_nodes = ['CHILD_A', 'CHILD_B', 'CHILD_C', 'CHILD_D']
    
    for i, child in enumerate(child_nodes):
        data.append({
            'NODE_NAME': parent_node,
            'FILE_ID': 20003,
            'BLOCK_NUM': 14,
            'STATMENT_NUM': 1,
            'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
            'TARGET_TABLE_NAME': parent_node,
            'SOURCE_TABLE': f'P_CZ_STG_DEV.{child}',
            'SOURCE_DB_NAME': 'P_CZ_STG_DEV',
            'SOURCE_TABLE_NAME': child
        })
    
    # 用例4: 循环依赖
    cyclic_nodes = ['CYCLE_A', 'CYCLE_B', 'CYCLE_C']
    for i, node in enumerate(cyclic_nodes):
        next_node = cyclic_nodes[(i + 1) % len(cyclic_nodes)]
        data.append({
            'NODE_NAME': node,
            'FILE_ID': 20004,
            'BLOCK_NUM': 15,
            'STATMENT_NUM': i+1,
            'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
            'TARGET_TABLE_NAME': node,
            'SOURCE_TABLE': f'P_CZ_ODS_DEV.{next_node}',
            'SOURCE_DB_NAME': 'P_CZ_ODS_DEV',
            'SOURCE_TABLE_NAME': next_node
        })
    
    # 用例5: 复杂嵌套 - 您提到的AK/AC/AD例子
    # AK节点有AB, AC两个来源
    data.append({
        'NODE_NAME': 'AK_NODE',
        'FILE_ID': 20005,
        'BLOCK_NUM': 16,
        'STATMENT_NUM': 1,
        'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
        'TARGET_TABLE_NAME': 'AK_NODE',
        'SOURCE_TABLE': 'P_CZ_STG_DEV.AB_TABLE',
        'SOURCE_DB_NAME': 'P_CZ_STG_DEV',
        'SOURCE_TABLE_NAME': 'AB_TABLE'
    })
    
    data.append({
        'NODE_NAME': 'AK_NODE',
        'FILE_ID': 20005,
        'BLOCK_NUM': 16,
        'STATMENT_NUM': 1,
        'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
        'TARGET_TABLE_NAME': 'AK_NODE',
        'SOURCE_TABLE': 'P_CZ_STG_DEV.AC_TABLE',
        'SOURCE_DB_NAME': 'P_CZ_STG_DEV',
        'SOURCE_TABLE_NAME': 'AC_TABLE'
    })
    
    # AC节点有AD, AA两个来源
    data.append({
        'NODE_NAME': 'AC_TABLE',  # 注意：这里AC_TABLE既是AK的来源，也是一个节点
        'FILE_ID': 20005,
        'BLOCK_NUM': 17,
        'STATMENT_NUM': 1,
        'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
        'TARGET_TABLE_NAME': 'AC_TABLE',
        'SOURCE_TABLE': 'P_CZ_ODS_DEV.AD_TABLE',
        'SOURCE_DB_NAME': 'P_CZ_ODS_DEV',
        'SOURCE_TABLE_NAME': 'AD_TABLE'
    })
    
    data.append({
        'NODE_NAME': 'AC_TABLE',
        'FILE_ID': 20005,
        'BLOCK_NUM': 17,
        'STATMENT_NUM': 1,
        'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
        'TARGET_TABLE_NAME': 'AC_TABLE',
        'SOURCE_TABLE': 'P_CZ_DWS_PRD.AA_TABLE',
        'SOURCE_DB_NAME': 'P_CZ_DWS_PRD',
        'SOURCE_TABLE_NAME': 'AA_TABLE'
    })
    
    # AA节点（自引用）
    data.append({
        'NODE_NAME': 'AA_TABLE',
        'FILE_ID': 20005,
        'BLOCK_NUM': 18,
        'STATMENT_NUM': 1,
        'TARGET_DB_NAME': 'P_CZ_DWS_PRD',
        'TARGET_TABLE_NAME': 'AA_TABLE',
        'SOURCE_TABLE': 'P_CZ_DWS_PRD.AA_TABLE',
        'SOURCE_DB_NAME': 'P_CZ_DWS_PRD',
        'SOURCE_TABLE_NAME': 'AA_TABLE'
    })
    
    # 转换为DataFrame并排序
    df = pd.DataFrame(data)
    df = df.sort_values(['BLOCK_NUM', 'STATMENT_NUM', 'NODE_NAME'])
    
    print(f"生成完成: {len(df)} 行数据")
    return df

def generate_test_data_b():
    """生成测试数据B（白名单表）- 包含复杂匹配关系"""
    
    # 白名单表 - 精心设计以测试迭代推导
    
    # 1. 直接匹配的节点
    direct_nodes = [
        'P_CZ_DWS_PRD_01',  # 用例1中的节点
        'CHAIN_B',           # 依赖链中的中间节点
        'AA_TABLE',          # 自引用节点
        'AB_TABLE',          # AK节点的直接来源
        'AD_TABLE',          # AC节点的来源
    ]
    
    # 2. 一些随机表
    random_tables = [
        'CUSTOMER_INFO',
        'ORDER_DATA',
        'PRODUCT_MASTER',
        'SALES_SUMMARY',
        'PAYMENT_HISTORY',
        'USER_PROFILE',
        'INVENTORY_DETAIL',
        'LOGISTICS_REPORT',
        'MARKETING_DATA',
        'FINANCE_REPORT'
    ]
    
    # 3. 一些STG表（符合特殊规则）
    stg_tables = [
        'STG_ZSRUN_DATA_01',
        'STG_ZSAES_LOG_01',
        'STG_ZSPSA_INFO_01',
        'STG_SGLDB_MASTER',
        'STG_AESDB_DETAIL',
        'STG_PICS_REPORT',
        'STG_YLCS_SUMMARY'
    ]
    
    # 4. 一些DWS/ODS/DIM表
    dws_tables = [
        'DWS_SALES_FACT',
        'DWS_CUSTOMER_DIM',
        'ODS_ORDER_DETAIL',
        'ODS_PAYMENT_LOG',
        'DIM_PRODUCT_INFO',
        'FACT_TRANSACTION'
    ]
    
    # 合并所有表
    all_tables = direct_nodes + random_tables + stg_tables + dws_tables
    
    # 创建DataFrame
    df = pd.DataFrame({'TBL_NM': all_tables})
    
    print(f"生成白名单: {len(df)} 个表名")
    return df

def analyze_dependencies(df_a):
    """分析数据依赖关系"""
    
    print("\n=== 依赖关系分析 ===")
    
    # 分析节点和来源表的关系
    node_to_sources = defaultdict(set)
    source_to_nodes = defaultdict(set)
    
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        
        node_to_sources[node].add(source)
        if source in df_a['NODE_NAME'].values:
            source_to_nodes[source].add(node)
    
    # 找出可以作为节点的来源表
    shared_tables = set(node_to_sources.keys()).intersection(
        set([s for sources in node_to_sources.values() for s in sources])
    )
    
    print(f"可以作为节点的来源表数量: {len(shared_tables)}")
    print("部分示例:")
    for i, table in enumerate(list(shared_tables)[:10], 1):
        print(f"  {i:2d}. {table}")
        print(f"     作为节点时的来源表: {list(node_to_sources.get(table, []))[:3]}")
        print(f"     作为来源时的节点: {list(source_to_nodes.get(table, []))[:3]}")
    
    # 分析特定测试用例
    print("\n=== 特定测试用例分析 ===")
    
    # AK/AC/AD用例分析
    print("1. AK/AC/AD用例:")
    ak_sources = node_to_sources.get('AK_NODE', set())
    ac_sources = node_to_sources.get('AC_TABLE', set())
    aa_sources = node_to_sources.get('AA_TABLE', set())
    
    print(f"   AK_NODE 的来源表: {list(ak_sources)}")
    print(f"   AC_TABLE 的来源表: {list(ac_sources)}")
    print(f"   AA_TABLE 的来源表: {list(aa_sources)}")
    
    # 依赖链分析
    print("\n2. 依赖链用例:")
    chain_relations = []
    for node in ['CHAIN_A', 'CHAIN_B', 'CHAIN_C', 'CHAIN_D']:
        if node in node_to_sources:
            chain_relations.append(f"{node} -> {list(node_to_sources[node])}")
    
    for relation in chain_relations:
        print(f"   {relation}")
    
    return node_to_sources, source_to_nodes

def create_test_scenarios(df_a, df_b):
    """创建测试场景说明"""
    
    scenarios = []
    
    scenarios.append("=== 测试场景说明 ===")
    scenarios.append("")
    
    # 场景1: 直接匹配
    scenarios.append("场景1: 直接匹配")
    scenarios.append("  - P_CZ_DWS_PRD_01 在B表中")
    scenarios.append("  - 预期: 所有NODE_NAME为P_CZ_DWS_PRD_01的行标记为1")
    scenarios.append("")
    
    # 场景2: 简单迭代
    scenarios.append("场景2: 简单迭代推导")
    scenarios.append("  - AA_TABLE 在B表中")
    scenarios.append("  - AA_TABLE 自引用 (SOURCE_TABLE_NAME = AA_TABLE)")
    scenarios.append("  - 预期: AA_TABLE标记为1 (自引用直接匹配)")
    scenarios.append("")
    
    # 场景3: 复杂迭代 (AK/AC/AD用例)
    scenarios.append("场景3: 复杂迭代推导 (AK/AC/AD用例)")
    scenarios.append("  B表中包含: AA_TABLE, AB_TABLE, AD_TABLE")
    scenarios.append("  B表中不包含: AC_TABLE, AK_NODE")
    scenarios.append("  初始标记:")
    scenarios.append("    - AA_TABLE: 1 (直接匹配)")
    scenarios.append("    - AB_TABLE: 1 (直接匹配)")
    scenarios.append("    - AD_TABLE: 1 (直接匹配)")
    scenarios.append("    - AC_TABLE: 空 (不在B表中)")
    scenarios.append("    - AK_NODE: 空 (不在B表中)")
    scenarios.append("  第一次迭代:")
    scenarios.append("    - AC_TABLE: 检查其来源表(AD_TABLE, AA_TABLE)")
    scenarios.append("    - 两个来源表都为1 → AC_TABLE标记为1")
    scenarios.append("  第二次迭代:")
    scenarios.append("    - AK_NODE: 检查其来源表(AB_TABLE, AC_TABLE)")
    scenarios.append("    - 两个来源表都为1 → AK_NODE标记为1")
    scenarios.append("")
    
    # 场景4: 依赖链
    scenarios.append("场景4: 依赖链推导")
    scenarios.append("  B表中包含: CHAIN_B")
    scenarios.append("  依赖链: CHAIN_A -> CHAIN_B -> CHAIN_C -> CHAIN_D")
    scenarios.append("  预期: CHAIN_B标记为1 (直接匹配)")
    scenarios.append("       其他节点保持为空 (依赖链不完整)")
    scenarios.append("")
    
    # 场景5: 分支依赖
    scenarios.append("场景5: 分支依赖")
    scenarios.append("  PARENT_NODE 依赖 CHILD_A, CHILD_B, CHILD_C, CHILD_D")
    scenarios.append("  如果所有子节点都不在B表中，PARENT_NODE保持为空")
    scenarios.append("")
    
    # 场景6: 循环依赖
    scenarios.append("场景6: 循环依赖")
    scenarios.append("  CYCLE_A -> CYCLE_B -> CYCLE_C -> CYCLE_A")
    scenarios.append("  如果没有节点在B表中，所有节点保持为空")
    scenarios.append("  迭代最多进行20轮，防止无限循环")
    scenarios.append("")
    
    return '\n'.join(scenarios)

def main():
    print("正在生成复杂测试数据...")
    
    # 生成数据
    df_a = generate_test_data_a()
    df_b = generate_test_data_b()
    
    # 保存到Excel文件
    output_file_a = "复杂测试数据_A.xlsx"
    output_file_b = "复杂测试数据_B.xlsx"
    
    df_a.to_excel(output_file_a, index=False)
    df_b.to_excel(output_file_b, index=False)
    
    print(f"\nA表数据已保存到: {output_file_a}")
    print(f"  - 数据量: {len(df_a)} 行")
    print(f"  - BLOCK_NUM: {df_a['BLOCK_NUM'].nunique()} 个 (1-{df_a['BLOCK_NUM'].max()})")
    print(f"  - 唯一NODE_NAME: {df_a['NODE_NAME'].nunique()} 个")
    print(f"  - 唯一SOURCE_TABLE_NAME: {df_a['SOURCE_TABLE_NAME'].nunique()} 个")
    
    print(f"\nB表数据已保存到: {output_file_b}")
    print(f"  - 数据量: {len(df_b)} 行")
    print(f"  - 唯一表名: {df_b['TBL_NM'].nunique()} 个")
    
    # 分析依赖关系
    node_to_sources, source_to_nodes = analyze_dependencies(df_a)
    
    # 创建测试场景说明
    scenarios_text = create_test_scenarios(df_a, df_b)
    
    scenarios_file = "测试场景说明.txt"
    with open(scenarios_file, 'w', encoding='utf-8') as f:
        f.write(scenarios_text)
    
    print(f"\n测试场景说明已保存到: {scenarios_file}")
    
    # 显示关键数据示例
    print("\n=== 关键数据示例 ===")
    
    print("\n1. A表前10行:")
    print(df_a.head(10).to_string())
    
    print("\n2. 特定测试用例数据:")
    
    # AK/AC/AD用例
    print("\nAK/AC/AD用例数据:")
    ak_ac_cases = df_a[df_a['NODE_NAME'].isin(['AK_NODE', 'AC_TABLE', 'AA_TABLE'])]
    print(ak_ac_cases.to_string())
    
    # 依赖链用例
    print("\n依赖链用例数据:")
    chain_cases = df_a[df_a['NODE_NAME'].isin(['CHAIN_A', 'CHAIN_B', 'CHAIN_C', 'CHAIN_D'])]
    print(chain_cases.to_string())
    
    print("\n3. B表示例 (前20个):")
    print(df_b.head(20).to_string())
    
    # 统计信息
    print("\n=== 统计信息 ===")
    
    # A表和B表的交集
    a_tables = set(df_a['SOURCE_TABLE_NAME'].unique())
    b_tables = set(df_b['TBL_NM'].unique())
    common_tables = a_tables.intersection(b_tables)
    
    print(f"A表和B表共有的表名: {len(common_tables)} 个")
    print("部分共有表名:")
    for i, table in enumerate(list(common_tables)[:15], 1):
        print(f"  {i:2d}. {table}")
    
    # NODE_NAME在B表中的情况
    nodes_in_b = set(df_a['NODE_NAME'].unique()).intersection(b_tables)
    print(f"\n在B表中出现的NODE_NAME: {len(nodes_in_b)} 个")
    for node in nodes_in_b:
        print(f"  - {node}")

if __name__ == "__main__":
    main()