import pandas as pd
import numpy as np
from collections import defaultdict, deque
import warnings
warnings.filterwarnings('ignore')

def initial_marking(df_a, df_b):
    """
    初始标记阶段：基于表B的白名单进行直接匹配
    """
    print("=" * 60)
    print("阶段一：初始标记阶段")
    print("=" * 60)
    
    # 获取B表中的所有表名
    tbl_nm_list = df_b['TBL_NM'].dropna().astype(str).unique().tolist()
    print(f"白名单表B中共有 {len(tbl_nm_list)} 个表名")
    
    # 初始化标记列
    df_a['SOURCE_TABLE_NAME_FLAG'] = ''  # 来源表标记
    df_a['STATMENT_NUM_FLAG'] = ''       # 代码块标记
    df_a['NODE_NAME_FLAG'] = ''          # 节点标记
    
    # 1. NODE_NAME匹配：若NODE_NAME存在于表B中，对应的所有行直接标记为1
    node_names_in_b = df_a['NODE_NAME'][df_a['NODE_NAME'].astype(str).isin(tbl_nm_list)].unique()
    print(f"\n1. NODE_NAME匹配:")
    print(f"   在表B中找到的NODE_NAME: {len(node_names_in_b)} 个")
    
    for node_name in node_names_in_b:
        mask = df_a['NODE_NAME'] == node_name
        df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'] = 1
        df_a.loc[mask, 'STATMENT_NUM_FLAG'] = 1
        df_a.loc[mask, 'NODE_NAME_FLAG'] = 1
        print(f"   - {node_name}: 标记 {mask.sum()} 行")
    
    # 2. SOURCE_TABLE_NAME匹配：若SOURCE_TABLE_NAME存在于表B中，该行标记为1
    source_mask = df_a['SOURCE_TABLE_NAME'].astype(str).isin(tbl_nm_list)
    source_rows_to_mark = source_mask & (df_a['SOURCE_TABLE_NAME_FLAG'] == '')
    sources_in_b = df_a.loc[source_rows_to_mark, 'SOURCE_TABLE_NAME'].unique()
    
    print(f"\n2. SOURCE_TABLE_NAME匹配:")
    print(f"   在表B中找到的SOURCE_TABLE_NAME: {len(sources_in_b)} 个")
    
    if len(sources_in_b) > 0:
        df_a.loc[source_rows_to_mark, 'SOURCE_TABLE_NAME_FLAG'] = 1
        print(f"   标记了 {source_rows_to_mark.sum()} 行")
    
    # 统计初始标记结果
    initial_stats(df_a)
    
    return df_a

def initial_stats(df_a):
    """初始标记统计"""
    total_rows = len(df_a)
    source_marked = (df_a['SOURCE_TABLE_NAME_FLAG'] != '').sum()
    stmt_marked = (df_a['STATMENT_NUM_FLAG'] != '').sum()
    node_marked = (df_a['NODE_NAME_FLAG'] != '').sum()
    
    print(f"\n初始标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME已标记: {source_marked} ({source_marked/total_rows*100:.1f}%)")
    print(f"  STATMENT_NUM已标记: {stmt_marked} ({stmt_marked/total_rows*100:.1f}%)")
    print(f"  NODE_NAME已标记: {node_marked} ({node_marked/total_rows*100:.1f}%)")

def build_strict_dependency_graph(df_a):
    """
    构建严格的依赖关系图
    """
    print("\n" + "=" * 60)
    print("构建严格的依赖关系图")
    print("=" * 60)
    
    # 存储结构
    dependency_graph = {
        'node_source_relations': defaultdict(lambda: defaultdict(set)),  # 节点 -> STATEMENT -> 来源表
        'source_node_relations': defaultdict(lambda: defaultdict(set)),  # 来源表 -> BLOCK -> (节点, STATEMENT)
        'stmt_info': defaultdict(dict),                                 # (BLOCK, STATEMENT) -> 信息
        'node_info': defaultdict(dict),                                 # 节点 -> 信息
        'source_info': defaultdict(dict),                               # 来源表 -> 信息
    }
    
    # 第一遍：收集基本信息
    for idx, row in df_a.iterrows():
        block_num = row['BLOCK_NUM']
        stmt_num = row['STATMENT_NUM']
        node_name = str(row['NODE_NAME'])
        source_name = str(row['SOURCE_TABLE_NAME'])
        
        # 节点 -> STATEMENT -> 来源表 关系
        dependency_graph['node_source_relations'][node_name][(block_num, stmt_num)].add(source_name)
        
        # 来源表 -> BLOCK -> (节点, STATEMENT) 关系
        dependency_graph['source_node_relations'][source_name][block_num].add((node_name, stmt_num))
        
        # STATEMENT信息
        stmt_key = (block_num, stmt_num)
        if stmt_key not in dependency_graph['stmt_info']:
            dependency_graph['stmt_info'][stmt_key] = {
                'nodes': set(),
                'sources': set(),
                'rows': []
            }
        dependency_graph['stmt_info'][stmt_key]['nodes'].add(node_name)
        dependency_graph['stmt_info'][stmt_key]['sources'].add(source_name)
        dependency_graph['stmt_info'][stmt_key]['rows'].append(idx)
        
        # 节点信息
        if node_name not in dependency_graph['node_info']:
            dependency_graph['node_info'][node_name] = {
                'blocks': set(),
                'stmts': set(),
                'sources': set(),
                'rows': []
            }
        dependency_graph['node_info'][node_name]['blocks'].add(block_num)
        dependency_graph['node_info'][node_name]['stmts'].add((block_num, stmt_num))
        dependency_graph['node_info'][node_name]['sources'].add(source_name)
        dependency_graph['node_info'][node_name]['rows'].append(idx)
        
        # 来源表信息
        if source_name not in dependency_graph['source_info']:
            dependency_graph['source_info'][source_name] = {
                'as_source_rows': [],
                'as_node_rows': [],
                'is_node': False
            }
        dependency_graph['source_info'][source_name]['as_source_rows'].append(idx)
    
    # 第二遍：标记哪些表也是节点
    for node_name in dependency_graph['node_info']:
        if node_name in dependency_graph['source_info']:
            dependency_graph['source_info'][node_name]['is_node'] = True
            # 找到该表作为节点的行
            node_rows = []
            for idx in dependency_graph['node_info'][node_name]['rows']:
                if df_a.at[idx, 'NODE_NAME'] == node_name:
                    node_rows.append(idx)
            dependency_graph['source_info'][node_name]['as_node_rows'] = node_rows
    
    print(f"依赖图统计:")
    print(f"  - 节点数量: {len(dependency_graph['node_info'])}")
    print(f"  - 来源表数量: {len(dependency_graph['source_info'])}")
    print(f"  - STATEMENT数量: {len(dependency_graph['stmt_info'])}")
    
    return dependency_graph

def strict_iterative_marking(df_a, df_b, dependency_graph):
    """
    严格的迭代标记：如果任何来源表为空，相关STATEMENT和节点都标记为空
    """
    print("\n" + "=" * 60)
    print("阶段二：严格的迭代标记")
    print("=" * 60)
    
    # 获取白名单
    tbl_nm_set = set(df_b['TBL_NM'].dropna().astype(str).unique().tolist())
    
    # 第一步：标记叶子节点（不是节点的表）
    print(f"\n1. 标记叶子节点...")
    
    for source_name, source_info in dependency_graph['source_info'].items():
        if not source_info['is_node']:  # 不是节点
            if source_name in tbl_nm_set:  # 在白名单中
                for row_idx in source_info['as_source_rows']:
                    if df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] != 1:
                        df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = 1
            else:  # 不在白名单中
                for row_idx in source_info['as_source_rows']:
                    if df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] != '':
                        df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = ''
    
    # 第二步：迭代标记节点
    print(f"\n2. 迭代标记节点...")
    
    max_iterations = 20
    for iteration in range(1, max_iterations + 1):
        print(f"\n  迭代 {iteration}:")
        
        changed_count = 0
        
        # 处理每个节点
        for node_name, node_info in dependency_graph['node_info'].items():
            # 检查节点是否已经标记
            node_rows = node_info['rows']
            if node_rows and df_a.at[node_rows[0], 'NODE_NAME_FLAG'] == 1:
                continue
            
            # 检查该节点在所有STATEMENT中的来源表状态
            all_stmts_valid = True
            stmts_with_empty = []
            
            for (block_num, stmt_num) in node_info['stmts']:
                # 获取该节点在该STATEMENT中的所有来源表
                sources = dependency_graph['node_source_relations'][node_name].get((block_num, stmt_num), set())
                
                # 检查所有来源表的标记
                stmt_valid = True
                for source in sources:
                    # 查找来源表的标记
                    source_rows = dependency_graph['source_info'][source]['as_source_rows']
                    if source_rows:
                        source_mark = df_a.at[source_rows[0], 'SOURCE_TABLE_NAME_FLAG']
                        if source_mark != 1:
                            stmt_valid = False
                            if source_mark == '':
                                stmts_with_empty.append((block_num, stmt_num))
                            break
                
                if not stmt_valid:
                    all_stmts_valid = False
            
            if all_stmts_valid:
                # 所有STATEMENT的所有来源表都标记为1，节点标记为1
                for row_idx in node_rows:
                    if df_a.at[row_idx, 'NODE_NAME_FLAG'] != 1:
                        df_a.at[row_idx, 'NODE_NAME_FLAG'] = 1
                        df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = 1
                        changed_count += 1
                
                if changed_count > 0:
                    print(f"    节点 {node_name}: 标记为1 (所有来源表都标记为1)")
            elif stmts_with_empty:
                # 有STATEMENT包含空的来源表，节点标记为空
                for row_idx in node_rows:
                    if df_a.at[row_idx, 'NODE_NAME_FLAG'] != '':
                        df_a.at[row_idx, 'NODE_NAME_FLAG'] = ''
                        df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = ''
                        changed_count += 1
                
                if changed_count > 0:
                    stmts_str = ', '.join([f"BLOCK_{b}_STATEMENT_{s}" for b, s in stmts_with_empty[:3]])
                    if len(stmts_with_empty) > 3:
                        stmts_str += f" ... 等{len(stmts_with_empty)}个"
                    print(f"    节点 {node_name}: 标记为空 (STATEMENT中有空标记: {stmts_str})")
        
        print(f"    本轮标记了 {changed_count} 行")
        
        if changed_count == 0:
            print(f"    没有新的标记产生，迭代结束")
            break
    
    # 第三步：更新STATEMENT标记（严格的空标记传播）
    print(f"\n3. 严格的STATEMENT标记更新...")
    
    stmt_updates = 0
    for (block_num, stmt_num), stmt_info in dependency_graph['stmt_info'].items():
        stmt_rows = stmt_info['rows']
        
        if not stmt_rows:
            continue
        
        # 检查该STATEMENT的所有来源表标记
        has_empty = False
        all_ones = True
        
        for row_idx in stmt_rows:
            source_mark = df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG']
            if source_mark == '':
                has_empty = True
                break
            elif source_mark != 1:
                all_ones = False
        
        # 更新STATEMENT标记
        if has_empty:
            # 有空的来源表，STATEMENT标记为空
            for row_idx in stmt_rows:
                if df_a.at[row_idx, 'STATMENT_NUM_FLAG'] != '':
                    df_a.at[row_idx, 'STATMENT_NUM_FLAG'] = ''
                    stmt_updates += 1
        elif all_ones:
            # 所有来源表都标记为1，STATEMENT标记为1
            for row_idx in stmt_rows:
                if df_a.at[row_idx, 'STATMENT_NUM_FLAG'] != 1:
                    df_a.at[row_idx, 'STATMENT_NUM_FLAG'] = 1
                    stmt_updates += 1
    
    print(f"    更新了 {stmt_updates} 个STATEMENT标记")
    
    # 第四步：再次检查节点标记（基于更新后的STATEMENT标记）
    print(f"\n4. 最终节点标记检查...")
    
    node_updates = 0
    for node_name, node_info in dependency_graph['node_info'].items():
        node_rows = node_info['rows']
        
        if not node_rows:
            continue
        
        # 检查节点是否有任何STATEMENT标记为空
        has_empty_stmt = False
        for (block_num, stmt_num) in node_info['stmts']:
            # 找到该STATEMENT的任意一行
            stmt_rows = dependency_graph['stmt_info'][(block_num, stmt_num)]['rows']
            if stmt_rows:
                stmt_mark = df_a.at[stmt_rows[0], 'STATMENT_NUM_FLAG']
                if stmt_mark == '':
                    has_empty_stmt = True
                    break
        
        # 如果有空的STATEMENT，节点标记为空
        if has_empty_stmt:
            for row_idx in node_rows:
                if df_a.at[row_idx, 'NODE_NAME_FLAG'] != '':
                    df_a.at[row_idx, 'NODE_NAME_FLAG'] = ''
                    df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = ''
                    node_updates += 1
    
    print(f"    更新了 {node_updates} 个节点标记")
    
    iterative_stats(df_a)
    return df_a

def iterative_stats(df_a):
    """迭代标记统计"""
    total_rows = len(df_a)
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    source_empty = (df_a['SOURCE_TABLE_NAME_FLAG'] == '').sum()
    stmt_1_count = (df_a['STATMENT_NUM_FLAG'] == 1).sum()
    stmt_empty = (df_a['STATMENT_NUM_FLAG'] == '').sum()
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    node_empty = (df_a['NODE_NAME_FLAG'] == '').sum()
    
    print(f"\n迭代标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME标记为1: {source_1_count} ({source_1_count/total_rows*100:.1f}%)")
    print(f"  SOURCE_TABLE_NAME未标记: {source_empty} ({source_empty/total_rows*100:.1f}%)")
    print(f"  STATMENT_NUM标记为1: {stmt_1_count} ({stmt_1_count/total_rows*100:.1f}%)")
    print(f"  STATMENT_NUM未标记: {stmt_empty} ({stmt_empty/total_rows*100:.1f}%)")
    print(f"  NODE_NAME标记为1: {node_1_count} ({node_1_count/total_rows*100:.1f}%)")
    print(f"  NODE_NAME未标记: {node_empty} ({node_empty/total_rows*100:.1f}%)")

def final_consistency_check(df_a, dependency_graph):
    """
    最终一致性检查：确保标记的一致性
    """
    print("\n" + "=" * 60)
    print("阶段三：最终一致性检查")
    print("=" * 60)
    
    updates = 0
    
    # 1. 确保节点标记的一致性
    print(f"\n1. 检查节点标记一致性...")
    for node_name, node_info in dependency_graph['node_info'].items():
        node_rows = node_info['rows']
        
        if not node_rows:
            continue
        
        # 获取节点的所有标记
        node_marks = set(df_a.loc[node_rows, 'NODE_NAME_FLAG'].unique())
        
        # 如果节点在任何地方标记为空，全部置为空
        if '' in node_marks:
            for row_idx in node_rows:
                if df_a.at[row_idx, 'NODE_NAME_FLAG'] != '':
                    df_a.at[row_idx, 'NODE_NAME_FLAG'] = ''
                    df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = ''
                    updates += 1
    
    # 2. 确保STATEMENT标记的一致性
    print(f"\n2. 检查STATEMENT标记一致性...")
    for (block_num, stmt_num), stmt_info in dependency_graph['stmt_info'].items():
        stmt_rows = stmt_info['rows']
        
        if not stmt_rows:
            continue
        
        # 检查是否有空的SOURCE_TABLE_NAME标记
        has_empty_source = any(df_a.at[idx, 'SOURCE_TABLE_NAME_FLAG'] == '' for idx in stmt_rows)
        
        if has_empty_source:
            # 有空的来源表，STATEMENT标记为空
            for row_idx in stmt_rows:
                if df_a.at[row_idx, 'STATMENT_NUM_FLAG'] != '':
                    df_a.at[row_idx, 'STATMENT_NUM_FLAG'] = ''
                    updates += 1
    
    # 3. 传播空标记到相关节点
    print(f"\n3. 传播空标记到相关节点...")
    
    # 找到所有标记为空的来源表
    empty_sources = set()
    for source_name, source_info in dependency_graph['source_info'].items():
        source_rows = source_info['as_source_rows']
        if source_rows and df_a.at[source_rows[0], 'SOURCE_TABLE_NAME_FLAG'] == '':
            empty_sources.add(source_name)
    
    # 对于每个空的来源表，找到使用它的所有节点
    for source_name in empty_sources:
        if source_name in dependency_graph['source_node_relations']:
            for block_num, node_stmt_set in dependency_graph['source_node_relations'][source_name].items():
                for node_name, stmt_num in node_stmt_set:
                    # 找到该节点的所有行
                    node_rows = dependency_graph['node_info'][node_name]['rows']
                    for row_idx in node_rows:
                        if df_a.at[row_idx, 'NODE_NAME_FLAG'] != '':
                            df_a.at[row_idx, 'NODE_NAME_FLAG'] = ''
                            df_a.at[row_idx, 'SOURCE_TABLE_NAME_FLAG'] = ''
                            updates += 1
    
    print(f"    进行了 {updates} 次一致性更新")
    
    final_stats(df_a)
    return df_a

def final_stats(df_a):
    """最终标记统计"""
    total_rows = len(df_a)
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    source_empty = (df_a['SOURCE_TABLE_NAME_FLAG'] == '').sum()
    stmt_1_count = (df_a['STATMENT_NUM_FLAG'] == 1).sum()
    stmt_empty = (df_a['STATMENT_NUM_FLAG'] == '').sum()
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    node_empty = (df_a['NODE_NAME_FLAG'] == '').sum()
    
    print(f"\n最终标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME标记为1: {source_1_count} ({source_1_count/total_rows*100:.1f}%)")
    print(f"  SOURCE_TABLE_NAME未标记: {source_empty} ({source_empty/total_rows*100:.1f}%)")
    print(f"  STATMENT_NUM标记为1: {stmt_1_count} ({stmt_1_count/total_rows*100:.1f}%)")
    print(f"  STATMENT_NUM未标记: {stmt_empty} ({stmt_empty/total_rows*100:.1f}%)")
    print(f"  NODE_NAME标记为1: {node_1_count} ({node_1_count/total_rows*100:.1f}%)")
    print(f"  NODE_NAME未标记: {node_empty} ({node_empty/total_rows*100:.1f}%)")

def analyze_your_example():
    """
    分析您提供的具体例子
    """
    print("\n" + "=" * 60)
    print("分析具体例子")
    print("=" * 60)
    
    # 创建测试数据
    test_data = [
        # BLOCK 1: DWS_D04_FIN_ORG_INFO_P 节点
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 1, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO',
         'SOURCE_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO'},
        
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 2, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO_TMP',
         'SOURCE_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO_TMP'},
        
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 2, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO',
         'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZRZZ'},
        
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 2, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO',
         'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZRBB'},
        
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 3, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO',
         'SOURCE_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO'},
        
        {'BLOCK_NUM': 1, 'STATMENT_NUM': 4, 'FILE_ID': 1000,
         'NODE_NAME': 'DWS_D04_FIN_ORG_INFO_P', 'TARGET_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO',
         'SOURCE_TABLE_NAME': 'DWS_D04_FIN_ORG_INFO_TMP'},
        
        # BLOCK 2: STG_ZSRUN_ZZRBB 节点
        {'BLOCK_NUM': 2, 'STATMENT_NUM': 1, 'FILE_ID': 1001,
         'NODE_NAME': 'STG_ZSRUN_ZZRBB', 'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZRBB'},
        
        {'BLOCK_NUM': 2, 'STATMENT_NUM': 2, 'FILE_ID': 1001,
         'NODE_NAME': 'STG_ZSRUN_ZZRBB', 'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZRBB'},
        
        {'BLOCK_NUM': 2, 'STATMENT_NUM': 2, 'FILE_ID': 1001,
         'NODE_NAME': 'STG_ZSRUN_ZZRBB', 'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZTEST'},
        
        # BLOCK 3: STG_ZSRUN_ZZTEST 节点
        {'BLOCK_NUM': 3, 'STATMENT_NUM': 1, 'FILE_ID': 1002,
         'NODE_NAME': 'STG_ZSRUN_ZZTEST', 'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZTEST_1'},
        
        # BLOCK 4: STG_ZSRUN_ZZTEST_1 节点
        {'BLOCK_NUM': 4, 'STATMENT_NUM': 1, 'FILE_ID': 1003,
         'NODE_NAME': 'STG_ZSRUN_ZZTEST_1', 'SOURCE_TABLE_NAME': 'STG_ZSRUN_ZZTEST_2'},
    ]
    
    test_df = pd.DataFrame(test_data)
    
    # B表白名单（假设）
    b_data = {'TBL_NM': ['DWS_D04_FIN_ORG_INFO', 'DWS_D04_FIN_ORG_INFO_TMP']}
    test_b = pd.DataFrame(b_data)
    
    print("测试数据:")
    print(test_df[['BLOCK_NUM', 'STATMENT_NUM', 'NODE_NAME', 'SOURCE_TABLE_NAME']].to_string(index=False))
    print("\n白名单表B:")
    print(test_b['TBL_NM'].to_string(index=False))
    
    print("\n分析:")
    print("  1. 初始标记:")
    print("     - DWS_D04_FIN_ORG_INFO: 在B表中 -> 标记为1")
    print("     - DWS_D04_FIN_ORG_INFO_TMP: 在B表中 -> 标记为1")
    print("     - STG_ZSRUN_ZZRZZ: 不在B表中 -> 标记为空")
    print("     - STG_ZSRUN_ZZRBB: 不在B表中 -> 标记为空")
    print("     - STG_ZSRUN_ZZTEST: 不在B表中 -> 标记为空")
    print("     - STG_ZSRUN_ZZTEST_1: 不在B表中 -> 标记为空")
    print("     - STG_ZSRUN_ZZTEST_2: 不在B表中 -> 标记为空")
    
    print("\n  2. 严格标记逻辑:")
    print("     - DWS_D04_FIN_ORG_INFO_P节点:")
    print("       * STATEMENT 1: DWS_D04_FIN_ORG_INFO=1 -> STATEMENT标记为1")
    print("       * STATEMENT 2: STG_ZSRUN_ZZRZZ为空 -> STATEMENT标记为空 -> 节点标记为空")
    print("       * STATEMENT 3: DWS_D04_FIN_ORG_INFO=1 -> STATEMENT标记为1")
    print("       * STATEMENT 4: DWS_D04_FIN_ORG_INFO_TMP=1 -> STATEMENT标记为1")
    print("       由于STATEMENT 2标记为空，整个节点标记为空")
    
    print("\n  3. 下游节点:")
    print("     - STG_ZSRUN_ZZRBB节点:")
    print("       * STATEMENT 1: STG_ZSRUN_ZZRBB为空 -> STATEMENT标记为空 -> 节点标记为空")
    print("       * STATEMENT 2: STG_ZSRUN_ZZTEST为空 -> STATEMENT标记为空 -> 节点标记为空")
    print("     - STG_ZSRUN_ZZTEST节点:")
    print("       * STATEMENT 1: STG_ZSRUN_ZZTEST_1为空 -> STATEMENT标记为空 -> 节点标记为空")
    print("     - STG_ZSRUN_ZZTEST_1节点:")
    print("       * STATEMENT 1: STG_ZSRUN_ZZTEST_2为空 -> STATEMENT标记为空 -> 节点标记为空")
    
    print("\n  期望最终结果: 所有节点标记都为空")
    
    # 执行标记
    print("\n执行标记流程...")
    result_df = test_df.copy()
    result_df = initial_marking(result_df, test_b)
    
    # 构建依赖图
    dependency_graph = build_strict_dependency_graph(result_df)
    
    # 迭代标记
    result_df = strict_iterative_marking(result_df, test_b, dependency_graph)
    result_df = final_consistency_check(result_df, dependency_graph)
    
    print("\n最终标记结果:")
    print(result_df[['BLOCK_NUM', 'STATMENT_NUM', 'NODE_NAME', 'SOURCE_TABLE_NAME', 
                    'SOURCE_TABLE_NAME_FLAG', 'STATMENT_NUM_FLAG', 'NODE_NAME_FLAG']].to_string(index=False))

def main():
    # 读取Excel文件
    print("数据标记处理系统 (严格空标记传播版)")
    print("=" * 60)
    
    # 分析具体例子
    analyze_your_example()
    
    print("\n" + "=" * 60)
    print("开始处理主数据")
    print("=" * 60)
    
    # 请根据实际文件路径修改
    file_a = "复杂测试数据_A.xlsx"  # 替换为您的A表文件路径
    file_b = "复杂测试数据_B.xlsx"  # 替换为您的B表文件路径
    
    try:
        # 读取数据
        df_a = pd.read_excel(file_a)
        df_b = pd.read_excel(file_b)
        
        print(f"读取数据成功:")
        print(f"  A表: {df_a.shape[0]} 行, {df_a.shape[1]} 列")
        print(f"  B表: {df_b.shape[0]} 行, {df_b.shape[1]} 列")
        
        # 确保列名正确
        required_columns = ['NODE_NAME', 'FILE_ID', 'BLOCK_NUM', 'STATMENT_NUM', 
                           'TARGET_DB_NAME', 'TARGET_TABLE_NAME', 'SOURCE_TABLE', 
                           'SOURCE_DB_NAME', 'SOURCE_TABLE_NAME']
        
        missing_cols = [col for col in required_columns if col not in df_a.columns]
        if missing_cols:
            print(f"错误: A表中缺少必要的列: {missing_cols}")
            return
        
        if 'TBL_NM' not in df_b.columns:
            print("错误: B表中缺少必要的列: TBL_NM")
            return
        
        print("\n开始执行标记流程...")
        
        # 阶段一：初始标记
        df_a = initial_marking(df_a, df_b)
        
        # 构建严格的依赖图
        dependency_graph = build_strict_dependency_graph(df_a)
        
        # 阶段二：严格的迭代标记
        df_a = strict_iterative_marking(df_a, df_b, dependency_graph)
        
        # 阶段三：最终一致性检查
        df_a = final_consistency_check(df_a, dependency_graph)
        
        # 保存结果
        output_file = "标记结果_严格空标记.xlsx"
        df_a.to_excel(output_file, index=False)
        print(f"\n标记结果已保存到: {output_file}")
        
        # 显示示例数据
        print(f"\n示例数据 (前20行):")
        example_cols = ['BLOCK_NUM', 'STATMENT_NUM', 'NODE_NAME', 'SOURCE_TABLE_NAME', 
                       'SOURCE_TABLE_NAME_FLAG', 'STATMENT_NUM_FLAG', 'NODE_NAME_FLAG']
        print(df_a[example_cols].head(20).to_string(index=False))
        
    except FileNotFoundError as e:
        print(f"文件读取错误: {e}")
        print("请确保文件路径正确，并且文件存在于指定位置")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()