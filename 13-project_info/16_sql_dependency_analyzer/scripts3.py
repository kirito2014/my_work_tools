import pandas as pd
import numpy as np
from collections import defaultdict, deque

def load_and_validate_data(file_a_path, file_b_path):
    """加载并验证表A、表B数据"""
    df_a = pd.read_excel(file_a_path)
    df_b = pd.read_excel(file_b_path)
    
    # 清理列名空格
    df_a.columns = df_a.columns.str.strip()
    df_b.columns = df_b.columns.str.strip()
    
    # 验证必要列
    required_a = ['NODE_NAME', 'FILE_ID', 'BLOCK_NUM', 'STATMENT_NUM', 
                  'SOURCE_TABLE_NAME', 'TARGET_DB_NAME', 'TARGET_TABLE_NAME']
    required_b = ['TBL_NM']
    
    for col in required_a:
        if col not in df_a.columns:
            raise ValueError(f"表A缺失必要列：{col}")
    if 'TBL_NM' not in df_b.columns:
        raise ValueError("表B缺失必要列：TBL_NM")
    
    # 处理空值
    df_a['SOURCE_TABLE_NAME'] = df_a['SOURCE_TABLE_NAME'].fillna('UNDEFINED_SOURCE')
    df_a['NODE_NAME'] = df_a['NODE_NAME'].fillna('UNDEFINED_NODE')
    df_b['TBL_NM'] = df_b['TBL_NM'].fillna('')
    
    # 生成白名单集合
    whitelist = set(df_b[df_b['TBL_NM'] != '']['TBL_NM'].unique())
    
    print(f"数据加载完成：表A{len(df_a)}行，表B白名单{len(whitelist)}个")
    return df_a, whitelist

def init_marking(df_a, whitelist):
    """初始化标记列"""
    df_a['SOURCE_FINAL'] = np.nan
    df_a['STATEMENT_FINAL'] = np.nan
    df_a['NODE_FINAL'] = np.nan
    df_a['IS_TERMINAL'] = False
    df_a['BLOCK_REASON'] = ''
    
    # 初始标记：SOURCE_TABLE_NAME在白名单中标记为1
    source_match_mask = df_a['SOURCE_TABLE_NAME'].isin(whitelist)
    df_a.loc[source_match_mask, 'SOURCE_FINAL'] = 1
    df_a.loc[source_match_mask, 'BLOCK_REASON'] = '白名单匹配'
    
    print(f"初始标记完成：{source_match_mask.sum()}行原表标记为1")
    return df_a

def build_hierarchy(df_a):
    """构建层级关系：包括向上传播所需的关系"""
    source_to_children = defaultdict(set)  # source的子source
    node_to_sources = defaultdict(set)     # node下的所有source
    source_to_nodes = defaultdict(set)     # source所属的node
    node_to_parents = defaultdict(set)     # node的父节点（即该node作为source时所在的node_name）
    node_dependency_graph = defaultdict(set)  # 节点依赖图（用于向上传播）
    
    # 1. 构建node与source的关联
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        node_to_sources[node].add(source)
        source_to_nodes[source].add(node)
    
    # 2. 构建source的子节点关系
    all_nodes = df_a['NODE_NAME'].unique()
    for source in df_a['SOURCE_TABLE_NAME'].unique():
        if source in all_nodes:
            # 该source作为node时的所有子source
            child_sources = df_a[df_a['NODE_NAME'] == source]['SOURCE_TABLE_NAME'].unique()
            for child in child_sources:
                source_to_children[source].add(child)
    
    # 3. 构建node的父节点关系（用于向上传播）
    for node in df_a['NODE_NAME'].unique():
        # 找出所有将该node作为source的父节点
        parent_nodes = df_a[df_a['SOURCE_TABLE_NAME'] == node]['NODE_NAME'].unique()
        node_to_parents[node] = set(parent_nodes)
    
    # 4. 构建节点依赖图（用于向上传播时找到所有相关节点）
    print("构建节点依赖图...")
    # 构建完整的节点依赖关系：A -> B 表示A依赖于B（A使用B作为source）
    for node in all_nodes:
        # 找出该节点使用的所有source（这些source可能是其他节点）
        node_sources = node_to_sources.get(node, set())
        for source in node_sources:
            if source in all_nodes:
                # 如果这个source本身也是一个节点，那么当前节点依赖于这个source节点
                node_dependency_graph[node].add(source)
    
    print(f"层级关系构建完成：{len(source_to_children)}个source有子节点，{len(node_to_sources)}个节点有关联source")
    return source_to_children, node_to_sources, source_to_nodes, node_to_parents, node_dependency_graph

def iterative_drilling(df_a, source_to_children, whitelist):
    """迭代下钻标记（严格校验子节点状态）"""
    max_iter = 20
    iter_count = 0
    changed = True
    terminal_nodes = []
    
    while changed and iter_count < max_iter:
        iter_count += 1
        changed = False
        prev_source_final = df_a['SOURCE_FINAL'].copy()
        
        # 定位空标记source（排除已标记1和终止节点）
        empty_source_mask = (df_a['SOURCE_FINAL'].isna()) & (~df_a['IS_TERMINAL'])
        empty_sources = df_a[empty_source_mask]['SOURCE_TABLE_NAME'].unique()
        
        if len(empty_sources) == 0:
            print(f"迭代{iter_count}轮：无空标记source，停止迭代")
            break
        
        print(f"迭代{iter_count}轮：处理{len(empty_sources)}个空标记source")
        
        for source in empty_sources:
            child_sources = source_to_children.get(source, set())
            
            # 场景1：无子节点 → 标记为终止节点，SOURCE_FINAL保持空
            if len(child_sources) == 0:
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'IS_TERMINAL'] = True
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'BLOCK_REASON'] = '无子节点且未匹配白名单'
                terminal_nodes.append(source)
                changed = True
                continue
            
            # 场景2：有子节点 → 检查所有子节点的标记状态
            child_marks = []
            has_unmarked_child = False
            for child in child_sources:
                # 获取子节点的标记值（去重）
                child_mark_vals = df_a[df_a['SOURCE_TABLE_NAME'] == child]['SOURCE_FINAL'].dropna().unique()
                if len(child_mark_vals) == 0:
                    # 子节点未标记 → 无法推导
                    has_unmarked_child = True
                    break
                child_marks.append(child_mark_vals[0])
            
            # 子节点存在未标记 → 当前source保持空
            if has_unmarked_child:
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'BLOCK_REASON'] = '子节点未全部标记'
                continue
            
            # 子节点全为1 → 当前source标记为1
            if all(mark == 1 for mark in child_marks):
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'SOURCE_FINAL'] = 1
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'BLOCK_REASON'] = '子节点全部标记为1'
                changed = True
            # 子节点存在非1 → 当前source保持空
            else:
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == source, 'BLOCK_REASON'] = '子节点存在非1标记'
                changed = True
        
        # 检查是否有更新
        if (df_a['SOURCE_FINAL'] == prev_source_final).all():
            changed = False
            print(f"迭代{iter_count}轮：无标记更新，停止迭代")
    
    # 去重终止节点
    terminal_nodes = list(set(terminal_nodes))
    print(f"\n向下迭代结束：共{iter_count}轮，检测到{len(terminal_nodes)}个终止节点")
    if terminal_nodes:
        print(f"终止节点列表：{', '.join(terminal_nodes[:10])}{'...' if len(terminal_nodes) > 10 else ''}")
    
    df_a['_iter_count'] = iter_count
    return df_a, terminal_nodes

def propagate_terminal_upwards_complete(df_a, terminal_nodes, node_to_parents, node_dependency_graph):
    """向上传播终止节点的未标记状态（完整链路影响）"""
    if not terminal_nodes:
        return df_a, set()
    
    print("\n开始向上传播终止节点影响（完整链路）...")
    
    # 记录受影响的所有节点
    all_affected_nodes = set()
    
    # 步骤1: 对于每个终止节点，找到所有直接和间接依赖它的节点
    for terminal_node in terminal_nodes:
        print(f"处理终止节点: {terminal_node}")
        
        # 使用BFS找到所有依赖该终止节点的节点
        queue = deque([terminal_node])
        visited = set([terminal_node])
        
        while queue:
            current = queue.popleft()
            
            # 找到所有直接依赖当前节点的节点
            for dependent_node in [n for n in node_dependency_graph if current in node_dependency_graph[n]]:
                if dependent_node not in visited:
                    visited.add(dependent_node)
                    queue.append(dependent_node)
                    all_affected_nodes.add(dependent_node)
    
    print(f"受影响的节点链: {all_affected_nodes}")
    
    # 步骤2: 标记所有受影响节点的所有source行为空
    affected_rows_count = 0
    for affected_node in all_affected_nodes:
        # 标记该节点下的所有SOURCE_TABLE_NAME为空
        node_mask = df_a['NODE_NAME'] == affected_node
        affected_rows = node_mask.sum()
        
        if affected_rows > 0:
            # 只处理未标记为1的行
            to_update_mask = node_mask & (df_a['SOURCE_FINAL'] != 1)
            if to_update_mask.any():
                df_a.loc[to_update_mask, 'SOURCE_FINAL'] = np.nan
                df_a.loc[to_update_mask, 'BLOCK_REASON'] = f'上游终止节点影响'
                affected_rows_count += to_update_mask.sum()
                print(f"  影响节点 {affected_node}: {to_update_mask.sum()} 行被标记为空")
    
    print(f"向上传播完成：影响了 {len(all_affected_nodes)} 个节点，共 {affected_rows_count} 行数据")
    return df_a, all_affected_nodes

def mark_statement_level(df_a):
    """代码块标记：严格校验分组内所有SOURCE_FINAL均为1"""
    group_key = ['NODE_NAME', 'BLOCK_NUM', 'STATMENT_NUM']
    
    # 按代码块分组，逐组判断
    statement_results = []
    for group_vals, group_df in df_a.groupby(group_key):
        node, block, stmt = group_vals
        # 分组内所有SOURCE_FINAL必须为1（无NaN、无非1）
        all_source_1 = (group_df['SOURCE_FINAL'].notna()).all() and (group_df['SOURCE_FINAL'] == 1).all()
        stmt_final = 1 if all_source_1 else np.nan
        statement_results.append({
            'NODE_NAME': node,
            'BLOCK_NUM': block,
            'STATMENT_NUM': stmt,
            'STATEMENT_FINAL': stmt_final
        })
    
    # 合并回原表
    stmt_df = pd.DataFrame(statement_results)
    df_a = df_a.merge(stmt_df, on=group_key, how='left', suffixes=('', '_new'))
    if 'STATEMENT_FINAL_new' in df_a.columns:
        df_a['STATEMENT_FINAL'] = df_a['STATEMENT_FINAL_new']
        df_a.drop('STATEMENT_FINAL_new', axis=1, inplace=True)
    
    # 统计标记数量
    stmt_1_count = len(stmt_df[stmt_df['STATEMENT_FINAL'] == 1])
    print(f"代码块标记完成：{stmt_1_count}个代码块标记为1")
    return df_a

def mark_node_level(df_a):
    """节点标记：严格校验节点下所有代码块的STATEMENT_FINAL均为1"""
    node_results = []
    for node, node_df in df_a.groupby('NODE_NAME'):
        # 获取节点下所有唯一代码块的STATEMENT_FINAL
        unique_stmt = node_df[['BLOCK_NUM', 'STATMENT_NUM', 'STATEMENT_FINAL']].drop_duplicates()
        # 所有代码块必须标记为1（无NaN、无非1）
        all_stmt_1 = (unique_stmt['STATEMENT_FINAL'].notna()).all() and (unique_stmt['STATEMENT_FINAL'] == 1).all()
        node_final = 1 if all_stmt_1 else np.nan
        node_results.append({
            'NODE_NAME': node,
            'NODE_FINAL': node_final
        })
    
    # 合并回原表
    node_df = pd.DataFrame(node_results)
    df_a = df_a.merge(node_df, on='NODE_NAME', how='left', suffixes=('', '_new'))
    if 'NODE_FINAL_new' in df_a.columns:
        df_a['NODE_FINAL'] = df_a['NODE_FINAL_new']
        df_a.drop('NODE_FINAL_new', axis=1, inplace=True)
    
    # 统计标记数量
    node_1_count = len(node_df[node_df['NODE_FINAL'] == 1])
    print(f"节点标记完成：{node_1_count}个节点标记为1")
    return df_a

def post_process_affected_statements(df_a):
    """后处理：确保受影响的节点中的所有代码块都被正确标记"""
    print("\n后处理：检查受影响的代码块标记...")
    
    # 找出所有SOURCE_FINAL为空的节点
    nodes_with_null_source = df_a[df_a['SOURCE_FINAL'].isna()]['NODE_NAME'].unique()
    
    for node in nodes_with_null_source:
        # 对于该节点的所有代码块，确保STATEMENT_FINAL为空
        node_mask = df_a['NODE_NAME'] == node
        df_a.loc[node_mask, 'STATEMENT_FINAL'] = np.nan
    
    return df_a

def generate_report(df_a, whitelist, terminal_nodes, affected_nodes=None):
    """生成分析报告"""
    total_rows = len(df_a)
    source_1_count = int((df_a['SOURCE_FINAL'] == 1).sum())
    source_null_count = int(df_a['SOURCE_FINAL'].isna().sum())
    
    # 代码块统计
    stmt_total = len(df_a[['NODE_NAME', 'BLOCK_NUM', 'STATMENT_NUM']].drop_duplicates())
    stmt_1_count = len(df_a[df_a['STATEMENT_FINAL'] == 1][['NODE_NAME', 'BLOCK_NUM', 'STATMENT_NUM']].drop_duplicates())
    
    # 节点统计
    node_total = len(df_a['NODE_NAME'].unique())
    node_1_count = len(df_a[df_a['NODE_FINAL'] == 1]['NODE_NAME'].unique())
    
    # 百分比计算
    source_1_pct = source_1_count / total_rows * 100 if total_rows > 0 else 0.0
    source_null_pct = source_null_count / total_rows * 100 if total_rows > 0 else 0.0
    stmt_1_pct = stmt_1_count / stmt_total * 100 if stmt_total > 0 else 0.0
    stmt_null_pct = (stmt_total - stmt_1_count) / stmt_total * 100 if stmt_total > 0 else 0.0
    node_1_pct = node_1_count / node_total * 100 if node_total > 0 else 0.0
    node_null_pct = (node_total - node_1_count) / node_total * 100 if node_total > 0 else 0.0
    
    # 构建报告
    report = f"""================================================================================
数据标记处理分析报告（最终修复版-完整链路传播）
================================================================================
一、基本统计信息
----------------------------------------
总数据行数: {total_rows}
白名单表数量: {len(whitelist)}
终止节点数量: {len(terminal_nodes)}
受影响节点数量: {len(affected_nodes) if affected_nodes else 0}
迭代匹配轮次: {int(df_a['_iter_count'].iloc[0])}

二、三级标记结果统计
----------------------------------------
1. 原表标记（SOURCE_FINAL）:
  标记为1: {source_1_count} 行 ({source_1_pct:.1f}%)
  未标记: {source_null_count} 行 ({source_null_pct:.1f}%)

2. 代码块标记（STATEMENT_FINAL）:
  标记为1: {stmt_1_count} 个 ({stmt_1_pct:.1f}%)
  未标记: {stmt_total - stmt_1_count} 个 ({stmt_null_pct:.1f}%)

3. 节点标记（NODE_FINAL）:
  标记为1: {node_1_count} 个 ({node_1_pct:.1f}%)
  未标记: {node_total - node_1_count} 个 ({node_null_pct:.1f}%)

"""
    
    # 终止节点详情
    report += "三、终止节点详情\n"
    report += "----------------------------------------\n"
    if terminal_nodes:
        for i, node in enumerate(terminal_nodes, 1):
            related_rows = df_a[df_a['SOURCE_TABLE_NAME'] == node]
            related_nodes = related_rows['NODE_NAME'].unique()
            reason = related_rows['BLOCK_REASON'].iloc[0] if len(related_rows) > 0 else '未知'
            report += f"{i}. {node}:\n"
            report += f"   关联行数: {len(related_rows)} 行\n"
            report += f"   关联节点: {', '.join(related_nodes)}\n"
            report += f"   未标记原因: {reason}\n"
    else:
        report += "  无终止节点\n"
    
    # 受影响节点详情
    if affected_nodes:
        report += "\n四、受终止节点影响的节点链\n"
        report += "----------------------------------------\n"
        for i, node in enumerate(sorted(affected_nodes), 1):
            node_rows = df_a[df_a['NODE_NAME'] == node]
            source_count = len(node_rows)
            null_source_count = node_rows['SOURCE_FINAL'].isna().sum()
            report += f"{i}. {node}: {null_source_count}/{source_count}行受影响\n"
    
    # 未标记SOURCE列表
    report += "\n五、未标记的SOURCE_TABLE_NAME列表\n"
    report += "----------------------------------------\n"
    null_sources = df_a[df_a['SOURCE_FINAL'].isna()]['SOURCE_TABLE_NAME'].unique()
    for i, source in enumerate(sorted(null_sources), 1):
        # 统计该source出现在哪些节点中
        related_nodes = df_a[df_a['SOURCE_TABLE_NAME'] == source]['NODE_NAME'].unique()
        related_rows = df_a[df_a['SOURCE_TABLE_NAME'] == source]
        reason = related_rows['BLOCK_REASON'].iloc[0] if len(related_rows) > 0 else '未知'
        report += f" {i:2d}. {source}\n"
        report += f"     关联节点: {', '.join(related_nodes)}\n"
        report += f"     原因: {reason}\n"
    
    return report, df_a

def save_results(df_a, report, output_path):
    """保存结果"""
    excel_path = output_path.replace('.txt', '.xlsx')
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df_a.to_excel(writer, sheet_name='详细标记数据', index=False)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\n结果保存完成：")
    print(f"  - Excel文件：{excel_path}")
    print(f"  - 报告文件：{output_path}")

def main():
    # 配置文件路径
    FILE_A = "复杂测试数据_A.xlsx"
    FILE_B = "复杂测试数据_B.xlsx"
    OUTPUT_REPORT = "数据标记处理分析报告（最终修复版-完整链路传播）.txt"
    
    try:
        # 1. 加载数据
        df_a, whitelist = load_and_validate_data(FILE_A, FILE_B)
        
        # 2. 初始标记
        df_a = init_marking(df_a, whitelist)
        
        # 3. 构建层级关系（包含完整依赖链）
        source_to_children, node_to_sources, source_to_nodes, node_to_parents, node_dependency_graph = build_hierarchy(df_a)
        
        # 4. 向下迭代标记
        df_a, terminal_nodes = iterative_drilling(df_a, source_to_children, whitelist)
        
        # 5. 向上传播终止节点影响（完整链路）
        df_a, affected_nodes = propagate_terminal_upwards_complete(df_a, terminal_nodes, node_to_parents, node_dependency_graph)
        
        # 6. 代码块标记
        df_a = mark_statement_level(df_a)
        
        # 7. 后处理：确保受影响的代码块被正确标记
        df_a = post_process_affected_statements(df_a)
        
        # 8. 节点标记
        df_a = mark_node_level(df_a)
        
        # 9. 生成报告和保存结果
        report, df_a = generate_report(df_a, whitelist, terminal_nodes, affected_nodes)
        save_results(df_a, report, OUTPUT_REPORT)
        
        print("\n" + "="*50)
        print("报告预览（前500字符）：")
        print("="*50)
        print(report[:500] + "...")
    
    except Exception as e:
        print(f"处理过程出错：{str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()