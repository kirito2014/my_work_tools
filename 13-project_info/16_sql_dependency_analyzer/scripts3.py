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
    
    print(f"初始标记完成：{source_match_mask.sum()}行原表标记为1")
    return df_a

def build_hierarchy(df_a):
    """构建层级关系：source -> 子节点列表、node -> 关联source列表"""
    source_to_children = defaultdict(set)  # source的子source
    node_to_sources = defaultdict(set)     # node下的所有source
    source_to_nodes = defaultdict(set)     # source所属的node
    
    # 1. 构建node与source的关联
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        node_to_sources[node].add(source)
        source_to_nodes[source].add(node)
    
    # 2. 构建source的子节点关系（source作为NODE_NAME时的子source）
    all_nodes = df_a['NODE_NAME'].unique()
    for source in df_a['SOURCE_TABLE_NAME'].unique():
        if source in all_nodes:
            # 该source作为node时的所有子source
            child_sources = df_a[df_a['NODE_NAME'] == source]['SOURCE_TABLE_NAME'].unique()
            for child in child_sources:
                source_to_children[source].add(child)
    
    print(f"层级关系构建完成：{len(source_to_children)}个source有子节点，{len(node_to_sources)}个节点有关联source")
    return source_to_children, node_to_sources, source_to_nodes

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
    print(f"\n迭代结束：共{iter_count}轮，检测到{len(terminal_nodes)}个终止节点")
    if terminal_nodes:
        print(f"终止节点列表：{', '.join(terminal_nodes[:10])}{'...' if len(terminal_nodes) > 10 else ''}")
    
    df_a['_iter_count'] = iter_count
    return df_a, terminal_nodes

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

def generate_report(df_a, whitelist, terminal_nodes):
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
数据标记处理分析报告（最终修复版）
================================================================================
一、基本统计信息
----------------------------------------
总数据行数: {total_rows}
白名单表数量: {len(whitelist)}
终止节点数量: {len(terminal_nodes)}
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
    
    # 未标记SOURCE列表
    report += "\n四、未标记的SOURCE_TABLE_NAME列表\n"
    report += "----------------------------------------\n"
    null_sources = df_a[df_a['SOURCE_FINAL'].isna()]['SOURCE_TABLE_NAME'].unique()
    for i, source in enumerate(sorted(null_sources), 1):
        report += f" {i:2d}. {source}\n"
    
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
    OUTPUT_REPORT = "数据标记处理分析报告（最终修复版）.txt"
    
    try:
        df_a, whitelist = load_and_validate_data(FILE_A, FILE_B)
        df_a = init_marking(df_a, whitelist)
        source_to_children, node_to_sources, source_to_nodes = build_hierarchy(df_a)
        df_a, terminal_nodes = iterative_drilling(df_a, source_to_children, whitelist)
        df_a = mark_statement_level(df_a)
        df_a = mark_node_level(df_a)
        report, df_a = generate_report(df_a, whitelist, terminal_nodes)
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