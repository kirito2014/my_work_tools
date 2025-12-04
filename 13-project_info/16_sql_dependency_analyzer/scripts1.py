import pandas as pd
import numpy as np
from collections import defaultdict, deque

def load_data(file_a_path, file_b_path):
    """加载Excel数据"""
    df_a = pd.read_excel(file_a_path)
    df_b = pd.read_excel(file_b_path)
    
    # 清理列名并验证必要列
    df_a.columns = df_a.columns.str.strip()
    df_b.columns = df_b.columns.str.strip()
    
    required_cols = ['NODE_NAME', 'BLOCK_NUM', 'STATMENT_NUM', 'SOURCE_TABLE_NAME']
    for col in required_cols:
        if col not in df_a.columns:
            raise ValueError(f"表A缺少必要列: {col}")
    
    if 'TBL_NM' not in df_b.columns:
        raise ValueError("表B缺少必要列: TBL_NM")
    
    # 处理空值
    df_a['SOURCE_TABLE_NAME'] = df_a['SOURCE_TABLE_NAME'].fillna('')
    df_a['NODE_NAME'] = df_a['NODE_NAME'].fillna('')
    
    return df_a, df_b

def initial_marking(df_a, df_b):
    """初始标记：基于表B的直接匹配"""
    # 获取表B的白名单
    whitelist = set(df_b['TBL_NM'].dropna().astype(str).tolist())
    
    # 初始化标记列
    df_a['SOURCE_FLAG'] = ''
    df_a['NODE_FLAG'] = ''
    df_a['TERMINAL_NODE'] = False
    df_a['OVERRIDDEN'] = False  # 是否被子节点否决
    
    # 条件1：NODE_NAME在白名单中，整行标记为1
    node_match = df_a['NODE_NAME'].isin(whitelist)
    df_a.loc[node_match, 'NODE_FLAG'] = 1
    df_a.loc[node_match, 'SOURCE_FLAG'] = 1
    
    # 条件2：SOURCE_TABLE_NAME在白名单中，标记为1
    source_match = df_a['SOURCE_TABLE_NAME'].isin(whitelist)
    df_a.loc[source_match & (df_a['SOURCE_FLAG'] == ''), 'SOURCE_FLAG'] = 1
    
    return df_a, whitelist

def build_complete_relations(df_a):
    """构建完整的父子关系映射（包含所有层级）"""
    # 节点到源表的映射：node -> {sources}
    node_to_sources = defaultdict(set)
    
    # 源表到节点的反向映射：source -> {parents}
    source_to_parents = defaultdict(set)
    
    # 所有节点和源表的集合
    all_tables = set()
    
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        
        if node and source:
            node_to_sources[node].add(source)
            source_to_parents[source].add(node)
            all_tables.add(node)
            all_tables.add(source)
    
    return node_to_sources, source_to_parents, all_tables

def detect_terminal_nodes_enhanced(df_a, node_to_sources, all_tables, whitelist):
    """增强版终止节点检测"""
    terminal_nodes = set()
    
    for table in all_tables:
        # 终止节点条件：不在白名单中 + 没有子节点 + 不是白名单节点的子节点
        if (table not in whitelist and 
            len(node_to_sources.get(table, set())) == 0 and
            table != ''):
            terminal_nodes.add(table)
    
    # 标记终止节点
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(terminal_nodes), 'TERMINAL_NODE'] = True
    
    print(f"\n检测到终止节点: {terminal_nodes}")
    return df_a, terminal_nodes

def propagate_deny_mark(df_a, source_to_parents, terminal_nodes, whitelist):
    """向上传播否决标记（子节点空→父节点空）"""
    denied_nodes = set(terminal_nodes)
    queue = deque(terminal_nodes)
    
    # 使用BFS向上传播否决标记
    while queue:
        current_node = queue.popleft()
        
        # 找到当前节点的所有父节点
        parents = source_to_parents.get(current_node, set())
        
        for parent in parents:
            if parent not in denied_nodes:
                denied_nodes.add(parent)
                queue.append(parent)
                
                # 强制将父节点标记为空，覆盖原有标记
                df_a.loc[df_a['NODE_NAME'] == parent, 'NODE_FLAG'] = ''
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == parent, 'SOURCE_FLAG'] = ''
                df_a.loc[df_a['NODE_NAME'] == parent, 'OVERRIDDEN'] = True
                df_a.loc[df_a['SOURCE_TABLE_NAME'] == parent, 'OVERRIDDEN'] = True
    
    print(f"被否决的节点（含终止节点）: {denied_nodes}")
    return df_a, denied_nodes

def final_adjustment(df_a, denied_nodes):
    """最终调整标记"""
    # 确保被否决的节点所有相关标记都是空
    df_a.loc[df_a['NODE_NAME'].isin(denied_nodes), 'NODE_FLAG'] = ''
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(denied_nodes), 'SOURCE_FLAG'] = ''
    
    # 生成最终标记列
    df_a['SOURCE_FINAL'] = df_a['SOURCE_FLAG'].replace('', np.nan)
    df_a['NODE_FINAL'] = df_a['NODE_FLAG'].replace('', np.nan)
    
    return df_a

def generate_detailed_report(df_a, denied_nodes, terminal_nodes):
    """生成详细报告"""
    # 节点汇总
    summary = df_a.groupby('NODE_NAME').agg({
        'SOURCE_FINAL': ['count', lambda x: (x == 1).sum(), lambda x: x.isna().sum()],
        'NODE_FINAL': 'first',
        'TERMINAL_NODE': 'any',
        'OVERRIDDEN': 'any'
    }).round(2)
    
    summary.columns = ['总行数', '标记1数量', '空标记数量', '节点最终标记', '包含终止节点', '被子节点否决']
    
    # 添加否决状态
    summary['是否被否决'] = summary.index.isin(denied_nodes)
    summary['是否终止节点'] = summary.index.isin(terminal_nodes)
    
    return summary

def main():
    """主函数"""
    FILE_A = "复杂测试数据_A.xlsx"  
    FILE_B = "复杂测试数据_B.xlsx"       
    OUTPUT = '修正标记结果.xlsx'
    
    try:
        print("=== 数据加载阶段 ===")
        df_a, df_b = load_data(FILE_A, FILE_B)
        print(f"表A数据行数: {len(df_a)}")
        print(f"表B白名单数量: {len(df_b)}")
        
        print("\n=== 初始标记阶段 ===")
        df_a, whitelist = initial_marking(df_a, df_b)
        initial_1_count = (df_a['SOURCE_FLAG'] == 1).sum()
        print(f"初始标记为1的行数: {initial_1_count}")
        
        print("\n=== 构建关系映射 ===")
        node_to_sources, source_to_parents, all_tables = build_complete_relations(df_a)
        print(f"总表数量: {len(all_tables)}")
        
        print("\n=== 检测终止节点 ===")
        df_a, terminal_nodes = detect_terminal_nodes_enhanced(df_a, node_to_sources, all_tables, whitelist)
        
        print("\n=== 传播否决标记 ===")
        df_a, denied_nodes = propagate_deny_mark(df_a, source_to_parents, terminal_nodes, whitelist)
        
        print("\n=== 最终调整 ===")
        df_a = final_adjustment(df_a, denied_nodes)
        
        print("\n=== 生成报告 ===")
        summary = generate_detailed_report(df_a, denied_nodes, terminal_nodes)
        
        # 保存结果
        with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
            df_a.to_excel(writer, sheet_name='详细标记', index=False)
            summary.to_excel(writer, sheet_name='节点汇总', index=True)
        
        print(f"\n=== 处理完成 ===")
        print(f"结果已保存至: {OUTPUT}")
        
        print("\n=== 关键统计 ===")
        print(f"终止节点数量: {len(terminal_nodes)}")
        print(f"被否决节点数量: {len(denied_nodes)}")
        print(f"最终标记为1的行数: {(df_a['SOURCE_FLAG'] == 1).sum()}")
        print(f"被否决的行数: {(df_a['OVERRIDDEN'] == True).sum()}")
        
        print("\n节点汇总结果预览:")
        print(summary.head(10))
        
    except Exception as e:
        print(f"\n处理过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()