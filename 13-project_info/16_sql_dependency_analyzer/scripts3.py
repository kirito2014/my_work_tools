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
    df_a['BLOCKED_BY'] = ''  # 记录被哪个终止节点阻塞
    df_a['IS_OVERRIDDEN'] = False  # 是否被强制置空
    
    # 条件1：NODE_NAME在白名单中，整行标记为1
    node_match = df_a['NODE_NAME'].isin(whitelist)
    df_a.loc[node_match, 'NODE_FLAG'] = 1
    df_a.loc[node_match, 'SOURCE_FLAG'] = 1
    
    # 条件2：SOURCE_TABLE_NAME在白名单中，标记为1
    source_match = df_a['SOURCE_TABLE_NAME'].isin(whitelist)
    df_a.loc[source_match & (df_a['SOURCE_FLAG'] == ''), 'SOURCE_FLAG'] = 1
    
    return df_a, whitelist

def build_hierarchy_graph(df_a):
    """构建完整的层级关系图"""
    # 节点到源表的映射：node -> {sources}
    node_children = defaultdict(set)
    
    # 源表到父节点的映射：source -> {parents}
    node_parents = defaultdict(set)
    
    # 所有表名集合
    all_tables = set()
    
    for _, row in df_a.iterrows():
        parent = row['NODE_NAME']
        child = row['SOURCE_TABLE_NAME']
        
        if parent and child:
            node_children[parent].add(child)
            node_parents[child].add(parent)
            all_tables.add(parent)
            all_tables.add(child)
    
    return node_children, node_parents, all_tables

def find_terminal_nodes(node_children, all_tables, whitelist):
    """查找终止节点（无子节点且不在白名单）"""
    terminal_nodes = []
    terminal_details = []
    
    for table in all_tables:
        if table == '':
            continue
            
        # 终止节点条件：不在白名单 + 没有子节点
        if table not in whitelist and len(node_children.get(table, set())) == 0:
            terminal_nodes.append(table)
            terminal_details.append({
                '终止节点': table,
                '所在层级': '叶子节点',
                '原因': '不在白名单且无下游节点'
            })
    
    return terminal_nodes, terminal_details

def trace_upstream_nodes(target_node, node_parents):
    """追踪目标节点的所有上游节点"""
    upstream_nodes = set()
    visited = set()
    queue = deque([target_node])
    
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
            
        visited.add(current)
        parents = node_parents.get(current, set())
        
        for parent in parents:
            upstream_nodes.add(parent)
            queue.append(parent)
    
    return sorted(list(upstream_nodes))

def propagate_blocking(df_a, terminal_nodes, node_parents, node_children):
    """传播阻塞标记：终止节点导致所有上游节点置空"""
    blocking_map = {}  # 记录每个节点被哪个终止节点阻塞
    all_blocked_nodes = set()
    
    # 对每个终止节点，追踪所有上游节点
    for terminal in terminal_nodes:
        upstream_nodes = trace_upstream_nodes(terminal, node_parents)
        blocking_map[terminal] = upstream_nodes
        all_blocked_nodes.update(upstream_nodes)
        all_blocked_nodes.add(terminal)
        
        # 标记被该终止节点影响的记录
        df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(upstream_nodes + [terminal]), 'BLOCKED_BY'] += terminal + ';'
        df_a.loc[df_a['NODE_NAME'].isin(upstream_nodes + [terminal]), 'BLOCKED_BY'] += terminal + ';'
    
    # 强制置空所有被阻塞的节点
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(all_blocked_nodes), 'SOURCE_FLAG'] = ''
    df_a.loc[df_a['NODE_NAME'].isin(all_blocked_nodes), 'NODE_FLAG'] = ''
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(terminal_nodes), 'TERMINAL_NODE'] = True
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(all_blocked_nodes), 'IS_OVERRIDDEN'] = True
    df_a.loc[df_a['NODE_NAME'].isin(all_blocked_nodes), 'IS_OVERRIDDEN'] = True
    
    return df_a, blocking_map, all_blocked_nodes

def generate_terminal_report(terminal_nodes, blocking_map):
    """生成终止节点详细报告"""
    report_data = []
    
    for terminal in terminal_nodes:
        upstream_nodes = blocking_map.get(terminal, [])
        report_data.append({
            '终止节点名称': terminal,
            '影响的上游节点数量': len(upstream_nodes),
            '上游节点列表': ', '.join(upstream_nodes) if upstream_nodes else '无',
            '阻断链路长度': len(upstream_nodes) + 1,
            '状态': '已阻断所有上游节点'
        })
    
    return pd.DataFrame(report_data)

def generate_node_summary(df_a):
    """生成节点汇总报告"""
    summary = df_a.groupby('NODE_NAME').agg({
        'SOURCE_FLAG': [
            ('总行数', 'count'),
            ('标记1数量', lambda x: (x == 1).sum()),
            ('空标记数量', lambda x: (x == '').sum()),
            ('被否决数量', lambda x: sum((x == '') & (df_a.loc[x.index, 'IS_OVERRIDDEN'])))
        ],
        'NODE_FLAG': [('最终节点标记', lambda x: x.iloc[0] if len(x) > 0 else '')],
        'TERMINAL_NODE': [('包含终止节点', 'any')],
        'IS_OVERRIDDEN': [('被强制置空', 'any')]
    }).round(2)
    
    # 展平列名
    summary.columns = ['总行数', '标记1数量', '空标记数量', '被否决数量', 
                       '最终节点标记', '包含终止节点', '被强制置空']
    
    # 计算标记率
    summary['有效标记率'] = (summary['标记1数量'] / summary['总行数'] * 100).round(2)
    summary['否决率'] = (summary['被否决数量'] / summary['总行数'] * 100).round(2)
    
    return summary

def final_processing(df_a):
    """最终处理：生成最终标记列"""
    df_a['SOURCE_FINAL'] = df_a['SOURCE_FLAG'].replace('', np.nan)
    df_a['NODE_FINAL'] = df_a['NODE_FLAG'].replace('', np.nan)
    
    # 清理BLOCKED_BY字段
    df_a['BLOCKED_BY'] = df_a['BLOCKED_BY'].str.strip(';').str.replace(';;', ';')
    
    return df_a

def main():
    """主函数"""
    FILE_A = "复杂测试数据_A.xlsx"  
    FILE_B = "复杂测试数据_B.xlsx"       
    OUTPUT = '终止节点追踪结果.xlsx'
    
    try:
        print("=== 数据加载阶段 ===")
        df_a, df_b = load_data(FILE_A, FILE_B)
        print(f"表A数据行数: {len(df_a)}")
        print(f"表B白名单数量: {len(df_b)}")
        
        print("\n=== 初始标记阶段 ===")
        df_a, whitelist = initial_marking(df_a, df_b)
        initial_1_count = (df_a['SOURCE_FLAG'] == 1).sum()
        print(f"初始标记为1的行数: {initial_1_count}")
        
        print("\n=== 构建层级关系 ===")
        node_children, node_parents, all_tables = build_hierarchy_graph(df_a)
        print(f"总表数量: {len(all_tables)}")
        print(f"层级关系数: {sum(len(v) for v in node_children.values())}")
        
        print("\n=== 查找终止节点 ===")
        terminal_nodes, terminal_details = find_terminal_nodes(node_children, all_tables, whitelist)
        print(f"发现终止节点数量: {len(terminal_nodes)}")
        for node in terminal_nodes:
            print(f"  - {node}")
        
        print("\n=== 传播阻塞标记 ===")
        df_a, blocking_map, all_blocked_nodes = propagate_blocking(df_a, terminal_nodes, node_parents, node_children)
        print(f"被阻断的节点总数: {len(all_blocked_nodes)}")
        
        print("\n=== 生成报告 ===")
        terminal_report = generate_terminal_report(terminal_nodes, blocking_map)
        node_summary = generate_node_summary(df_a)
        df_a = final_processing(df_a)
        
        # 保存结果
        with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
            df_a.to_excel(writer, sheet_name='详细标记数据', index=False)
            terminal_report.to_excel(writer, sheet_name='终止节点报告', index=False)
            node_summary.to_excel(writer, sheet_name='节点汇总分析', index=True)
        
        print(f"\n=== 处理完成 ===")
        print(f"结果已保存至: {OUTPUT}")
        
        print("\n=== 关键统计信息 ===")
        print(f"终止节点数量: {len(terminal_nodes)}")
        print(f"被阻断的上游节点数量: {len(all_blocked_nodes) - len(terminal_nodes)}")
        print(f"最终标记为1的行数: {(df_a['SOURCE_FLAG'] == 1).sum()}")
        print(f"被强制置空的行数: {(df_a['IS_OVERRIDDEN'] == True).sum()}")
        
        print("\n=== 终止节点影响详情 ===")
        for terminal, upstream in blocking_map.items():
            print(f"\n终止节点 '{terminal}' 影响的上游节点:")
            if upstream:
                print(f"  {', '.join(upstream)}")
            else:
                print("  无上游节点")
        
    except Exception as e:
        print(f"\n处理过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()