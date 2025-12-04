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
    df_a['TERMINAL_NODE'] = False  # 标记是否为终止节点
    
    # 条件1：NODE_NAME在白名单中，整行标记为1
    node_match = df_a['NODE_NAME'].isin(whitelist)
    df_a.loc[node_match, 'NODE_FLAG'] = 1
    df_a.loc[node_match, 'SOURCE_FLAG'] = 1
    
    # 条件2：SOURCE_TABLE_NAME在白名单中，标记为1
    source_match = df_a['SOURCE_TABLE_NAME'].isin(whitelist)
    df_a.loc[source_match & (df_a['SOURCE_FLAG'] == ''), 'SOURCE_FLAG'] = 1
    
    return df_a, whitelist

def build_table_relations(df_a):
    """构建表之间的层级关系映射"""
    # 节点到源表的映射：node -> {sources}
    node_to_sources = defaultdict(set)
    
    # 源表到节点的反向映射：source -> {nodes}
    source_to_nodes = defaultdict(set)
    
    # 所有存在的节点名称
    all_nodes = set(df_a['NODE_NAME'].unique())
    
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        
        if node and source:
            node_to_sources[node].add(source)
            source_to_nodes[source].add(node)
    
    return node_to_sources, source_to_nodes, all_nodes

def detect_terminal_nodes(df_a, node_to_sources, all_nodes, whitelist):
    """检测终止节点（B表无数据且无子节点的节点）"""
    terminal_nodes = set()
    
    for node in all_nodes:
        # 条件：不在白名单中，且没有子节点
        if node not in whitelist and len(node_to_sources.get(node, set())) == 0:
            terminal_nodes.add(node)
    
    # 标记终止节点
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(terminal_nodes), 'TERMINAL_NODE'] = True
    
    print(f"\n检测到终止节点: {terminal_nodes}")
    return df_a, terminal_nodes

def propagate_empty_marking(df_a, source_to_nodes, terminal_nodes):
    """将终止节点的空标记向上传播"""
    affected_nodes = set(terminal_nodes)
    queue = deque(terminal_nodes)
    
    # 使用BFS向上传播
    while queue:
        current_node = queue.popleft()
        
        # 找到当前节点的所有父节点
        parent_nodes = source_to_nodes.get(current_node, set())
        
        for parent in parent_nodes:
            if parent not in affected_nodes:
                affected_nodes.add(parent)
                queue.append(parent)
    
    # 标记受影响的节点
    df_a.loc[df_a['NODE_NAME'].isin(affected_nodes), 'NODE_FLAG'] = ''
    df_a.loc[df_a['SOURCE_TABLE_NAME'].isin(affected_nodes), 'SOURCE_FLAG'] = ''
    
    print(f"受影响的父节点: {affected_nodes - terminal_nodes}")
    return df_a, affected_nodes

def iterative_marking(df_a, node_to_sources, source_to_nodes, whitelist):
    """迭代标记：基于层级关系的补充标记"""
    max_iterations = 20
    iteration = 0
    changed = True
    
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        print(f"\n迭代轮次 {iteration}...")
        
        prev_flags = df_a['SOURCE_FLAG'].copy()
        
        # 按BLOCK_NUM分组处理
        for block_num in df_a['BLOCK_NUM'].unique():
            block_data = df_a[df_a['BLOCK_NUM'] == block_num]
            
            # 找出当前块中空标记且非终止节点的SOURCE_TABLE_NAME
            empty_sources = block_data[
                (block_data['SOURCE_FLAG'] == '') & 
                (block_data['TERMINAL_NODE'] == False)
            ]['SOURCE_TABLE_NAME'].unique()
            
            for source in empty_sources:
                if not source or source in whitelist:
                    continue
                
                child_sources = node_to_sources.get(source, set())
                
                if child_sources:
                    # 获取子节点的标记状态
                    child_flags = []
                    for child in child_sources:
                        child_flag = df_a[
                            (df_a['BLOCK_NUM'] == block_num) & 
                            (df_a['SOURCE_TABLE_NAME'] == child)
                        ]['SOURCE_FLAG'].unique()
                        child_flags.extend(child_flag)
                    
                    # 子节点全部标记为1，则当前source标记为1
                    if child_flags and '' not in child_flags and all(flag == 1 for flag in child_flags):
                        update_mask = (
                            (df_a['BLOCK_NUM'] == block_num) & 
                            (df_a['SOURCE_TABLE_NAME'] == source) & 
                            (df_a['SOURCE_FLAG'] == '') &
                            (df_a['TERMINAL_NODE'] == False)
                        )
                        
                        if update_mask.any():
                            df_a.loc[update_mask, 'SOURCE_FLAG'] = 1
                            changed = True
        
        # 更新NODE_FLAG
        for node in df_a['NODE_NAME'].unique():
            if not node:
                continue
                
            node_mask = df_a['NODE_NAME'] == node
            node_sources = df_a[node_mask]['SOURCE_FLAG'].unique()
            
            # 如果所有子节点都是1，则标记为1；否则保持为空
            if '' not in node_sources and all(flag == 1 for flag in node_sources):
                df_a.loc[node_mask & (df_a['NODE_FLAG'] == ''), 'NODE_FLAG'] = 1
    
    return df_a

def final_summary(df_a, terminal_nodes, affected_nodes):
    """生成最终汇总报告"""
    # 创建最终标记列
    df_a['SOURCE_FINAL'] = df_a['SOURCE_FLAG'].replace('', np.nan)
    df_a['NODE_FINAL'] = df_a['NODE_FLAG'].replace('', np.nan)
    
    # 统计汇总
    summary = df_a.groupby('NODE_NAME').agg({
        'SOURCE_FINAL': ['count', lambda x: (x == 1).sum(), lambda x: x.isna().sum()],
        'NODE_FINAL': 'first',
        'TERMINAL_NODE': 'any'
    }).round(2)
    
    summary.columns = ['总行数', '标记1数量', '空标记数量', '节点最终标记', '包含终止节点']
    summary['标记覆盖率'] = (summary['标记1数量'] / summary['总行数'] * 100).round(2)
    
    # 突出显示受影响的节点
    summary['受终止节点影响'] = summary.index.isin(affected_nodes)
    
    return df_a, summary

def main():
    """主函数"""
    # 文件路径配置
    FILE_A = '复杂测试数据_A.xlsx'
    FILE_B = '复杂测试数据_B.xlsx'
    OUTPUT = '最终标记结果.xlsx'
    
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
        node_to_sources, source_to_nodes, all_nodes = build_table_relations(df_a)
        print(f"节点数量: {len(node_to_sources)}")
        
        print("\n=== 检测终止节点 ===")
        df_a, terminal_nodes = detect_terminal_nodes(df_a, node_to_sources, all_nodes, whitelist)
        
        print("\n=== 迭代标记阶段 ===")
        df_a = iterative_marking(df_a, node_to_sources, source_to_nodes, whitelist)
        
        print("\n=== 传播空标记 ===")
        df_a, affected_nodes = propagate_empty_marking(df_a, source_to_nodes, terminal_nodes)
        
        print("\n=== 结果汇总 ===")
        df_a, summary = final_summary(df_a, terminal_nodes, affected_nodes)
        
        # 保存结果
        with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
            df_a.to_excel(writer, sheet_name='详细标记', index=False)
            summary.to_excel(writer, sheet_name='节点汇总', index=True)
        
        print(f"\n=== 处理完成 ===")
        print(f"结果已保存至: {OUTPUT}")
        
        print("\n=== 关键统计 ===")
        print(f"终止节点数量: {len(terminal_nodes)}")
        print(f"受影响节点数量: {len(affected_nodes) - len(terminal_nodes)}")
        print(f"最终标记为1的行数: {(df_a['SOURCE_FLAG'] == 1).sum()}")
        print(f"空标记行数: {(df_a['SOURCE_FLAG'] == '').sum()}")
        
        print("\n节点汇总结果预览:")
        print(summary.head(10))
        
    except Exception as e:
        print(f"\n处理过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    # 安装依赖：pip install pandas openpyxl numpy
    main()