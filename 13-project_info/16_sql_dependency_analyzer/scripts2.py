import pandas as pd
import numpy as np
from collections import defaultdict
#-------ddddd
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
    
    for _, row in df_a.iterrows():
        node = row['NODE_NAME']
        source = row['SOURCE_TABLE_NAME']
        
        if node and source:
            node_to_sources[node].add(source)
            source_to_nodes[source].add(node)
    
    return node_to_sources, source_to_nodes

def iterative_marking(df_a, node_to_sources, source_to_nodes):
    """迭代标记：基于层级关系的补充标记"""
    max_iterations = 20  # 防止无限循环
    iteration = 0
    changed = True
    
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        print(f"迭代轮次 {iteration}...")
        
        # 复制当前标记状态用于比较
        prev_flags = df_a['SOURCE_FLAG'].copy()
        
        # 按BLOCK_NUM分组处理
        for block_num in df_a['BLOCK_NUM'].unique():
            block_data = df_a[df_a['BLOCK_NUM'] == block_num]
            
            # 找出当前块中空标记的SOURCE_TABLE_NAME
            empty_sources = block_data[block_data['SOURCE_FLAG'] == '']['SOURCE_TABLE_NAME'].unique()
            
            for source in empty_sources:
                if not source:
                    continue
                
                # 将空标记的source作为node，查找其对应的sources
                child_sources = node_to_sources.get(source, set())
                
                if child_sources:
                    # 获取子节点的标记状态
                    child_flags = []
                    for child in child_sources:
                        child_flag = df_a[(df_a['BLOCK_NUM'] == block_num) & 
                                        (df_a['SOURCE_TABLE_NAME'] == child)]['SOURCE_FLAG'].unique()
                        child_flags.extend(child_flag)
                    
                    # 子节点全部标记为1，则当前source标记为1
                    if child_flags and '' not in child_flags and all(flag == 1 for flag in child_flags):
                        # 更新当前block中该source的所有标记
                        update_mask = (df_a['BLOCK_NUM'] == block_num) & \
                                    (df_a['SOURCE_TABLE_NAME'] == source) & \
                                    (df_a['SOURCE_FLAG'] == '')
                        
                        if update_mask.any():
                            df_a.loc[update_mask, 'SOURCE_FLAG'] = 1
                            changed = True
        
        # 更新NODE_FLAG：如果节点下的所有source都标记为1，则节点标记为1
        for node in df_a['NODE_NAME'].unique():
            if not node:
                continue
                
            node_mask = df_a['NODE_NAME'] == node
            node_sources = df_a[node_mask]['SOURCE_FLAG'].unique()
            
            if '' not in node_sources and all(flag == 1 for flag in node_sources):
                df_a.loc[node_mask & (df_a['NODE_FLAG'] == ''), 'NODE_FLAG'] = 1
    
    return df_a

def final_processing(df_a):
    """最终处理：生成最终标记列"""
    # 处理剩余空值
    df_a['SOURCE_FINAL'] = df_a['SOURCE_FLAG'].replace('', np.nan)
    df_a['NODE_FINAL'] = df_a['NODE_FLAG'].replace('', np.nan)
    
    # 创建汇总统计
    summary = df_a.groupby('NODE_NAME').agg({
        'SOURCE_FINAL': ['count', lambda x: (x == 1).sum()],
        'NODE_FINAL': 'first'
    }).round(2)
    
    summary.columns = ['总行数', '标记1数量', '节点最终标记']
    summary['标记覆盖率'] = (summary['标记1数量'] / summary['总行数'] * 100).round(2)
    
    return df_a, summary

def main():
    """主函数"""
    # 文件路径配置
    FILE_A = '表A.xlsx'
    FILE_B = '表B.xlsx'
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
        node_to_sources, source_to_nodes = build_table_relations(df_a)
        print(f"节点数量: {len(node_to_sources)}")
        print(f"源表数量: {len(source_to_nodes)}")
        
        print("\n=== 迭代标记阶段 ===")
        df_a = iterative_marking(df_a, node_to_sources, source_to_nodes)
        final_1_count = (df_a['SOURCE_FLAG'] == 1).sum()
        print(f"最终标记为1的行数: {final_1_count}")
        print(f"新增标记行数: {final_1_count - initial_1_count}")
        
        print("\n=== 结果处理阶段 ===")
        df_a, summary = final_processing(df_a)
        
        # 保存结果
        with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
            df_a.to_excel(writer, sheet_name='详细标记', index=False)
            summary.to_excel(writer, sheet_name='节点汇总', index=True)
        
        print(f"\n=== 处理完成 ===")
        print(f"结果已保存至: {OUTPUT}")
        print("\n节点汇总结果预览:")
        print(summary.head(10))
        
    except Exception as e:
        print(f"\n处理过程中出错: {str(e)}")
        raise

if __name__ == "__main__":
    # 安装依赖：pip install pandas openpyxl numpy
    main()