import pandas as pd
import numpy as np
from collections import defaultdict, deque
import warnings
warnings.filterwarnings('ignore')

def initial_stats(df_a):
    """初始标记统计"""
    total_rows = len(df_a)
    source_marked = (df_a['SOURCE_TABLE_NAME_FLAG'] != '').sum()
    node_marked = (df_a['NODE_NAME_FLAG'] != '').sum()
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    
    print(f"\n初始标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME已标记: {source_marked} ({source_marked/total_rows*100:.1f}%)")
    print(f"  SOURCE_TABLE_NAME标记为1: {source_1_count}")
    print(f"  NODE_NAME已标记: {node_marked} ({node_marked/total_rows*100:.1f}%)")
    print(f"  NODE_NAME标记为1: {node_1_count}")

def iterative_stats(df_a):
    """迭代标记统计"""
    total_rows = len(df_a)
    source_marked = (df_a['SOURCE_TABLE_NAME_FLAG'] != '').sum()
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    source_empty = (df_a['SOURCE_TABLE_NAME_FLAG'] == '').sum()
    node_marked = (df_a['NODE_NAME_FLAG'] != '').sum()
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    node_empty = (df_a['NODE_NAME_FLAG'] == '').sum()
    
    print(f"\n迭代标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME标记为1: {source_1_count} ({source_1_count/total_rows*100:.1f}%)")
    print(f"  SOURCE_TABLE_NAME未标记: {source_empty} ({source_empty/total_rows*100:.1f}%)")
    print(f"  NODE_NAME标记为1: {node_1_count} ({node_1_count/total_rows*100:.1f}%)")
    print(f"  NODE_NAME未标记: {node_empty} ({node_empty/total_rows*100:.1f}%)")

def final_stats(df_a):
    """最终标记统计"""
    total_rows = len(df_a)
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    source_empty = (df_a['SOURCE_TABLE_NAME_FLAG'] == '').sum()
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    node_empty = (df_a['NODE_NAME_FLAG'] == '').sum()
    
    print(f"\n最终标记统计:")
    print(f"  总行数: {total_rows}")
    print(f"  SOURCE_TABLE_NAME标记为1: {source_1_count} ({source_1_count/total_rows*100:.1f}%)")
    print(f"  SOURCE_TABLE_NAME未标记: {source_empty} ({source_empty/total_rows*100:.1f}%)")
    print(f"  NODE_NAME标记为1: {node_1_count} ({node_1_count/total_rows*100:.1f}%)")
    print(f"  NODE_NAME未标记: {node_empty} ({node_empty/total_rows*100:.1f}%)")

def initial_marking(df_a, df_b):
    """
    初始标记阶段：基于表B的白名单进行直接匹配
    注意：即使节点在B表中，后续仍可能被强制置为空
    """
    print("=" * 60)
    print("阶段一：初始标记阶段")
    print("=" * 60)
    
    # 获取B表中的所有表名
    tbl_nm_list = df_b['TBL_NM'].dropna().astype(str).unique().tolist()
    print(f"白名单表B中共有 {len(tbl_nm_list)} 个表名")
    
    # 初始化标记列
    df_a['SOURCE_TABLE_NAME_FLAG'] = ''  # SOURCE_TABLE_NAME标记
    df_a['NODE_NAME_FLAG'] = ''          # NODE_NAME标记
    
    # 1. NODE_NAME匹配：若NODE_NAME存在于表B中，对应的所有行直接标记为1
    node_names_in_b = df_a['NODE_NAME'][df_a['NODE_NAME'].astype(str).isin(tbl_nm_list)].unique()
    print(f"\n1. NODE_NAME匹配:")
    print(f"   在表B中找到的NODE_NAME: {len(node_names_in_b)} 个")
    
    for node_name in node_names_in_b:
        mask = df_a['NODE_NAME'] == node_name
        df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'] = 1
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

def build_dependency_graph(df_a):
    """
    构建完整的依赖关系图
    返回：
    - 节点到子节点的映射
    - 节点到父节点的映射
    - 所有节点集合
    - 所有表集合
    """
    print("\n" + "=" * 60)
    print("构建依赖关系图")
    print("=" * 60)
    
    # 构建映射关系
    node_to_children = defaultdict(set)      # 节点 → 直接子节点
    node_to_all_children = defaultdict(set)  # 节点 → 所有后代节点（递归）
    child_to_parents = defaultdict(set)      # 子节点 → 直接父节点
    child_to_all_parents = defaultdict(set)  # 子节点 → 所有祖先节点（递归）
    all_tables = set()                       # 所有表名（节点+来源表）
    all_nodes = set()                        # 所有节点名
    
    # 第一遍：构建直接关系
    for _, row in df_a.iterrows():
        node_name = str(row['NODE_NAME'])
        source_name = str(row['SOURCE_TABLE_NAME'])
        
        # 记录表名
        all_tables.add(node_name)
        all_tables.add(source_name)
        all_nodes.add(node_name)
        
        # 直接关系
        node_to_children[node_name].add(source_name)
        child_to_parents[source_name].add(node_name)
    
    print(f"直接关系统计:")
    print(f"  - 总表数量: {len(all_tables)}")
    print(f"  - 节点数量: {len(all_nodes)}")
    print(f"  - 依赖关系数量: {sum(len(v) for v in node_to_children.values())}")
    
    # 第二遍：构建完整的祖先/后代关系
    print(f"\n构建完整依赖关系...")
    
    # 计算所有后代节点（向下递归）
    for node in all_nodes:
        visited = set()
        queue = deque([node])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            
            if current in node_to_children:
                for child in node_to_children[current]:
                    if child not in visited:
                        queue.append(child)
                        node_to_all_children[node].add(child)
    
    # 计算所有祖先节点（向上递归）
    for table in all_tables:
        visited = set()
        queue = deque([table])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            
            if current in child_to_parents:
                for parent in child_to_parents[current]:
                    if parent not in visited:
                        queue.append(parent)
                        child_to_all_parents[table].add(parent)
    
    print(f"完整关系统计:")
    print(f"  - 平均后代数量: {np.mean([len(v) for v in node_to_all_children.values()]):.1f}")
    print(f"  - 平均祖先数量: {np.mean([len(v) for v in child_to_all_parents.values()]):.1f}")
    
    return {
        'node_to_children': node_to_children,
        'node_to_all_children': node_to_all_children,
        'child_to_parents': child_to_parents,
        'child_to_all_parents': child_to_all_parents,
        'all_tables': all_tables,
        'all_nodes': all_nodes
    }

def mark_invalid_tables(df_a, df_b, graph):
    """
    标记无法匹配的表并向上传播影响
    """
    print("\n" + "=" * 60)
    print("标记无法匹配的表")
    print("=" * 60)
    
    # 获取白名单
    tbl_nm_list = df_b['TBL_NM'].dropna().astype(str).unique().tolist()
    
    # 创建标记状态字典
    table_status = {}
    for table in graph['all_tables']:
        # 获取当前标记状态
        if table in df_a['SOURCE_TABLE_NAME'].values:
            marks = df_a[df_a['SOURCE_TABLE_NAME'] == table]['SOURCE_TABLE_NAME_FLAG'].unique()
        elif table in df_a['NODE_NAME'].values:
            marks = df_a[df_a['NODE_NAME'] == table]['NODE_NAME_FLAG'].unique()
        else:
            marks = ['']
        
        # 取非空标记
        non_empty = [m for m in marks if m != '']
        table_status[table] = non_empty[0] if non_empty else ''
    
    # 第一轮：识别无法匹配的叶子节点
    invalid_tables = set()
    for table in graph['all_tables']:
        # 如果是叶子节点（不是节点）
        if table not in graph['all_nodes']:
            if table not in tbl_nm_list:  # 不在白名单中
                invalid_tables.add(table)
    
    print(f"第一轮识别无法匹配的叶子节点: {len(invalid_tables)} 个")
    if invalid_tables:
        print("无法匹配的叶子节点:")
        for i, table in enumerate(sorted(invalid_tables)[:20], 1):
            print(f"  {i:2d}. {table}")
        if len(invalid_tables) > 20:
            print(f"  ... 还有 {len(invalid_tables) - 20} 个")
    
    # 第二轮：向上传播影响
    print(f"\n第二轮：向上传播影响")
    
    # 使用队列进行广度优先传播
    queue = deque(invalid_tables)
    processed = set(invalid_tables)
    
    while queue:
        current = queue.popleft()
        
        # 找到所有父节点
        parents = graph['child_to_parents'].get(current, set())
        
        for parent in parents:
            # 如果父节点已经被标记为无效，跳过
            if parent in processed:
                continue
            
            # 将父节点标记为无效
            invalid_tables.add(parent)
            processed.add(parent)
            queue.append(parent)
            
            print(f"  {parent}: 标记为无效 (因为子节点 {current} 无法匹配)")
    
    # 第三轮：将无效标记传播到所有祖先
    print(f"\n第三轮：传播到所有祖先")
    all_invalid_tables = set(invalid_tables)
    
    for table in invalid_tables:
        # 获取所有祖先节点
        ancestors = graph['child_to_all_parents'].get(table, set())
        all_invalid_tables.update(ancestors)
        
        if ancestors:
            print(f"  {table} 影响祖先节点: {len(ancestors)} 个")
            for ancestor in sorted(ancestors)[:5]:
                print(f"    - {ancestor}")
            if len(ancestors) > 5:
                print(f"    - ... 还有 {len(ancestors) - 5} 个")
    
    print(f"\n总计无效表数量: {len(all_invalid_tables)}")
    
    # 更新DataFrame标记
    print(f"\n更新DataFrame标记...")
    
    # 更新SOURCE_TABLE_NAME_FLAG
    for table in all_invalid_tables:
        # 更新作为来源表的行
        source_mask = df_a['SOURCE_TABLE_NAME'] == table
        if source_mask.any():
            df_a.loc[source_mask, 'SOURCE_TABLE_NAME_FLAG'] = ''
            print(f"  表 {table}: 作为来源表的 {source_mask.sum()} 行标记为空")
        
        # 更新作为节点的行
        node_mask = df_a['NODE_NAME'] == table
        if node_mask.any():
            df_a.loc[node_mask, 'SOURCE_TABLE_NAME_FLAG'] = ''
            df_a.loc[node_mask, 'NODE_NAME_FLAG'] = ''
            print(f"  节点 {table}: {node_mask.sum()} 行标记为空")
    
    return df_a, all_invalid_tables

def iterative_marking(df_a, df_b):
    """
    迭代补充标记阶段：处理正常的下钻匹配
    """
    print("\n" + "=" * 60)
    print("阶段二：迭代补充标记阶段")
    print("=" * 60)
    
    # 构建依赖图
    graph = build_dependency_graph(df_a)
    
    # 首先标记无效表
    df_a, invalid_tables = mark_invalid_tables(df_a, df_b, graph)
    
    # 获取白名单
    tbl_nm_list = df_b['TBL_NM'].dropna().astype(str).unique().tolist()
    
    # 创建标记状态字典（排除无效表）
    table_status = {}
    for table in graph['all_tables']:
        if table in invalid_tables:
            table_status[table] = ''  # 无效表保持为空
            continue
            
        # 获取当前标记状态
        if table in df_a['SOURCE_TABLE_NAME'].values:
            marks = df_a[df_a['SOURCE_TABLE_NAME'] == table]['SOURCE_TABLE_NAME_FLAG'].unique()
        elif table in df_a['NODE_NAME'].values:
            marks = df_a[df_a['NODE_NAME'] == table]['NODE_NAME_FLAG'].unique()
        else:
            marks = ['']
        
        non_empty = [m for m in marks if m != '']
        table_status[table] = non_empty[0] if non_empty else ''
    
    print(f"\n开始迭代标记（排除 {len(invalid_tables)} 个无效表）...")
    
    # 迭代标记
    max_iterations = 20
    total_changed = 0
    
    for iteration in range(1, max_iterations + 1):
        print(f"\n--- 迭代 {iteration} ---")
        
        # 找出当前未标记且不是无效表的节点
        unmarked_nodes = [
            node for node in graph['all_nodes'] 
            if table_status.get(node, '') == '' 
            and node not in invalid_tables
        ]
        
        if not unmarked_nodes:
            print("所有有效节点都已标记，迭代结束")
            break
        
        print(f"未标记的有效节点数量: {len(unmarked_nodes)}")
        
        changed_this_iteration = 0
        
        for node in unmarked_nodes:
            # 获取直接子节点
            children = graph['node_to_children'].get(node, set())
            
            if not children:
                # 没有子节点，检查是否在白名单中
                if node in tbl_nm_list:
                    table_status[node] = 1
                    changed_this_iteration += 1
                    
                    # 更新DataFrame
                    mask = df_a['NODE_NAME'] == node
                    df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'] = 1
                    df_a.loc[mask, 'NODE_NAME_FLAG'] = 1
                    
                    print(f"  {node}: 标记为1 (叶子节点，在白名单中)")
                continue
            
            # 检查所有子节点的状态
            child_statuses = []
            for child in children:
                status = table_status.get(child, '')
                child_statuses.append(status)
            
            # 判断条件
            all_marked = all(status == 1 for status in child_statuses if status != '')
            some_unmarked = any(status == '' for status in child_statuses)
            
            if all_marked and not some_unmarked:
                # 所有子节点都已标记为1
                table_status[node] = 1
                changed_this_iteration += 1
                
                # 更新DataFrame
                mask = df_a['NODE_NAME'] == node
                df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'] = 1
                df_a.loc[mask, 'NODE_NAME_FLAG'] = 1
                
                print(f"  {node}: 标记为1 (所有子节点都为1)")
        
        total_changed += changed_this_iteration
        print(f"本轮标记了 {changed_this_iteration} 个节点")
        
        if changed_this_iteration == 0:
            print("本轮没有新的标记产生，迭代结束")
            break
    
    print(f"\n迭代完成，共标记 {total_changed} 个有效节点")
    iterative_stats(df_a)
    
    return df_a, invalid_tables

def hierarchical_propagation(df_a):
    """
    层级传递标记阶段：最终整理标记结果
    """
    print("\n" + "=" * 60)
    print("阶段三：层级传递标记阶段")
    print("=" * 60)
    
    # 清理标记：确保一致性
    print(f"\n清理标记确保一致性...")
    
    # 对于每个NODE_NAME，确保所有行的标记一致
    nodes_processed = 0
    for node_name in df_a['NODE_NAME'].unique():
        mask = df_a['NODE_NAME'] == node_name
        
        # 获取该节点的所有标记
        source_marks = df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'].unique()
        node_marks = df_a.loc[mask, 'NODE_NAME_FLAG'].unique()
        
        # 如果节点标记有空值，将整个节点置为空
        if '' in node_marks:
            df_a.loc[mask, 'SOURCE_TABLE_NAME_FLAG'] = ''
            df_a.loc[mask, 'NODE_NAME_FLAG'] = ''
            nodes_processed += 1
    
    print(f"清理了 {nodes_processed} 个节点的标记")
    
    final_stats(df_a)
    return df_a

def generate_detailed_report(df_a, df_b, invalid_tables, graph):
    """
    生成详细的报告
    """
    print("\n" + "=" * 60)
    print("生成详细报告")
    print("=" * 60)
    
    report_lines = []
    
    report_lines.append("=" * 80)
    report_lines.append("数据标记处理详细报告")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # 1. 基本统计
    report_lines.append("一、基本统计信息")
    report_lines.append("-" * 40)
    report_lines.append(f"总数据行数: {len(df_a)}")
    report_lines.append(f"白名单表数量: {len(df_b)}")
    report_lines.append(f"总表数量: {len(graph['all_tables'])}")
    report_lines.append(f"总节点数量: {len(graph['all_nodes'])}")
    report_lines.append("")
    
    # 2. 无法匹配的表详细分析
    report_lines.append("二、无法匹配的表分析")
    report_lines.append("-" * 40)
    report_lines.append(f"无法匹配的表总数: {len(invalid_tables)}")
    
    # 分类统计
    invalid_nodes = [t for t in invalid_tables if t in graph['all_nodes']]
    invalid_leaves = [t for t in invalid_tables if t not in graph['all_nodes']]
    
    report_lines.append(f"无法匹配的节点: {len(invalid_nodes)} 个")
    report_lines.append(f"无法匹配的叶子表: {len(invalid_leaves)} 个")
    report_lines.append("")
    
    # 列出无法匹配的叶子表
    if invalid_leaves:
        report_lines.append("无法匹配的叶子表列表:")
        for i, table in enumerate(sorted(invalid_leaves)[:50], 1):
            report_lines.append(f"  {i:3d}. {table}")
        if len(invalid_leaves) > 50:
            report_lines.append(f"  ... 还有 {len(invalid_leaves) - 50} 个")
        report_lines.append("")
    
    # 3. 受影响的上游节点分析
    report_lines.append("三、受影响的上游节点")
    report_lines.append("-" * 40)
    
    # 对于每个无法匹配的叶子表，找出所有受影响的节点
    leaf_to_affected_nodes = {}
    
    for leaf in invalid_leaves:
        # 获取所有祖先节点
        ancestors = graph['child_to_all_parents'].get(leaf, set())
        if ancestors:
            leaf_to_affected_nodes[leaf] = ancestors
    
    # 统计影响范围
    total_affected_nodes = set()
    for ancestors in leaf_to_affected_nodes.values():
        total_affected_nodes.update(ancestors)
    
    report_lines.append(f"受影响的节点总数: {len(total_affected_nodes)}")
    report_lines.append("")
    
    # 按影响范围排序
    leaf_impact = []
    for leaf, ancestors in leaf_to_affected_nodes.items():
        leaf_impact.append((leaf, len(ancestors)))
    
    leaf_impact.sort(key=lambda x: x[1], reverse=True)
    
    if leaf_impact:
        report_lines.append("影响范围最大的叶子表:")
        for leaf, impact_count in leaf_impact[:20]:
            report_lines.append(f"  {leaf}: 影响 {impact_count} 个上游节点")
        
        # 详细列出影响最大的前几个
        if leaf_impact:
            top_leaf, top_count = leaf_impact[0]
            report_lines.append("")
            report_lines.append(f"影响最大的叶子表 '{top_leaf}' 影响的上游节点:")
            ancestors = leaf_to_affected_nodes[top_leaf]
            for i, node in enumerate(sorted(ancestors)[:30], 1):
                # 检查节点是否在B表中
                in_b = " (在B表中)" if node in df_b['TBL_NM'].values else ""
                report_lines.append(f"  {i:3d}. {node}{in_b}")
            if len(ancestors) > 30:
                report_lines.append(f"  ... 还有 {len(ancestors) - 30} 个")
    
    # 4. 标记结果统计
    report_lines.append("")
    report_lines.append("四、最终标记结果统计")
    report_lines.append("-" * 40)
    
    source_1_count = (df_a['SOURCE_TABLE_NAME_FLAG'] == 1).sum()
    source_empty = (df_a['SOURCE_TABLE_NAME_FLAG'] == '').sum()
    
    node_1_count = (df_a['NODE_NAME_FLAG'] == 1).sum()
    node_empty = (df_a['NODE_NAME_FLAG'] == '').sum()
    
    report_lines.append(f"SOURCE_TABLE_NAME标记:")
    report_lines.append(f"  标记为1: {source_1_count} 行 ({source_1_count/len(df_a)*100:.1f}%)")
    report_lines.append(f"  未标记: {source_empty} 行 ({source_empty/len(df_a)*100:.1f}%)")
    report_lines.append("")
    
    report_lines.append(f"NODE_NAME标记:")
    report_lines.append(f"  标记为1: {node_1_count} 行 ({node_1_count/len(df_a)*100:.1f}%)")
    report_lines.append(f"  未标记: {node_empty} 行 ({node_empty/len(df_a)*100:.1f}%)")
    
    # 5. 示例数据
    report_lines.append("")
    report_lines.append("五、标记结果示例")
    report_lines.append("-" * 40)
    
    # 找出有变化的节点
    changed_nodes = []
    for node in graph['all_nodes']:
        mask = df_a['NODE_NAME'] == node
        node_mark = df_a.loc[mask, 'NODE_NAME_FLAG'].iloc[0] if mask.any() else ''
        
        # 如果节点在B表中但最终标记为空，记录下来
        if node in df_b['TBL_NM'].values and node_mark == '':
            changed_nodes.append(node)
    
    if changed_nodes:
        report_lines.append(f"在B表中但最终标记为空的节点 ({len(changed_nodes)} 个):")
        for i, node in enumerate(changed_nodes[:20], 1):
            report_lines.append(f"  {i:2d}. {node}")
        if len(changed_nodes) > 20:
            report_lines.append(f"  ... 还有 {len(changed_nodes) - 20} 个")
    else:
        report_lines.append("没有在B表中但最终标记为空的节点")
    
    # 保存报告
    report_file = "详细标记分析报告.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"详细报告已保存到: {report_file}")
    
    # 打印关键信息
    print(f"\n关键统计:")
    print(f"  - 无法匹配的表: {len(invalid_tables)} 个")
    print(f"  - 受影响的节点: {len(total_affected_nodes)} 个")
    print(f"  - 最终标记为1的节点: {node_1_count} 行")
    print(f"  - 在B表中但最终标记为空的节点: {len(changed_nodes)} 个")

def main():
    # 读取Excel文件
    print("数据标记处理系统 (强制空标记传播版)")
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
        
        # 构建完整的依赖图（提前构建，供后续使用）
        graph = build_dependency_graph(df_a)
        
        # 阶段二：迭代补充标记（包含强制空标记）
        df_a, invalid_tables = iterative_marking(df_a, df_b)
        
        # 阶段三：层级传递标记
        df_a = hierarchical_propagation(df_a)
        
        # 保存结果
        output_file = "标记结果_强制空标记.xlsx"
        df_a.to_excel(output_file, index=False)
        print(f"\n标记结果已保存到: {output_file}")
        
        # 生成详细报告
        generate_detailed_report(df_a, df_b, invalid_tables, graph)
        
    except FileNotFoundError as e:
        print(f"文件读取错误: {e}")
        print("请确保文件路径正确，并且文件存在于指定位置")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()

def test_aa_ac_ac2_scenario():
    """测试AA/AC/AC2场景：AA在B中，但AC2无法匹配"""
    print("\n" + "=" * 60)
    print("AA/AC/AC2场景测试")
    print("=" * 60)
    
    # 创建测试数据
    test_data = [
        # AA节点在B表中，依赖AC
        {'NODE_NAME': 'AA', 'BLOCK_NUM': 1, 'STATMENT_NUM': 1, 
         'SOURCE_TABLE_NAME': 'AC', 'SOURCE_TABLE_NAME_FLAG': '', 'NODE_NAME_FLAG': ''},
        
        # AC节点，依赖AC2
        {'NODE_NAME': 'AC', 'BLOCK_NUM': 2, 'STATMENT_NUM': 1, 
         'SOURCE_TABLE_NAME': 'AC2', 'SOURCE_TABLE_NAME_FLAG': '', 'NODE_NAME_FLAG': ''},
        
        # AC2叶子节点，不在B表中，也没有下游
        {'NODE_NAME': 'AC2', 'BLOCK_NUM': 3, 'STATMENT_NUM': 1, 
         'SOURCE_TABLE_NAME': 'LEAF', 'SOURCE_TABLE_NAME_FLAG': '', 'NODE_NAME_FLAG': ''},
    ]
    
    test_df = pd.DataFrame(test_data)
    
    # B表白名单
    b_data = {'TBL_NM': ['AA']}  # 只有AA在B中，AC和AC2不在
    test_b = pd.DataFrame(b_data)
    
    print("测试数据:")
    print(test_df[['NODE_NAME', 'SOURCE_TABLE_NAME']].to_string(index=False))
    print("\n白名单表B:")
    print(test_b['TBL_NM'].to_string(index=False))
    
    print("\n期望结果:")
    print("  1. AA在B表中，初始标记为1")
    print("  2. AC2是叶子节点且不在B表中，识别为无法匹配")
    print("  3. AC因为依赖AC2，强制标记为空")
    print("  4. AA因为依赖AC（已被强制为空），强制标记为空")
    print("  最终：AA、AC、AC2都标记为空")
    
    # 执行标记
    print("\n执行标记流程...")
    result_df = test_df.copy()
    result_df = initial_marking(result_df, test_b)
    
    # 构建依赖图
    graph = build_dependency_graph(result_df)
    
    # 迭代标记
    result_df, invalid_tables = iterative_marking(result_df, test_b)
    result_df = hierarchical_propagation(result_df)
    
    print("\n最终标记结果:")
    print(result_df[['NODE_NAME', 'SOURCE_TABLE_NAME', 'SOURCE_TABLE_NAME_FLAG', 'NODE_NAME_FLAG']].to_string(index=False))
    
    # 验证结果
    print("\n验证结果:")
    
    expected_results = {
        'AA': '',  # 应该在B表中但被强制为空
        'AC': '',  # 因为依赖AC2而为空
        'AC2': '', # 叶子节点不在B表中
    }
    
    for node, expected in expected_results.items():
        node_mask = result_df['NODE_NAME'] == node
        if node_mask.any():
            actual = result_df.loc[node_mask, 'NODE_NAME_FLAG'].iloc[0]
            status = "✓" if actual == expected else "✗"
            print(f"  {status} {node}: 期望={expected}, 实际={actual}")
        else:
            print(f"  ✗ {node}: 在结果中未找到")

if __name__ == "__main__":
    # 测试AA/AC/AC2场景
    test_aa_ac_ac2_scenario()
    
    print("\n" + "=" * 60)
    print("开始处理主数据")
    print("=" * 60)
    
    # 主处理流程
    main()