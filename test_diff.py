import pandas as pd
import numpy as np

# 示例数据框架
data_b = {
    'group_no': [1, 2, 3],
    'ser_no': [1, 2, 3],
    'column_1': ['a1', 'b1', 'c\n'],
    'column_2': ['d\n', 'e1', 'f1n'],
    '_original_index': [1, 3, 5],
}
data_a = {
    'group_no': [1, 2, 3],
    'ser_no': [1, 2, 3],
    'column_1': ['a\n', 'b2', 'c\n'],
    'column_2': ['dn', 'e1', 'f\n'],
    '_original_index': [1, 2, 7],
}

diff_in_b = pd.DataFrame(data_b)
diff_in_a = pd.DataFrame(data_a)

# 存储差异信息的列表
diff_list_info = []
diff_list = []

print(diff_in_b)
print(diff_in_a)

# 循环行
for idx in diff_in_b.index:
    group_no = diff_in_b.at[idx, 'group_no']
    ser_no = diff_in_b.at[idx, 'ser_no']
    #_original_index = diff_in_b.at[idx, '_original_index']
    
    diff_list_info.append((idx, group_no, ser_no))
    
    for col in diff_in_b.columns:
        if col in ['group_no', 'ser_no', '_original_index']:
            continue
        
        old_value = diff_in_a.at[idx, col]
        new_value = diff_in_b.at[idx, col]
        
        if pd.isna(old_value) and pd.isna(new_value):
            continue
        
        old_value_str = str(old_value).replace('\n', ' ')
        new_value_str = str(new_value).replace('\n', ' ')
        
        if old_value_str != new_value_str:
            diff_list.append((idx, old_value_str, new_value_str))

# 按照指定格式展示结果
output = []

for info in diff_list_info:
    idx, group_no, ser_no = info
    output.append(f"{idx +1 } UNIONALL{group_no} {ser_no}:")
    
    for diff in diff_list:
        diff_idx, old_value, new_value = diff
        if diff_idx == idx:
            output.append(f"\t{diff_idx + 1}: {old_value} -> {new_value};")
            
output_str = "\n".join(output) 
print(output_str)
