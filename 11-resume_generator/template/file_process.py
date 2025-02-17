import pandas as pd
import openpyxl

def backup_sheet(wb, sheet_name):
    """备份原始 sheet 页"""
    original_sheet = wb[sheet_name]
    backup_sheet_name = f"备份_{sheet_name}"
    if backup_sheet_name not in wb.sheetnames:
        backup_sheet = wb.copy_worksheet(original_sheet)
        backup_sheet.title = backup_sheet_name
    return backup_sheet_name

def move_columns(df):
    """将指定列移动到 '个人简介（必填）' 列的后方"""
    columns_to_move = ['业务与技术能力详述（必填）', '资质认证', '参与培训', '技能标签（必填）']
    columns = df.columns.tolist()
    for col in columns_to_move:
        columns.remove(col)
    insert_index = columns.index('个人简介（必填）') + 1
    for col in columns_to_move:
        columns.insert(insert_index, col)
    df = df[columns]
    return df

def modify_headers(df):
    """修改标题行"""
    new_headers = [
        '填写人', '所在部门', '填写时间', '用户类型', '基本情况-姓名', '基本情况-工作年限', '基本情况-最高学历',
        '基本情况-最高学历-毕业日期', '基本情况-最高学历-毕业学校', '基本情况-最高学历-专业', '基本情况-第一学历-毕业日期',
        '基本情况-第一学历-毕业学校', '基本情况-第一学历-专业', '基本情况-部门', '基本情况-职称', '基本情况-个人简介',
        '业务与技术能力详述', '资质认证', '参与培训', '技能标签', '工作经历-开始时间', '工作经历-结束时间',
        '工作经历-公司名称', '工作经历-担任职务', '工作经历-工作职责说明', '项目经历-开始时间', '项目经历-结束时间',
        '项目经历-项目名称', '项目经历-项目角色', '项目经历-项目职责说明', '项目经历-开始时间', '项目经历-结束时间',
        '项目经历-项目名称', '项目经历-项目角色', '项目经历-项目职责说明', '项目经历-开始时间', '项目经历-结束时间',
        '项目经历-项目名称', '项目经历-项目角色', '项目经历-项目职责说明', '项目经历-开始时间', '项目经历-结束时间',
        '项目经历-项目名称', '项目经历-项目角色', '项目经历-项目职责说明', '工作经历-开始时间', '工作经历-结束时间',
        '工作经历-公司名称', '工作经历-担任职务', '工作经历-工作职责说明', '基本情况-第2学历-毕业日期',
        '基本情况-第2学历-毕业学校', '基本情况-第2学历-专业', '基本情况-第3学历-毕业日期', '基本情况-第3学历-毕业学校',
        '基本情况-第3学历-专业'
    ]
    df.columns = new_headers
    return df

def remove_duplicates(file_path, sheet_name):
    """按人员分组，根据填写时间保留最新的记录，并处理合并单元格"""
    # 使用 openpyxl 读取 Excel 文件
    wb = openpyxl.load_workbook(file_path)
    ws = wb[sheet_name]

    # 获取所有合并单元格的范围
    merged_cells = ws.merged_cells.ranges

    # 读取数据到 DataFrame
    data = ws.values
    headers = next(data)
    df = pd.DataFrame(data, columns=headers)

    # 将 '填写时间' 列转换为 datetime 类型
    df['填写时间'] = pd.to_datetime(df['填写时间'])

    # 按 '填写人' 分组，找到每个组的最新记录
    latest_records = df.loc[df.groupby('填写人')['填写时间'].idxmax()]

    # 找到需要删除的行
    rows_to_delete = set()
    for index, row in df.iterrows():
        name = row['填写人']
        # 检查是否有匹配的记录
        matching_records = latest_records.loc[latest_records['填写人'] == name, '填写时间'].values
        if len(matching_records) > 0:
            latest_time = matching_records[0]
            if row['填写时间'] != latest_time:
                rows_to_delete.add(index + 2)  # +2 因为 Excel 行号从 1 开始，且跳过了标题行

    # 删除非最新记录的行（从后往前删除，避免行号变化）
    for row_idx in sorted(rows_to_delete, reverse=True):
        ws.delete_rows(row_idx)

    # 保存修改后的文件
    wb.save(file_path)

def process_file(file_path, sheet_name):
    """处理文件"""
    # 读取 Excel 文件
    wb = openpyxl.load_workbook(file_path)
    ws = wb[sheet_name]

    # 备份原始 sheet 页
    backup_sheet(wb, sheet_name)

    # 记录合并单元格的范围
    merged_ranges = [mcr.coord for mcr in ws.merged_cells.ranges]

    # 取消合并单元格
    for merged_range in merged_ranges:
        ws.unmerge_cells(merged_range)

    # 读取数据到 DataFrame
    data = ws.values
    headers = next(data)
    df = pd.DataFrame(data, columns=headers)

    # 移动列
    df = move_columns(df)

    # 修改标题行
    df = modify_headers(df)

    # 清空原有数据
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            cell.value = None

    # 将修改后的 DataFrame 写回 Excel 文件
    for r_idx, row in enumerate(df.itertuples(index=False), start=2):
        for c_idx, value in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx, value=value)

    # 重新合并单元格
    for merged_range in merged_ranges:
        ws.merge_cells(merged_range)

    # 保存修改后的文件
    wb.save(file_path)

    # 去重操作
    remove_duplicates(file_path, sheet_name)

if __name__ == "__main__":
    file_path = "人员简历20250217.xlsx"  # 替换为你的文件路径
    sheet_name = "数据来源"  # 替换为你要操作的 sheet 页名称
    process_file(file_path, sheet_name)