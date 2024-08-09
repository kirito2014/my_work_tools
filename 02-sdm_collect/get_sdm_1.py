import tkinter as tk
from tkinter import filedialog, messagebox
import os
import pandas as pd
import xlwings as xw

# 上面的代码封装成一个函数
def copy_sheets_and_metadata(source_file, target_file):
    # 初始化错误列表
    error_list = []

    # 打开 Excel 文件
    excel_app = xw.App(visible=False)
    
    try:
        src_wb = xw.Book(source_file)
        if os.path.exists(target_file):
            tgt_wb = xw.Book(target_file)
        else:
            tgt_wb = xw.Book()
            tgt_wb.save(target_file)

        # 获取 Index 表中所有标记为 Y 的行
        index_data = pd.read_excel(source_file, sheet_name='index', usecols="C,D,M")
        index_data = index_data.iloc[1:]
        index_data.columns = ['table_id', 'table_name', 'enable_flag']
        index_data['table_id'] = index_data['table_id'].apply(lambda x: x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']

        # 计算标记为 Y 的数量
        y_count = filtered_index.shape[0]
        log_message(f"Number of sheets marked as 'Y': {y_count}")

        # 生成并打印 table_name_list
        table_name_list = [f"{i+1}、{row['table_name']}" for i, row in filtered_index.iterrows()]
        log_message(f"Table names list: {', '.join(table_name_list)}")

        for _, row in filtered_index.iterrows():
            table_id = row['table_id']
            table_name = row['table_name']

            # 检查源文件中是否存在对应的 sheet 页
            if table_id not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Sheet {table_id} not found in source file.")
                continue

            # 删除目标文件中的同名 sheet 页
            if table_id in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets[table_id].delete()

            # 复制 sheet 页内容
            src_sheet = src_wb.sheets[table_id]
            src_sheet.api.Copy(Before=tgt_wb.sheets[0].api)
            tgt_wb.sheets[0].name = table_id

            # 复制数据字典
            rem_data_dict_sheet = 'rem-数据字典'
            if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Data dictionary sheet not found for table {table_name}.")
            else:
                src_dd_sheet = src_wb.sheets[rem_data_dict_sheet]
                if rem_data_dict_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                    tgt_wb.sheets.add(rem_data_dict_sheet)
                tgt_dd_sheet = tgt_wb.sheets[rem_data_dict_sheet]

                # 删除目标文件中的数据字典相关内容
                tgt_dd_data = tgt_dd_sheet.range('B2').expand('table').value
                if tgt_dd_data:
                    tgt_dd_df = pd.DataFrame(tgt_dd_data[1:], columns=tgt_dd_data[0])
                    tgt_dd_df = tgt_dd_df[tgt_dd_df['表英文名'] != table_name]
                    tgt_dd_sheet.clear_contents()
                    tgt_dd_sheet.range('B2').value = [tgt_dd_data[0]] + tgt_dd_df.values.tolist()

                data_dict_found = False
                for row in src_dd_sheet.range('A2').expand('table').value:
                    if row[1] == table_name:
                        data_dict_found = True
                        tgt_dd_sheet.range('A' + str(tgt_dd_sheet.range('A' + str(tgt_dd_sheet.cells.last_cell.row)).end('up').row + 1)).value = row

                if not data_dict_found:
                    error_list.append(f"Data dictionary not found for table {table_name}.")

            # 复制代码映射
            rem_code_map_sheet = 'rem-代码映射'
            if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
                error_list.append(f"Code mapping sheet not found for table {table_name}.")
            else:
                src_cm_sheet = src_wb.sheets[rem_code_map_sheet]
                if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                    tgt_wb.sheets.add(rem_code_map_sheet)
                tgt_cm_sheet = tgt_wb.sheets[rem_code_map_sheet]

                # 删除目标文件中的代码映射相关内容
                tgt_cm_data = tgt_cm_sheet.range('I3').expand('table').value
                if tgt_cm_data:
                    tgt_cm_df = pd.DataFrame(tgt_cm_data[1:], columns=tgt_cm_data[0])
                    tgt_cm_df = tgt_cm_df[tgt_cm_df['表英文名'] != table_name]
                    tgt_cm_sheet.clear_contents()
                    tgt_cm_sheet.range('I3').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()

                code_map_found = False
                for row in src_cm_sheet.range('A3').expand('table').value:
                    if row[8] == table_name:
                        code_map_found = True
                        tgt_cm_sheet.range('A' + str(tgt_cm_sheet.range('A' + str(tgt_cm_sheet.cells.last_cell.row)).end('up').row + 1)).value = row

                if not code_map_found:
                    error_list.append(f"Code mapping not found for table {table_name}.")

        # 保存并关闭目标文件
        tgt_wb.save(target_file)
        tgt_wb.close()
        src_wb.close()
    except Exception as e:
        error_list.append(str(e))
    finally:
        excel_app.quit()

    # 返回错误列表
    return error_list

# 日志信息处理
def log_message(message):
    log_text.config(state=tk.NORMAL)
    log_text.insert(tk.END, message + '\n')
    log_text.config(state=tk.DISABLED)
    log_text.yview(tk.END)

# 选择源文件
def select_source_file():
    global source_file
    source_file = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if source_file:
        source_label.config(text=os.path.basename(source_file), fg='green')
    else:
        source_label.config(text="请选择合并文件", fg='red')

# 选择目标文件
def select_target_file():
    global target_file
    target_file = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
    if target_file:
        target_label.config(text=os.path.basename(target_file), fg='green')
    else:
        target_label.config(text="请选择目标文件", fg='red')

# 确认合并
def confirm_merge():
    if not source_file or not target_file:
        messagebox.showerror("错误", "请选择源文件和目标文件")
        return

    log_message("开始合并...")
    errors = copy_sheets_and_metadata(source_file, target_file)
    if errors:
        log_message(f"Errors: {errors}")
    else:
        log_message("合并完成")
        
# 清除日志信息
def clear_log():
    log_text.config(state=tk.NORMAL)
    log_text.delete(1.0, tk.END)
    log_text.config(state=tk.DISABLED)

# 主窗口
root = tk.Tk()
root.title("Excel合并工具")

# 文件选择行
frame_files = tk.Frame(root)
frame_files.pack(pady=10)

source_button = tk.Button(frame_files, text="选择要合并的文件", command=select_source_file, width=25)
source_button.pack(side=tk.LEFT, padx=5)

target_button = tk.Button(frame_files, text="选择目标文件", command=select_target_file, width=25)
target_button.pack(side=tk.LEFT, padx=5)

# 选中文件名显示行
frame_files_selected = tk.Frame(root)
frame_files_selected.pack(pady=5)

source_label = tk.Label(frame_files_selected, text="请选择合并文件", fg='red', width=25)
source_label.pack(side=tk.LEFT, padx=5)

target_label = tk.Label(frame_files_selected, text="请选择目标文件", fg='red', width=25)
target_label.pack(side=tk.LEFT, padx=5)

# 确认合并和清除日志行
frame_actions = tk.Frame(root)
frame_actions.pack(pady=10)

confirm_button = tk.Button(frame_actions, text="确认合并", command=confirm_merge, width=30)
confirm_button.pack(side=tk.LEFT, padx=5)

clear_button = tk.Button(frame_actions, text="清除日志信息", command=clear_log, width=10)
clear_button.pack(side=tk.LEFT, padx=5)

# 日志信息显示
log_text = tk.Text(root, state=tk.DISABLED, height=15, width=80)
log_text.pack(pady=10)

# 初始化文件路径
source_file = ""
target_file = ""

# 运行主循环
root.mainloop()
