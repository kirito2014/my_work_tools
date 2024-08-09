import os
import pandas as pd
import xlwings as xw

def process_code_mapping(table_list_file):
    # 获取人员名称
    person_name = os.path.splitext(os.path.basename(table_list_file))[0].split('-')[-1]

    # 日志文件
    error_log = f"error_log_{person_name}.txt"

    # 创建/打开目标文件pub_cd_map.xlsx
    target_file = 'pub_cd_map.xlsx'
    if os.path.exists(target_file):
        tgt_wb = xw.Book(target_file)
    else:
        tgt_wb = xw.Book()
        tgt_wb.save(target_file)

    # 读取表名列表
    with open(table_list_file, 'r', encoding='utf-8') as file:
        table_names = [line.strip().upper() for line in file.readlines()]

    # 处理每个表名
    for table_name in table_names:
        # 处理不同格式的代码映射文件
        code_map_files = [
            os.path.join('pub_cd_map', f'code_map-{person_name}.xlsx'),
            os.path.join('pub_cd_map', f'code_map-{person_name}.xls'),
            os.path.join('pub_cd_map', f'code_map-{person_name}.xlsm')
        ]

        code_map_file = next((file for file in code_map_files if os.path.exists(file)), None)

        if not code_map_file:
            log_error(error_log, f"Code mapping file for {person_name} not found.")
            continue

        try:
            # 打开源代码映射文件
            src_wb = xw.Book(code_map_file)
            rem_code_map_sheet = 'rem-代码映射'

            if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
                log_error(error_log, f"Code mapping sheet not found in {code_map_file}.")
                src_wb.close()
                continue

            src_cm_sheet = src_wb.sheets[rem_code_map_sheet]
            if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]:
                tgt_wb.sheets.add(rem_code_map_sheet)
            tgt_cm_sheet = tgt_wb.sheets[rem_code_map_sheet]

            # 删除目标文件中的代码映射相关内容
            tgt_cm_data = tgt_cm_sheet.range('A1').expand('table').value
            if tgt_cm_data:
                tgt_cm_df = pd.DataFrame(tgt_cm_data[1:], columns=tgt_cm_data[0])
                tgt_cm_df = tgt_cm_df[tgt_cm_df['表英文名'] != table_name]
                tgt_cm_sheet.range('A1').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()

            # 读取并筛选源代码映射数据
            src_cm_data = src_cm_sheet.range('A1').expand('table').value
            if src_cm_data:
                src_cm_df = pd.DataFrame(src_cm_data[1:], columns=src_cm_data[0])
                filtered_src_cm_df = src_cm_df[src_cm_df['表英文名'] == table_name]

                if filtered_src_cm_df.empty:
                    log_error(error_log, f"Code mapping not found for table {table_name} in {code_map_file}.")
                else:
                    # 将筛选出的数据追加到目标工作表
                    start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                    tgt_cm_sheet.range(f'A{start_row}').value = filtered_src_cm_df.values.tolist()

            src_wb.close()

        except Exception as e:
            log_error(error_log, f"Error processing {table_name} in {code_map_file}: {str(e)}")

    tgt_wb.save(target_file)
    tgt_wb.close()

def log_error(log_file, message):
    with open(log_file, 'a', encoding='utf-8') as log:
        log.write(message + '\n')
    print(message)

if __name__ == "__main__":
    table_list_file = "table_list-XXX.txt"  # 将其替换为实际的文件名
    process_code_mapping(table_list_file)
