import os,sys
import shutil
import openpyxl
import xlwings as xw
import pandas as pd

def copy_data_to_target(folder_path,target_file):
    if not os.path.isfile(target_file):
        print(f"[ ERROR ] 目标文件未找到:<{os.path.basename(target_file)}>.")
        return

    app = xw.App(visible=False)
    tgt_wb = app.books.open(target_file)
    tgt_sht = tgt_wb.sheets['模板字段']


    last_row = tgt_sht.range('A' + str(tgt_sht.cells.last_cell.row)).end('up').row
    if last_row >=3:
        tgt_sht.range(f"3:{last_row}").clear_contents()
        print(f"[ INFO ] 清除目标表的存量数据完成.")

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path,filename)

        if not filename.endswith('.xls') and not filename.endswith('.xlsx') and not filename.endswith('.xlsm'):
            continue
        df = pd.read_excel(file_path,sheet_name='模板字段',skiprows=1)
        #print(df)
        if df.empty:
            print(f"[ ERROR ] 复制文件内容< {filename} > 无数据请检查.")
            continue

        last_row = tgt_sht.range('A' + str(tgt_sht.cells.last_cell.row)).end('up').row
        tgt_sht.range(f'A{last_row + 1}').value = df.values
        print(f"[SUCCESS] 复制文件内容< {filename} >到:<{os.path.basename(target_file)}> 成功.")
    
    tgt_wb.save()
    app.quit()
    print(f"[ INFO ] 所有文件处理完成.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
            print("Usage: python merge_chk_file.py <folder_path> <target_file>")
            sys.exit(1)
    folder_path = sys.argv[1]
    target_file = sys.argv[2]
    copy_data_to_target(folder_path,target_file)