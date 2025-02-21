import os,sys
import shutil
import openpyxl
import xlwings as xw
import pandas as pd
import xlrd

def copy_data_to_target(folder_path,target_file):
    try:
        if not os.path.isfile(target_file):
            print(f"[ ERROR ] 目标文件未找到:<{os.path.basename(target_file)}>.")
            return

        app = xw.App(visible=False)
        tgt_wb = app.books.open(target_file)
        count_sheet1 = '1-记录数检查'
        count_sheet2 = '1-记录数'
        if count_sheet2 not in [sheet.name for sheet in tgt_wb.sheets]:
            print(f"[ ERROR ] 目标文件未找到记录数检查sheet页")
        else:
            tgt_sht = tgt_wb.sheets[count_sheet2]


        last_row = tgt_sht.range('B' + str(tgt_sht.cells.last_cell.row)).end('up').row
        if last_row >3:
            tgt_sht.range(f"4:{last_row}").clear_contents()
            print(f"[ INFO ] 清除目标表的存量数据完成.")

        tgt_wb.save(target_file)

        for filename in os.listdir(folder_path):
            #print(filename)
            if filename.startswith('~$'):
                continue
            file_path = os.path.join(folder_path,filename)
            sheet_name = ''

            if not filename.endswith('.xls') and not filename.endswith('.xlsx') and not filename.endswith('.xlsm'):
                continue

            sheets = pd.ExcelFile(file_path).sheet_names
            if count_sheet1 in sheets:
                sheet_name =count_sheet1
            elif count_sheet2 in sheets:
                sheet_name =count_sheet2


            #print(f"当前sheet:{sheet_name}")

            df = pd.read_excel(file_path,sheet_name=sheet_name,skiprows=2,engine='openpyxl')
            #print(df)
            if df.empty:
                print(f"[ ERROR ] 复制文件内容< {filename} > 无数据请检查.")
                continue

            last_row = tgt_sht.range('B' + str(tgt_sht.cells.last_cell.row)).end('up').row
            #print(last_row)
            tgt_sht.range(f'A{last_row + 1}').value = df.values
            print(f"[SUCCESS] 复制文件内容< {filename} >到:<{os.path.basename(target_file)}> 成功.")
            tgt_wb.save(target_file)
    except Exception as e:
        print(f"error:{e}")
    finally:
        tgt_wb.save(target_file)
        tgt_wb.close()
        app.quit()
        print(f"[ INFO ] 所有文件处理完成.")
if __name__ == "__main__":
    if len(sys.argv) != 3:
            print("Usage: python merge_chk_file.py <folder_path> <target_file>")
            sys.exit(1)
    folder_path = sys.argv[1]
    target_file = sys.argv[2]
    copy_data_to_target(folder_path,target_file)