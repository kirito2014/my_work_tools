import os
import pandas as pd
from tkinter import Tk, filedialog, messagebox

def check_and_process_file(filepath, df_cases, df_sup, df_data, df_cust):
    # 打开Excel文件
    try:
        df_source = pd.read_excel(filepath, sheet_name='案例调研')
    except Exception as e:
        print(f"Error opening {filepath}: {e}")
        return

    # 检查A4单元格内容是否为空
    if not pd.isna(df_source.at[3, 'A']):
        messagebox.showwarning("格式错误", f"表格填写错误，请检查是否删除示例行。文件名：{filepath}")
        return

    # 将源表中的值复制到目标表中
    df_cases = df_cases.append({
        'A': df_source.at[2, 'A'],
        'B': df_source.at[2, 'B'],
        'C': df_source.at[2, 'C'],
        'D': df_source.at[2, 'D'],
        'E': df_source.at[2, 'E'],
        'J': df_source.at[2, 'H'],
        'K': df_source.at[2, 'I'],
        'L': df_source.at[2, 'J'],
        'N': df_source.at[16, 'H'],
        'O': df_source.at[17, 'H'],
        'P': df_source.at[18, 'H'],
        'Q': df_source.at[19, 'H'],
        'R': df_source.at[20, 'H'],
        'S': df_source.at[21, 'H'],
        'T': df_source.at[22, 'H'],
        'U': df_source.at[23, 'H'],
        'V': df_source.at[24, 'H'],
        'W': df_source.at[25, 'H'],
        'X': df_source.at[26, 'H'],
        'Y': df_source.at[27, 'H'],
        'Z': df_source.at[28, 'H'],
        'AA': df_source.at[29, 'H'],
        'AB': df_source.at[5, 'A'],
        'AC': df_source.at[2, 'L'],
        'AD': df_source.at[2, 'M'],
        'AE': f'=IFERROR(HYPERLINK("#\'通用补充信息\'!A"&MATCH(A{len(df_cases)+1}, \'通用补充信息\'!A:A, 0), ">>>通用补充信息<<<"), ">>>暂无补充<<<")',
        'AF': f'=IFERROR(HYPERLINK("#\'数据应用类补充信息\'!A"&MATCH(A{len(df_cases)+1}, \'数据应用类补充信息\'!A:A, 0), ">>>数据应用类补充信息<<<"), ">>>暂无补充<<<")',
        'AH': df_source.at[2, 'K'],
        'AI': df_source.at[2, 'F'],
    }, ignore_index=True)

    df_sup = df_sup.append({
        'A': df_source.at[2, 'A'],
        'B': df_source.at[34, 'A'],
        'C': df_source.at[34, 'B'],
        'D': df_source.at[34, 'C'],
        'E': df_source.at[34, 'D'],
        'F': df_source.at[34, 'E'],
        'G': df_source.at[34, 'F'],
        'H': df_source.at[34, 'G'],
        'I': df_source.at[34, 'H'],
        'J': df_source.at[34, 'I'],
        'K': df_source.at[34, 'J'],
        'L': df_source.at[34, 'K'],
        'M': df_source.at[34, 'L'],
        'N': df_source.at[34, 'M'],
        'P': df_source.at[40, 'A'],
        'Q': df_source.at[40, 'B'],
        'R': df_source.at[40, 'D'],
        'S': df_source.at[40, 'E'],
        'T': df_source.at[40, 'F'],
        'U': df_source.at[40, 'G'],
    }, ignore_index=True)

    df_data = df_data.append({
        'A': df_source.at[2, 'A'],
        'B': df_source.at[40, 'H'],
        'C': df_source.at[40, 'I'],
        'D': df_source.at[40, 'J'],
        'E': df_source.at[40, 'K'],
        'F': df_source.at[40, 'C'],
        'G': df_source.at[40, 'L'],
    }, ignore_index=True)

    df_cust = df_cust.append({
        'B': df_source.at[2, 'E']
    }, ignore_index=True)


def main():
    # 创建图形界面
    root = Tk()
    root.withdraw()
    messagebox.showinfo("选择文件夹", "请选择要处理的文件夹")

    # 选择要处理的文件夹
    folder_path = filedialog.askdirectory(title="选择要处理的文件夹")
    if not folder_path:
        return

    # 检查文件夹中是否存在文件
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    if not files:
        messagebox.showwarning("文件夹为空", "没有要处理的文件，请确认文件是否存在！")
        return

    # 创建目标表格
    df_cases = pd.DataFrame(columns=['A', 'B', 'C', 'D', 'E', 'J', 'K', 'L', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AH', 'AI'])
    df_sup = pd.DataFrame(columns=['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U'])
    df_data = pd.DataFrame(columns=['A', 'B', 'C', 'D', 'E', 'F', 'G'])
    df_cust = pd.DataFrame(columns=['B'])

    # 遍历文件夹中的所有文件并处理
    for file in files:
        check_and_process_file(file, df_cases, df_sup, df_data, df_cust)

    # 去重并删除空白单元格
    df_cust.drop_duplicates(subset=['B'], keep='first', inplace=True)
    df_cust.dropna(subset=['B'], inplace=True)

    # 保存处理后的数据
    with pd.ExcelWriter('1111.xlsx') as writer:
        df_cases.to_excel(writer, sheet_name='案例清单', index=False)
        df_sup.to_excel(writer, sheet_name='通用补充信息', index=False)
        df_data.to_excel(writer, sheet_name='数据应用类补充信息', index=False)
        df_cust.to_excel(writer, sheet_name='客户清单', index=False)

    # 提示处理完成
    messagebox.showinfo("处理完成", "处理已完成")

if __name__ == "__main__":
    main()
