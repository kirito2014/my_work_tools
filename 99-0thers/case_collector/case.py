import tkinter as tk
from tkinter import filedialog, messagebox
import os
import pandas as pd
from  test import process_files_in_folder


def select_folder():
    folder_path = filedialog.askdirectory()
    if folder_path:
        folder_entry.delete(0, tk.END)
        folder_entry.insert(0, folder_path)

def select_file():
    file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
    if file_path:
        file_entry.delete(0, tk.END)
        file_entry.insert(0, file_path)

def confirm_action():
    folder_path = folder_entry.get()
    target_file_path = file_entry.get()
    if not folder_path or not target_file_path:
        messagebox.showwarning("警告", "请先选择文件夹和目标文件")
        return

    result = process_files_in_folder(folder_path, target_file_path)
    log_text.config(state=tk.NORMAL)
    log_text.insert(tk.END, result + "\n")
    log_text.config(state=tk.DISABLED)

# 创建主窗口
root = tk.Tk()
root.title("案例合并工具")

# 设置窗口大小
root.geometry("500x400")

# 创建并放置组件
folder_label = tk.Label(root, text="选择文件夹:")
folder_label.pack(pady=5)
folder_entry = tk.Entry(root, width=50)
folder_entry.pack(pady=5)
folder_button = tk.Button(root, text="浏览文件夹", command=select_folder)
folder_button.pack(pady=5)

file_label = tk.Label(root, text="选择目标文件:")
file_label.pack(pady=5)
file_entry = tk.Entry(root, width=50)
file_entry.pack(pady=5)
file_button = tk.Button(root, text="浏览文件", command=select_file)
file_button.pack(pady=5)

confirm_button = tk.Button(root, text="确认", command=confirm_action)
confirm_button.pack(pady=20)

log_text = tk.Text(root, height=10, width=60, state=tk.DISABLED)
log_text.pack(pady=10)

# 运行主循环
root.mainloop()
