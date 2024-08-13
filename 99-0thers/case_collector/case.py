import tkinter as tk
from tkinter import filedialog, messagebox,Text,ttk,scrolledtext
import subprocess
import os,sys,re

sys.stdout.reconfigure(encoding='utf-8')

def select_folder():
    folder_path = filedialog.askdirectory()
    if folder_path:
        folder_var.set(folder_path)

def select_target_file():
    file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls *.xlsm")])
    if file_path:
        target_file_var.set(file_path)

def run_script():
    folder_path = folder_var.get()
    target_file_path = target_file_var.get()

    if not folder_path or not os.path.isdir(folder_path):
        messagebox.showerror("Error", "Please select a valid folder.")
        return
    
    if not target_file_path or not os.path.isfile(target_file_path):
        messagebox.showerror("Error", "Please select a valid target file.")
        return

    try:
        # Replace 'python_script.py' with the name of your script
        process = subprocess.Popen(
            ['python', 'test.py', folder_path, target_file_path]
        )


    except Exception as e:
        messagebox.showerror("Error", f"Failed to run the script: {e}")

app = tk.Tk()
app.title("Python Script Runner")

folder_var = tk.StringVar()
target_file_var = tk.StringVar()

tk.Label(app, text="选择要合并的文件夹:").pack(pady=5)
tk.Entry(app, textvariable=folder_var, width=50).pack(pady=5)
tk.Button(app, text="选择文件夹", command=select_folder).pack(pady=5)

tk.Label(app, text="请选择目标文件:").pack(pady=5)
tk.Entry(app, textvariable=target_file_var, width=50).pack(pady=5)
tk.Button(app, text="选择目标文件", command=select_target_file).pack(pady=5)

tk.Button(app, text="合并案例", command=run_script).pack(pady=20)

log_text = tk.Text(app, height=15, width=80)
log_text.pack(pady=5)

app.mainloop()
