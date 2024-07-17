import tkinter as tk
from tkinter import filedialog, scrolledtext
from tkinter import messagebox
import os

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel Comparison Tool")

        self.baseline_file = None
        self.new_version_file = None

        # Frame for buttons
        frame = tk.Frame(self.root)
        frame.pack(pady=10)

        # Baseline version file selection
        self.baseline_button = tk.Button(frame, text="选择基线版本文件", command=self.select_baseline_file, width=20)
        self.baseline_button.grid(row=0, column=0, padx=5, pady=5)

        # New version file selection
        self.new_version_button = tk.Button(frame, text="选择新版本文件", command=self.select_new_version_file, width=20)
        self.new_version_button.grid(row=0, column=1, padx=5, pady=5)

        # Compare button
        self.compare_button = tk.Button(self.root, text="确认对比", command=self.compare_files, width=20)
        self.compare_button.pack(pady=5)

        # Clear button
        self.clear_button = tk.Button(self.root, text="清除信息栏", command=self.clear_info, width=20)
        self.clear_button.pack(pady=5)

        # Information display
        self.info_label = tk.Label(self.root, text="待选择文件", fg="red")
        self.info_label.pack()

        self.info_display = scrolledtext.ScrolledText(self.root, width=80, height=20, wrap=tk.WORD)
        self.info_display.pack(pady=5)

    def select_baseline_file(self):
        self.baseline_file = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if self.baseline_file:
            self.info_label.config(text=os.path.basename(self.baseline_file), fg="black")
            self.info_display.insert(tk.END, f"基线版本文件已选择: {self.baseline_file}\n")
        else:
            self.info_label.config(text="待选择文件", fg="red")

    def select_new_version_file(self):
        self.new_version_file = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if self.new_version_file:
            self.info_label.config(text=os.path.basename(self.new_version_file), fg="black")
            self.info_display.insert(tk.END, f"新版本文件已选择: {self.new_version_file}\n")
        else:
            self.info_label.config(text="待选择文件", fg="red")

    def compare_files(self):
        if not self.baseline_file or not self.new_version_file:
            messagebox.showwarning("文件未选择", "请先选择两个文件再进行对比。")
            return

        # Call the function to compare the files
        self.info_display.insert(tk.END, "正在对比文件...\n")
        self.info_display.see(tk.END)
        self.info_display.update()

        # 示例对比过程，需替换为实际的对比逻辑
        if self.baseline_file and self.new_version_file:
            self.info_display.insert(tk.END, "文件对比完成。\n")
        else:
            self.info_display.insert(tk.END, "文件对比失败。\n")

        self.info_display.see(tk.END)

    def clear_info(self):
        self.info_display.delete('1.0', tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
