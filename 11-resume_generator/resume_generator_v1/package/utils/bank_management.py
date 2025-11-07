#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
银行管理模块 - 用于管理银行信息和图标转换
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinter import scrolledtext
import tkinter.font as font
import subprocess
import threading

# 导入ttkthemes并处理导入失败的情况
try:
    from ttkthemes import ThemedTk
except ImportError:
    print("警告: 未找到ttkthemes模块，请先安装: pip install ttkthemes")
    ThemedTk = tk.Tk

# 获取当前脚本所在目录的父目录作为基础目录
base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class BankManagementDialog:
    """银行管理对话框"""
    
    def __init__(self, parent=None):
        """初始化银行管理对话框"""
        # 创建主窗口，根据是否有父窗口选择窗口类型
        if parent:
            self.root = tk.Toplevel(parent)
        else:
            # 如果没有父窗口，使用ThemedTk并应用arc主题
            self.root = ThemedTk(theme="arc")
        
        self.root.title("银行管理")
        self.root.geometry("600x450")
        self.root.resizable(False, False)
        
        # 设置字体配置 - 统一使用微软雅黑10号
        self.font_config = {
            'label': ('Microsoft YaHei', 10),
            'entry': ('Microsoft YaHei', 10),
            'text': ('Microsoft YaHei', 10)
        }
        
        # 设置全局字体
        default_font = font.nametofont("TkDefaultFont")
        default_font.configure(family="Microsoft YaHei", size=10)
        
        # 应用全局字体到所有部件
        text_font = font.nametofont("TkTextFont")
        text_font.configure(family="Microsoft YaHei", size=10)
        
        fixed_font = font.nametofont("TkFixedFont")
        fixed_font.configure(family="Microsoft YaHei", size=10)
        
        # 初始化界面
        self._init_ui()
        
        # 设置窗口属性
        if parent:
            self.root.transient(parent)
            self.root.grab_set()
    
    def _init_ui(self):
        """初始化用户界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. PNG文件选择部分
        png_frame = ttk.LabelFrame(main_frame, text="PNG文件选择", padding="10")
        png_frame.pack(fill=tk.X, pady=10)
        
        ttk.Label(png_frame, text="PNG图片路径:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_png_path = tk.StringVar()
        ttk.Entry(png_frame, textvariable=self.bank_png_path, width=40, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(png_frame, text="浏览", command=self._select_png_file).pack(side=tk.LEFT, padx=5)
        
        # 2. 银行名称输入部分
        name_frame = ttk.LabelFrame(main_frame, text="银行信息", padding="10")
        name_frame.pack(fill=tk.X, pady=10)
        
        ttk.Label(name_frame, text="银行名称:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_name = tk.StringVar()
        ttk.Entry(name_frame, textvariable=self.bank_name, width=40, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Label(name_frame, text="(如: 恒丰银行)", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        
        # 3. 执行操作部分 - 放在银行信息下面，处理日志上面
        action_frame = ttk.LabelFrame(main_frame, text="执行操作", padding="10")
        action_frame.pack(fill=tk.X, pady=10)
        
        button_frame = ttk.Frame(action_frame)
        button_frame.pack(fill=tk.X, pady=5, side=tk.RIGHT)
        ttk.Button(button_frame, text="开始处理", command=self._add_bank, style="Accent.TButton").pack(side=tk.RIGHT, padx=10)
        ttk.Button(button_frame, text="关闭", command=self.root.destroy).pack(side=tk.RIGHT, padx=10)
        
        # 4. 日志栏
        log_frame = ttk.LabelFrame(main_frame, text="处理日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.bank_log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=self.font_config['text'], height=10)
        self.bank_log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 设置样式
        self._setup_styles()
    
    def _setup_styles(self):
        """设置界面样式"""
        style = ttk.Style()
        # 设置全局ttk组件字体
        style.configure(".", font=self.font_config['label'])
        # 创建强调按钮样式
        style.configure("Accent.TButton", font=self.font_config['label'])
        style.configure("Accent.TButton", foreground="#0078D7")  # 设置强调按钮的前景色
    
    def _select_png_file(self):
        """选择PNG文件"""
        file_path = filedialog.askopenfilename(filetypes=[("PNG图片", "*.png")])
        if file_path:
            self.bank_png_path.set(file_path)
    
    def _add_bank(self):
        """添加银行信息并转换图标"""
        # 获取输入值
        png_path = self.bank_png_path.get().strip()
        bank_name = self.bank_name.get().strip()
        
        # 验证输入
        if not png_path:
            messagebox.showerror("错误", "请选择PNG文件")
            return
        
        if not bank_name:
            messagebox.showerror("错误", "请输入银行名称")
            return
        
        if not os.path.exists(png_path):
            messagebox.showerror("错误", "选择的PNG文件不存在")
            return
        
        # 清空日志
        self.bank_log_text.delete(1.0, tk.END)
        self.bank_log_text.insert(tk.END, f"开始处理银行信息: {bank_name}\n")
        
        # 定义转换函数
        def convert_icon():
            try:
                # 设置输出路径
                bank_pics_dir = os.path.join(base_dir, 'resources', 'bank_pics')
                os.makedirs(bank_pics_dir, exist_ok=True)
                
                # 输出ICO文件路径
                ico_filename = f"{bank_name}.ico"
                ico_path = os.path.join(bank_pics_dir, ico_filename)
                
                # 记录日志
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"输出路径: {ico_path}\n"))
                
                # 调用icon_converter.py进行转换
                converter_path = os.path.join(base_dir, 'package', 'utils', 'icon_converter.py')
                cmd = [sys.executable, converter_path, png_path, ico_path, '--size', '32']
                
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"执行转换命令...\n"))
                
                # 执行转换
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # 实时输出转换过程
                stdout, stderr = process.communicate()
                
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, stdout))
                if stderr:
                    self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"错误: {stderr}\n"))
                
                # 检查转换是否成功
                if process.returncode == 0 and os.path.exists(ico_path):
                    # 更新银行列表配置
                    bank_list_config = os.path.join(base_dir, 'config', 'bank_list.config')
                    os.makedirs(os.path.dirname(bank_list_config), exist_ok=True)
                    
                    # 读取现有银行列表
                    bank_list = []
                    if os.path.exists(bank_list_config):
                        with open(bank_list_config, 'r', encoding='utf-8') as f:
                            bank_list = [line.strip() for line in f.readlines() if line.strip()]
                    
                    # 添加新银行（如果不存在）
                    if bank_name not in bank_list:
                        bank_list.append(bank_name)
                        # 保存银行列表
                        with open(bank_list_config, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(bank_list))
                        self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"\n银行列表已更新，当前包含 {len(bank_list)} 个银行\n"))
                    else:
                        self.root.after(0, lambda: self.bank_log_text.insert(tk.END, "\n银行名称已存在于配置中\n"))
                    
                    self.root.after(0, lambda: [
                        self.bank_log_text.insert(tk.END, "\n✓ 银行添加成功！"),
                        self.bank_log_text.see(tk.END)
                    ])
                    self.root.after(500, lambda: messagebox.showinfo("成功", "银行添加成功！"))
                else:
                    self.root.after(0, lambda: [
                        self.bank_log_text.insert(tk.END, "\n[ERR] 银行添加失败，请查看日志！"),
                        self.bank_log_text.see(tk.END)
                    ])
                    self.root.after(500, lambda: messagebox.showerror("失败", "银行添加失败，请查看日志"))
                    
            except Exception as e:
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"\n[ERR] 处理出错: {str(e)}\n"))
                self.root.after(500, lambda: messagebox.showerror("错误", f"处理出错: {str(e)}"))
        
        # 在新线程中执行转换
        threading.Thread(target=convert_icon, daemon=True).start()
    
    def run(self):
        """运行对话框（独立模式）"""
        if not hasattr(self.root, 'master') or not self.root.master:
            self.root.mainloop()


def main():
    """主函数"""
    app = BankManagementDialog()
    app.run()


if __name__ == "__main__":
    main()
