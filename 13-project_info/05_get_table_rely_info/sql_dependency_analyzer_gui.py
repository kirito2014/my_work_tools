#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL依赖关系分析工具 - GUI界面
使用tkinter和ttkthemes库
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import queue
from pathlib import Path
import yaml
import logging
from datetime import datetime

# 导入自定义模块
import sql_dependency_analyzer as analyzer

# 尝试导入ttkthemes
try:
    from ttkthemes import ThemedTk
    USE_THEMED_TK = True
except ImportError:
    USE_THEMED_TK = False
    from tkinter import Tk as ThemedTk

# 配置日志系统
class TextHandler(logging.Handler):
    """将日志输出到文本控件"""
    
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget
        self.text_widget.configure(state='disabled')
        
    def emit(self, record):
        msg = self.format(record)
        
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.see(tk.END)
            self.text_widget.configure(state='disabled')
        
        self.text_widget.after(0, append)


class SQLDependencyAnalyzerGUI:
    """SQL依赖关系分析工具GUI界面"""
    
    def __init__(self):
        # 创建主窗口
        if USE_THEMED_TK:
            self.root = ThemedTk(theme="arc")
        else:
            self.root = ThemedTk()
        
        self.root.title("SQL依赖关系分析工具 v2.0")
        self.root.geometry("1000x750")
        
        # 设置图标（如果有）
        self._set_icon()
        
        # 创建样式
        self._create_styles()
        
        # 初始化变量
        self.config_path = tk.StringVar()
        self.output_path = tk.StringVar()
        self.output_format = tk.StringVar(value="excel")
        self.dependency_file_path = tk.StringVar()
        self.folder_path = tk.StringVar()
        self.verbose_mode = tk.BooleanVar(value=False)
        self.config_data = {}
        self.is_running = False
        
        # 设置默认值
        self._set_default_paths()
        
        # 创建界面
        self._create_widgets()
        
        # 绑定事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # 初始化配置
        self.load_configuration()
    
    def _set_icon(self):
        """设置窗口图标"""
        try:
            # 尝试加载图标文件
            icon_path = "sunline.ico"
            if os.path.exists(icon_path):
                self.root.iconbitmap(icon_path)
        except:
            pass
    
    def _set_default_paths(self):
        """设置默认路径"""
        # 当前目录
        current_dir = os.getcwd()
        
        # 设置默认配置文件路径
        default_config = os.path.join(current_dir, "config.yaml")
        if os.path.exists(default_config):
            self.config_path.set(default_config)
        else:
            self.config_path.set(default_config)
        
        # 设置默认输出文件路径
        default_output = os.path.join(current_dir, "dependency_analysis.xlsx")
        self.output_path.set(default_output)
    
    def _create_styles(self):
        """创建样式"""
        style = ttk.Style()
        
        # 配置字体
        font_name = "Microsoft YaHei"
        
        # 检查字体是否存在
        try:
            # 尝试创建使用微软雅黑的字体
            default_font = (font_name, 11)
            
            # 为不同控件配置字体
            style.configure("TLabel", font=default_font)
            style.configure("TButton", font=default_font)
            style.configure("TEntry", font=default_font)
            style.configure("TCombobox", font=default_font)
            style.configure("TCheckbutton", font=default_font)
            style.configure("TNotebook.Tab", font=(font_name, 10))
            
            # 配置标题字体
            title_font = (font_name, 12, "bold")
            style.configure("Title.TLabel", font=title_font)
            
        except Exception:
            # 如果微软雅黑不可用，使用默认字体
            pass
        
        # 配置颜色
        style.configure("Success.TLabel", foreground="green")
        style.configure("Error.TLabel", foreground="red")
        style.configure("Warning.TLabel", foreground="orange")
    
    def _create_widgets(self):
        """创建界面控件"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置行和列的权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # 创建标题
        title_label = ttk.Label(
            main_frame, 
            text="📊 SQL依赖关系分析工具", 
            style="Title.TLabel"
        )
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # 创建标签页
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 配置标签页所在行和列的权重
        main_frame.rowconfigure(1, weight=1)
        
        # 配置标签页
        config_frame = ttk.Frame(notebook, padding="10")
        generate_frame = ttk.Frame(notebook, padding="10")
        log_frame = ttk.Frame(notebook, padding="10")
        
        notebook.add(config_frame, text="📝 配置")
        notebook.add(generate_frame, text="⚙️ 生成")
        notebook.add(log_frame, text="📋 日志")
        
        # 创建配置页面
        self._create_config_tab(config_frame)
        
        # 创建生成页面
        self._create_generate_tab(generate_frame)
        
        # 创建日志页面
        self._create_log_tab(log_frame)
        
        # 创建底部按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=2, column=0, columnspan=3, pady=(10, 0))
        
        ttk.Button(
            button_frame, 
            text="保存配置", 
            command=self.save_configuration,
            width=15
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="开始分析", 
            command=self.start_analysis,
            width=15
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="退出", 
            command=self.on_closing,
            width=15
        ).pack(side=tk.LEFT, padx=5)
    
    def _create_config_tab(self, parent):
        """创建配置标签页"""
        # 配置文件选择
        row = 0
        ttk.Label(parent, text="配置文件:").grid(row=row, column=0, sticky=tk.W, pady=5)
        
        config_entry = ttk.Entry(parent, textvariable=self.config_path, width=50)
        config_entry.grid(row=row, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        
        ttk.Button(
            parent, 
            text="浏览", 
            command=lambda: self.browse_file(self.config_path, "选择配置文件", [("YAML文件", "*.yaml;*.yml"), ("所有文件", "*.*")])
        ).grid(row=row, column=2, padx=5, pady=5)
        
        row += 1
        
        ttk.Button(
            parent, 
            text="加载配置", 
            command=self.load_configuration,
            width=15
        ).grid(row=row, column=0, columnspan=3, pady=10)
        
        row += 1
        
        # 配置显示区域
        ttk.Label(parent, text="当前配置:").grid(row=row, column=0, sticky=tk.W, pady=(10, 5))
        row += 1
        
        # 创建配置显示文本框
        self.config_text = scrolledtext.ScrolledText(parent, width=80, height=20, font=("Consolas", 10))
        self.config_text.grid(row=row, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        
        # 配置文本框所在行和列的权重
        parent.rowconfigure(row, weight=1)
        parent.columnconfigure(1, weight=1)
    
    def _create_generate_tab(self, parent):
        """创建生成标签页"""
        row = 0
        
        # SQL文件夹选择
        ttk.Label(parent, text="SQL文件夹:").grid(row=row, column=0, sticky=tk.W, pady=5)
        
        folder_entry = ttk.Entry(parent, textvariable=self.folder_path, width=50)
        folder_entry.grid(row=row, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        
        ttk.Button(
            parent, 
            text="浏览", 
            command=lambda: self.browse_folder(self.folder_path)
        ).grid(row=row, column=2, padx=5, pady=5)
        
        row += 1
        
        # 输出文件选择
        ttk.Label(parent, text="输出文件:").grid(row=row, column=0, sticky=tk.W, pady=5)
        
        output_entry = ttk.Entry(parent, textvariable=self.output_path, width=50)
        output_entry.grid(row=row, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        
        ttk.Button(
            parent, 
            text="浏览", 
            command=lambda: self.browse_file(self.output_path, "选择输出文件", 
                                           [("Excel文件", "*.xlsx"), ("CSV文件", "*.csv"), 
                                            ("JSON文件", "*.json"), ("HTML文件", "*.html"),
                                            ("所有文件", "*.*")])
        ).grid(row=row, column=2, padx=5, pady=5)
        
        row += 1
        
        # 输出格式选择
        ttk.Label(parent, text="输出格式:").grid(row=row, column=0, sticky=tk.W, pady=5)
        
        format_combo = ttk.Combobox(
            parent, 
            textvariable=self.output_format, 
            values=["excel", "csv", "json", "html"],
            state="readonly",
            width=20
        )
        format_combo.grid(row=row, column=1, sticky=tk.W, padx=5, pady=5)
        
        row += 1
        
        # 依赖清单文件选择
        ttk.Label(parent, text="依赖清单文件:").grid(row=row, column=0, sticky=tk.W, pady=5)
        
        dependency_entry = ttk.Entry(parent, textvariable=self.dependency_file_path, width=50)
        dependency_entry.grid(row=row, column=1, sticky=(tk.W, tk.E), padx=5, pady=5)
        
        ttk.Button(
            parent, 
            text="浏览", 
            command=lambda: self.browse_file(self.dependency_file_path, "选择依赖清单文件", 
                                           [("Excel文件", "*.xlsx;*.xls"), ("所有文件", "*.*")])
        ).grid(row=row, column=2, padx=5, pady=5)
        
        row += 1
        
        # 详细日志选项
        ttk.Checkbutton(
            parent, 
            text="显示详细日志", 
            variable=self.verbose_mode
        ).grid(row=row, column=0, sticky=tk.W, pady=10)
        
        row += 1
        
        # 进度条
        ttk.Label(parent, text="进度:").grid(row=row, column=0, sticky=tk.W, pady=(20, 5))
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            parent, 
            variable=self.progress_var, 
            maximum=100,
            length=400,
            mode='determinate'
        )
        self.progress_bar.grid(row=row, column=1, sticky=(tk.W, tk.E), padx=5, pady=(20, 5))
        
        self.progress_label = ttk.Label(parent, text="0%")
        self.progress_label.grid(row=row, column=2, padx=5, pady=(20, 5))
        
        row += 1
        
        # 状态标签
        self.status_label = ttk.Label(parent, text="就绪")
        self.status_label.grid(row=row, column=0, columnspan=3, pady=10)
        
        # 配置权重
        parent.columnconfigure(1, weight=1)
    
    def _create_log_tab(self, parent):
        """创建日志标签页"""
        # 创建日志文本框
        self.log_text = scrolledtext.ScrolledText(parent, width=100, height=25, font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        # 创建按钮框架
        button_frame = ttk.Frame(parent)
        button_frame.grid(row=1, column=0, sticky=tk.E, pady=(5, 0))
        
        ttk.Button(
            button_frame, 
            text="清除日志", 
            command=self.clear_log,
            width=15
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="保存日志", 
            command=self.save_log,
            width=15
        ).pack(side=tk.LEFT, padx=5)
        
        # 配置权重
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)
    
    def browse_file(self, path_var, title, filetypes):
        """浏览文件"""
        filename = filedialog.askopenfilename(
            title=title,
            filetypes=filetypes,
            initialdir=os.path.dirname(path_var.get()) if path_var.get() else os.getcwd()
        )
        
        if filename:
            path_var.set(filename)
    
    def browse_folder(self, path_var):
        """浏览文件夹"""
        folder = filedialog.askdirectory(
            title="选择SQL文件夹",
            initialdir=path_var.get() if path_var.get() else os.getcwd()
        )
        
        if folder:
            path_var.set(folder)
    
    def load_configuration(self):
        """加载配置文件"""
        config_file = self.config_path.get()
        
        if not config_file:
            messagebox.showwarning("警告", "请先选择配置文件路径")
            return
        
        if not os.path.exists(config_file):
            # 尝试创建默认配置文件
            try:
                default_config = analyzer.ConfigManager()._get_default_config()
                with open(config_file, 'w', encoding='utf-8') as f:
                    yaml.dump(default_config, f, allow_unicode=True, default_flow_style=False)
                messagebox.showinfo("信息", f"已创建默认配置文件: {config_file}")
            except Exception as e:
                messagebox.showerror("错误", f"创建配置文件失败: {e}")
                return
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config_data = yaml.safe_load(f)
            
            # 在文本框中显示配置
            self.config_text.delete(1.0, tk.END)
            
            if self.config_data:
                # 格式化YAML内容
                import yaml
                formatted_config = yaml.dump(
                    self.config_data, 
                    allow_unicode=True, 
                    default_flow_style=False,
                    indent=2,
                    sort_keys=False
                )
                self.config_text.insert(tk.END, formatted_config)
            
            self.log_message("INFO", f"配置文件加载成功: {config_file}")
            
            # 根据配置文件更新UI
            self._update_ui_from_config()
            
        except Exception as e:
            self.log_message("ERROR", f"加载配置文件失败: {e}")
            messagebox.showerror("错误", f"加载配置文件失败: {e}")
    
    def _update_ui_from_config(self):
        """根据配置文件更新UI"""
        if not self.config_data:
            return
        
        # 根据配置文件中的设置更新输出格式
        output_config = self.config_data.get('output', {})
        if output_config:
            # 可以根据配置设置默认输出格式
            pass
    
    def save_configuration(self):
        """保存配置文件"""
        config_file = self.config_path.get()
        
        if not config_file:
            messagebox.showwarning("警告", "请先选择配置文件路径")
            return
        
        try:
            # 从文本框获取配置内容
            config_text = self.config_text.get(1.0, tk.END).strip()
            
            if not config_text:
                messagebox.showwarning("警告", "配置内容为空")
                return
            
            # 解析YAML内容
            try:
                config_data = yaml.safe_load(config_text)
            except yaml.YAMLError as e:
                messagebox.showerror("错误", f"YAML格式错误: {e}")
                return
            
            # 保存到文件
            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, allow_unicode=True, default_flow_style=False, indent=2)
            
            self.config_data = config_data
            self.log_message("INFO", f"配置文件保存成功: {config_file}")
            messagebox.showinfo("成功", "配置文件保存成功")
            
        except Exception as e:
            self.log_message("ERROR", f"保存配置文件失败: {e}")
            messagebox.showerror("错误", f"保存配置文件失败: {e}")
    
    def start_analysis(self):
        """开始分析"""
        # 检查是否已经在运行
        if self.is_running:
            messagebox.showwarning("警告", "分析任务正在进行中，请等待完成")
            return
        
        # 验证输入
        if not self.folder_path.get():
            messagebox.showwarning("警告", "请选择SQL文件夹")
            return
        
        if not os.path.exists(self.folder_path.get()):
            messagebox.showwarning("警告", "SQL文件夹不存在")
            return
        
        # 重置进度条
        self.progress_var.set(0)
        self.progress_label.config(text="0%")
        self.status_label.config(text="分析中...")
        
        # 禁用按钮
        self.is_running = True
        
        # 在新线程中运行分析任务
        analysis_thread = threading.Thread(target=self.run_analysis)
        analysis_thread.daemon = True
        analysis_thread.start()
    
    def run_analysis(self):
        """运行分析任务"""
        try:
            # 准备参数
            args = {
                'folder_path': self.folder_path.get(),
                'output': self.output_path.get(),
                'config': self.config_path.get(),
                'format': self.output_format.get(),
                'dependency_file': self.dependency_file_path.get() or None,
                'verbose': self.verbose_mode.get()
            }
            
            # 检查配置文件
            if not os.path.exists(args['config']):
                self.root.after(0, lambda: messagebox.showwarning("警告", "配置文件不存在"))
                return
            
            # 创建分析器
            analyzer_obj = analyzer.SQLDependencyAnalyzer(args['config'])
            
            # 处理文件夹
            self.root.after(0, lambda: self.log_message("INFO", f"开始分析文件夹: {args['folder_path']}"))
            
            # 获取文件列表
            sql_files = analyzer_obj._find_sql_files(args['folder_path'])
            total_files = len(sql_files)
            
            if total_files == 0:
                self.root.after(0, lambda: messagebox.showwarning("警告", "未找到SQL文件"))
                return
            
            self.root.after(0, lambda: self.log_message("INFO", f"找到 {total_files} 个SQL文件"))
            
            # 处理每个文件
            data = []
            for i, file_path in enumerate(sql_files, 1):
                try:
                    file_data = analyzer_obj.process_single_file(file_path)
                    data.extend(file_data)
                    
                    # 更新进度
                    progress = (i / total_files) * 100
                    self.root.after(0, lambda p=progress: self.update_progress(p, f"处理文件 {i}/{total_files}"))
                    
                    if i % 10 == 0 or i == total_files:
                        self.root.after(0, lambda f=i: self.log_message("INFO", f"已处理 {f}/{total_files} 个文件"))
                        
                except Exception as e:
                    self.root.after(0, lambda f=file_path, err=e: self.log_message("ERROR", f"处理文件 {f} 失败: {err}"))
            
            if not data:
                self.root.after(0, lambda: messagebox.showwarning("警告", "未找到任何依赖关系数据"))
                return
            
            # 导出结果
            self.root.after(0, lambda: self.log_message("INFO", f"分析完成，共处理 {len(data)} 条依赖关系"))
            self.root.after(0, lambda: self.log_message("INFO", "正在导出结果..."))
            
            exporter = analyzer.ResultExporter(analyzer_obj.config)
            include_dependency = bool(args['dependency_file'])
            
            exporter.export(
                data, 
                args['output'], 
                args['format'], 
                include_dependency, 
                args['dependency_file']
            )
            
            # 完成
            self.root.after(0, lambda: self.log_message("INFO", f"导出完成: {args['output']}"))
            self.root.after(0, lambda: self.update_progress(100, "完成"))
            self.root.after(0, lambda: self.status_label.config(text="分析完成"))
            
            # 显示完成消息
            self.root.after(0, lambda: messagebox.showinfo(
                "完成", 
                f"分析完成！\n\n"
                f"处理文件数: {total_files}\n"
                f"依赖关系数: {len(data)}\n"
                f"输出文件: {args['output']}"
            ))
            
        except Exception as e:
            self.root.after(0, lambda: self.log_message("ERROR", f"分析失败: {e}"))
            self.root.after(0, lambda: self.status_label.config(text="分析失败"))
            self.root.after(0, lambda: messagebox.showerror("错误", f"分析失败: {e}"))
        
        finally:
            # 恢复按钮状态
            self.is_running = False
    
    def update_progress(self, value, message=""):
        """更新进度条"""
        self.progress_var.set(value)
        self.progress_label.config(text=f"{value:.1f}%")
        if message:
            self.status_label.config(text=message)
    
    def log_message(self, level, message):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] [{level}] {message}"
        
        # 根据日志级别设置颜色
        colors = {
            "INFO": "black",
            "WARNING": "orange",
            "ERROR": "red",
            "DEBUG": "gray"
        }
        
        color = colors.get(level, "black")
        
        # 在日志文本框中添加消息
        self.log_text.configure(state='normal')
        self.log_text.insert(tk.END, formatted_message + '\n', color)
        self.log_text.see(tk.END)
        self.log_text.configure(state='disabled')
    
    def clear_log(self):
        """清除日志"""
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')
    
    def save_log(self):
        """保存日志到文件"""
        filename = filedialog.asksaveasfilename(
            title="保存日志文件",
            defaultextension=".log",
            filetypes=[("日志文件", "*.log"), ("文本文件", "*.txt"), ("所有文件", "*.*")],
            initialdir=os.getcwd()
        )
        
        if filename:
            try:
                log_content = self.log_text.get(1.0, tk.END)
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(log_content)
                self.log_message("INFO", f"日志已保存到: {filename}")
            except Exception as e:
                messagebox.showerror("错误", f"保存日志失败: {e}")
    
    def on_closing(self):
        """关闭窗口时的处理"""
        if self.is_running:
            if messagebox.askyesno("确认", "分析任务正在进行中，确定要退出吗？"):
                self.root.destroy()
        else:
            self.root.destroy()


def main():
    """主函数"""
    try:
        app = SQLDependencyAnalyzerGUI()
        app.root.mainloop()
    except Exception as e:
        print(f"程序启动失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()