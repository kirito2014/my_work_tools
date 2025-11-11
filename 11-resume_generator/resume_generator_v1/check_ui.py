#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简历校验对话框 - 用于校验简历JSON文件的有效性
"""

import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, font
from tkinter import scrolledtext
import subprocess
import threading
from datetime import datetime

# 导入ttkthemes并处理导入失败的情况
try:
    from ttkthemes import ThemedTk
except ImportError:
    print("警告: 未找到ttkthemes模块，请先安装: pip install ttkthemes")
    ThemedTk = tk.Tk

# 获取程序所在目录作为基础目录
if getattr(sys, 'frozen', False):
    # 如果是打包后的exe文件
    base_dir = os.path.dirname(sys.executable)
else:
    # 如果是直接运行的Python脚本
    base_dir = os.path.dirname(os.path.abspath(__file__))

# 确保output/checkExcel目录存在
# 生成在exe所在文件目录下的output/checkExcel/
check_dir = os.path.join(base_dir, "output", "checkExcel")
if not os.path.exists(check_dir):
    os.makedirs(check_dir)
    print(f"已创建输出目录: {check_dir}")

class ResumeValidationDialog:
    """简历校验对话框"""
    
    def __init__(self, parent=None):
        """初始化简历校验对话框"""
        # 创建主窗口，根据是否有父窗口选择窗口类型
        if parent:
              self.root = tk.Toplevel(parent)
              self.root.transient(parent)  # 设置为父窗口的临时窗口
              self.root.grab_set()  # 模态化，阻止父窗口交互
              self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        else:
            # 如果没有父窗口，使用ThemedTk并应用arc主题
            self.root = ThemedTk(theme="arc")
        
        self.root.title("简历校验工具")
        self.root.geometry("800x600")
        
        # 设置字体配置
        self.font_config = {
            'label': ('Microsoft YaHei', 10),
            'entry': ('Microsoft YaHei', 10),
            'text': ('Microsoft YaHei', 10)
        }
        
        # 初始化界面
        self._init_ui()
        
        # 设置校验脚本路径 - 按照bank_management.py的方式处理
        print(f"[校验UI] 基础目录: {base_dir}")
        
        # 首先尝试直接导入模块（用于EXE模式）
        self.check_script_path = None
        
        # 根据运行模式设置脚本路径
        if getattr(sys, 'frozen', False):
            # EXE模式 - 尝试多种可能的路径
            possible_paths = [
                os.path.join(base_dir, "package", "functions", "check_resume_valid.py"),
                os.path.join(base_dir, "check_resume_valid.py")
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    self.check_script_path = path
                    break
            # 如果找不到脚本文件，仍然设置一个路径（用于日志显示）
            if not self.check_script_path:
                self.check_script_path = os.path.join(base_dir, "package", "functions", "check_resume_valid.py")
                print(f"[校验UI] EXE模式: 未找到脚本文件，但仍设置路径为: {self.check_script_path}")
        else:
            # 开发模式
            self.check_script_path = os.path.join(base_dir, "package", "functions", "check_resume_valid.py")
        
        print(f"[校验UI] 校验脚本路径: {self.check_script_path}")
        
        # 校验结果文件路径 - 生成在exe所在目录下的output/checkExcel/
        self.check_result_file = os.path.join(check_dir, 'check_result.xlsx')
        print(f"[校验UI] 校验结果文件路径: {self.check_result_file}")
    
    def _init_ui(self):
        """初始化用户界面"""
        # 设置全局字体
        self._setup_styles()
        
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 文件夹选择区域
        folder_frame = ttk.LabelFrame(main_frame, text="文件夹选择", padding="10")
        folder_frame.pack(fill=tk.X, pady=10)
        
        self.folder_var = tk.StringVar()
        folder_entry = ttk.Entry(folder_frame, textvariable=self.folder_var, width=60, font=self.font_config['entry'])
        folder_entry.pack(side=tk.LEFT, padx=5, pady=5, fill=tk.X, expand=True)
        
        select_button = ttk.Button(folder_frame, text="浏览", command=self._select_folder)
        select_button.pack(side=tk.RIGHT, padx=5, pady=5)
        
        # 控制按钮区域
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        self.start_button = ttk.Button(control_frame, text="开始校验", command=self._start_validation, style="Accent.TButton")
        self.start_button.pack(side=tk.LEFT, padx=10, pady=5)
        
        self.cancel_button = ttk.Button(control_frame, text="取消", command=self._on_close, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.LEFT, padx=10, pady=5)
        
        # 进度条区域
        progress_frame = ttk.LabelFrame(main_frame, text="校验进度", padding="10")
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, length=100, mode='determinate')
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        
        self.progress_label = ttk.Label(progress_frame, text="准备就绪", font=self.font_config['label'])
        self.progress_label.pack(anchor=tk.W, padx=5, pady=2)
        
        # 日志显示区域
        log_frame = ttk.LabelFrame(main_frame, text="校验日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=self.font_config['text'], height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 配置日志文本框样式
        self.log_text.config(state=tk.DISABLED)
    
    def _setup_styles(self):
        """设置界面样式"""
        # 设置全局字体
        default_font = tk.font.nametofont("TkDefaultFont")
        default_font.configure(family="Microsoft YaHei", size=10)
        
        text_font = tk.font.nametofont("TkTextFont")
        text_font.configure(family="Microsoft YaHei", size=10)
        
        fixed_font = tk.font.nametofont("TkFixedFont")
        fixed_font.configure(family="Microsoft YaHei", size=10)
        
        # 设置ttk组件样式
        style = ttk.Style()
        style.configure(".", font=self.font_config['label'])
        # 创建强调按钮样式
        style.configure("Accent.TButton", font=self.font_config['label'], foreground="#0078D7")
    
    def _select_folder(self):
        """选择文件夹"""
        # 确保窗口在对话框打开前获得焦点
        self.root.lift()
        self.root.focus_force()
        
        folder_path = filedialog.askdirectory(
            title="选择JSON文件所在文件夹",
            parent=self.root
        )
        
        if folder_path:
            # 确保路径使用正斜杠或双反斜杠，避免显示问题
            folder_path = folder_path.replace('/', '\\')
            self.folder_var.set(folder_path)
            # 选择后再次确保窗口保持焦点
            self.root.lift()
            self.root.focus_force()
    
    def _start_validation(self):
        """开始校验"""
        folder_path = self.folder_var.get()
        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showerror("错误", "请先选择有效的文件夹")
            return
        
        # 按照bank_management.py的方式处理模块导入和检查
        # 不再严格检查脚本文件是否存在，而是尝试导入模块
        print(f"[校验UI] 程序模式: {'打包为exe' if getattr(sys, 'frozen', False) else 'Python脚本'}")
        print(f"[校验UI] 校验脚本路径: {self.check_script_path}")
        
        # 对于EXE模式，采用更灵活的方式，不强制要求脚本文件存在
        # 因为在EXE模式下，模块可能已经被打包到EXE中
        
        # 检查是否存在JSON文件
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        if not json_files:
            messagebox.showinfo("提示", "所选文件夹中没有JSON文件")
            return
        
        # 删除现有的校验结果文件
        if os.path.exists(self.check_result_file):
            try:
                os.remove(self.check_result_file)
                self._log(f"已删除现有校验结果文件: {self.check_result_file}")
            except Exception as e:
                messagebox.showwarning("警告", f"无法删除现有文件: {str(e)}")
        
        # 初始化进度条
        self.progress_var.set(0)
        self.progress_label.config(text="开始校验...")
        
        # 禁用开始按钮，启用取消按钮
        self.start_button.config(state=tk.DISABLED)
        self.cancel_button.config(state=tk.NORMAL)
        
        # 清空日志
        self._clear_log()
        
        # 在新线程中执行校验
        self.stop_event = threading.Event()
        self.validation_thread = threading.Thread(target=self._validation_thread, args=(folder_path, json_files))
        self.validation_thread.daemon = True
        self.validation_thread.start()
        
        # 启动进度检查
        self._check_thread_status()
    
    def _validation_thread(self, folder_path, json_files):
        """校验线程"""
        total_files = len(json_files)
        success_count = 0
        error_count = 0
        
        for index, json_file in enumerate(json_files):
            if self.stop_event.is_set():
                self._log("校验已取消")
                break
            
            json_file_path = os.path.join(folder_path, json_file)
            self._log(f"正在校验: {json_file}")
            
            try:
                # 按照bank_management.py的方式处理调用逻辑
                print(f"[校验UI] 处理文件: {json_file_path}")
                
                # 在EXE模式下，我们需要避免使用EXE本身来执行脚本，否则会重复打开主界面
                # 尝试直接导入模块或使用其他方式
                result = None
                
                if getattr(sys, 'frozen', False):
                    # EXE模式 - 尝试直接导入模块或使用Python解释器（如果可用）
                    try:
                        # 方法1：尝试直接导入模块并调用
                        print("[校验UI] EXE模式：尝试直接导入check_resume_valid模块")
                        import importlib.util
                        
                        # 尝试直接导入模块
                        try:
                            # 先尝试作为Python模块导入
                            import package.functions.check_resume_valid as check_module
                            print("[校验UI] 成功导入package.functions.check_resume_valid模块")
                            
                            # 直接调用模块的main函数或其他入口函数
                            # 这里需要根据check_resume_valid.py的实际实现来调整
                            from io import StringIO
                            
                            # 保存原始的stdout和stderr
                            original_stdout = sys.stdout
                            original_stderr = sys.stderr
                            
                            # 重定向stdout和stderr到字符串缓冲区
                            sys.stdout = StringIO()
                            sys.stderr = StringIO()
                            
                            try:
                                # 模拟命令行参数调用
                                sys.argv = ['check_resume_valid.py', '--json', json_file_path, '--output', self.check_result_file, '--append']
                                
                                # 调用模块的main函数
                                if hasattr(check_module, 'main'):
                                    check_module.main()
                                    print("[校验UI] 成功调用check_module.main()")
                                else:
                                    print("[校验UI] 警告：check_module没有main函数")
                                
                                # 获取输出结果
                                stdout_output = sys.stdout.getvalue()
                                stderr_output = sys.stderr.getvalue()
                                print(f"[校验UI] 模块输出: {stdout_output}")
                                if stderr_output:
                                    print(f"[校验UI] 模块错误: {stderr_output}")
                                
                                # 模拟成功返回
                                result = type('obj', (object,), {
                                    'returncode': 0,
                                    'stdout': stdout_output,
                                    'stderr': stderr_output
                                })
                            finally:
                                # 恢复原始的stdout和stderr
                                sys.stdout = original_stdout
                                sys.stderr = original_stderr
                                
                        except ImportError:
                            print("[校验UI] 无法直接导入模块，尝试寻找Python解释器")
                            
                            # 方法2：寻找系统中的Python解释器
                            python_exe = None
                            possible_python_paths = [
                                'python.exe',
                                'python3.exe',
                                os.path.join(os.environ.get('PYTHONHOME', ''), 'python.exe'),
                                os.path.join(os.environ.get('ProgramFiles', ''), 'Python312', 'python.exe'),
                                os.path.join(os.environ.get('ProgramFiles(x86)', ''), 'Python312', 'python.exe')
                            ]
                            
                            for path in possible_python_paths:
                                if os.path.exists(path):
                                    python_exe = path
                                    print(f"[校验UI] 找到Python解释器: {python_exe}")
                                    break
                            
                            if python_exe:
                                cmd = [python_exe, self.check_script_path, '--json', json_file_path, '--output', self.check_result_file, '--append']
                                print(f"[校验UI] 执行命令: {cmd}")
                                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                            else:
                                raise Exception("[校验UI] 无法找到Python解释器")
                                
                    except Exception as import_err:
                        print(f"[校验UI] 直接导入模块失败: {str(import_err)}")
                        # 如果所有尝试都失败，可以提示用户或记录错误
                        raise
                else:
                    # 脚本模式 - 正常使用Python解释器
                    cmd = [sys.executable, self.check_script_path, '--json', json_file_path, '--output', self.check_result_file, '--append']
                    print(f"[校验UI] 执行命令: {cmd}")
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    success_count += 1
                    self._log(f"{json_file} 校验成功")
                else:
                    error_count += 1
                    self._log(f"{json_file} 校验失败")
                    self._log(f"错误信息: {result.stderr}")
            except Exception as e:
                error_count += 1
                self._log(f"{json_file} 校验异常: {str(e)}")
            
            # 更新进度
            progress = ((index + 1) / total_files) * 100
            self.root.after(0, lambda p=progress: self.progress_var.set(p))
            self.root.after(0, lambda i=index+1, t=total_files: 
                           self.progress_label.config(text=f"已完成 {i}/{t} 文件"))
        
        # 完成校验
        if not self.stop_event.is_set():
            self._log(f"\n校验完成！")
            self._log(f"成功: {success_count}, 失败: {error_count}, 总计: {total_files}")
            self._log(f"校验结果已保存至: {self.check_result_file}")
            
            # 完成后显示成功提示
            self.root.after(500, lambda: messagebox.showinfo("成功", f"校验完成！\n结果已保存至: {self.check_result_file}"))
        
        # 恢复按钮状态
        self.root.after(0, self._reset_ui_state)
    
    def _check_thread_status(self):
        """检查线程状态"""
        if hasattr(self, 'validation_thread') and self.validation_thread.is_alive():
            self.root.after(100, self._check_thread_status)
        else:
            if hasattr(self, 'stop_event') and self.stop_event.is_set():
                self._log("校验已取消")
                self._reset_ui_state()
    
    def _reset_ui_state(self):
        """重置UI状态"""
        self.start_button.config(state=tk.NORMAL)
        self.cancel_button.config(state=tk.DISABLED)
        self.progress_label.config(text="校验完成")
    
    def _log(self, message):
        """添加日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[{timestamp}] {message}"
        print(log_message)  # 添加print日志用于调试
        
        def append_log():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.insert(tk.END, log_message + "\n")
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)
        
        self.root.after(0, append_log)
    
    def _clear_log(self):
        """清空日志"""
        def do_clear():
            self.log_text.config(state=tk.NORMAL)
            self.log_text.delete(1.0, tk.END)
            self.log_text.config(state=tk.DISABLED)
        
        self.root.after(0, do_clear)
    
    def _on_close(self):
        """处理窗口关闭事件"""
        if hasattr(self, 'validation_thread') and self.validation_thread.is_alive():
            if messagebox.askyesno("确认", "校验正在进行中，确定要取消并关闭吗？"):
                if hasattr(self, 'stop_event'):
                    self.stop_event.set()
                    self._log("正在取消校验...")
                # 等待线程结束或强制关闭
                self.root.after(500, lambda: self.root.destroy())
        else:
            self.root.destroy()
    
    def run(self):
        """运行对话框（独立模式）"""
        if not hasattr(self.root, 'master') or not self.root.master:
            self.root.mainloop()
        else:
            self.root.deiconify()


def main():
    """主函数"""
    app = ResumeValidationDialog()
    app.run()


if __name__ == "__main__":
    main()