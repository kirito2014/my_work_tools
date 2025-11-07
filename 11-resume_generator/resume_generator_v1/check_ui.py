import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import os
import sys
import json
import subprocess
import threading
from datetime import datetime

# 导入ttkthemes以使用arc主题
try:
    from ttkthemes import ThemedTk
except ImportError:
    print("警告: 未找到ttkthemes模块，请先安装: pip install ttkthemes")
    # 如果没有ttkthemes，将ThemedTk设置为普通的tk.Tk作为备用
    ThemedTk = tk.Tk

# 设置项目根目录
base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_dir)

class ResumeValidationUI:
    def __init__(self, root):
        self.root = root
        self.root.title("简历校验工具")
        self.root.geometry("800x600")
        
        # 配置主题
        if hasattr(self.root, 'set_theme'):
            self.root.set_theme("arc")
        
        # 创建界面组件
        self._create_widgets()
        
        # 校验脚本路径
        self.check_script_path = os.path.join(base_dir, 'package', 'functions', 'check_resume_valid.py')
        
        # 确保output\checkExcel目录存在（使用正确的路径分隔符）
        self.check_dir = os.path.join(base_dir, "output", "checkExcel")
        if not os.path.exists(self.check_dir):
            os.makedirs(self.check_dir)
        
        # 校验结果文件路径
        self.check_result_file = os.path.join(self.check_dir, 'check_result.xlsx')
    
    def _create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 文件夹选择区域
        folder_frame = ttk.LabelFrame(main_frame, text="文件夹选择", padding="10")
        folder_frame.pack(fill=tk.X, pady=10)
        
        self.folder_var = tk.StringVar()
        folder_entry = ttk.Entry(folder_frame, textvariable=self.folder_var, width=60)
        folder_entry.pack(side=tk.LEFT, padx=5, pady=5, fill=tk.X, expand=True)
        
        select_button = ttk.Button(folder_frame, text="浏览", command=self._select_folder)
        select_button.pack(side=tk.RIGHT, padx=5, pady=5)
        
        # 控制按钮区域
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        self.start_button = ttk.Button(control_frame, text="开始校验", command=self._start_validation)
        self.start_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        self.cancel_button = ttk.Button(control_frame, text="取消", command=self._cancel_validation, state=tk.DISABLED)
        self.cancel_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # 进度条区域
        progress_frame = ttk.LabelFrame(main_frame, text="校验进度", padding="10")
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, length=100, mode='determinate')
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        
        self.progress_label = ttk.Label(progress_frame, text="准备就绪")
        self.progress_label.pack(anchor=tk.W, padx=5, pady=2)
        
        # 日志显示区域
        log_frame = ttk.LabelFrame(main_frame, text="校验日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, width=80, height=20)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 配置日志文本框样式
        self.log_text.config(state=tk.DISABLED)
    
    def _select_folder(self):
        """选择文件夹"""
        folder_path = filedialog.askdirectory(title="选择JSON文件所在文件夹")
        if folder_path:
            self.folder_var.set(folder_path)
    
    def _start_validation(self):
        """开始校验"""
        folder_path = self.folder_var.get()
        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showerror("错误", "请先选择有效的文件夹")
            return
        
        # 检查是否存在JSON文件
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        if not json_files:
            messagebox.showinfo("提示", "所选文件夹中没有JSON文件")
            return
        
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
    
    def _cancel_validation(self):
        """取消校验"""
        if hasattr(self, 'stop_event'):
            self.stop_event.set()
            self._log("正在取消校验...")
    
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
                # 调用校验脚本，指定输出文件路径并设置追加模式
                cmd = [
                    sys.executable,
                    self.check_script_path,
                    "--json",
                    json_file_path,
                    "--output",
                    self.check_result_file,
                    "--append"
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 设置超时时间
                )
                
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
            self._log(f"校验结果已保存至: {os.path.join(self.check_dir, 'check_result.xlsx')}")
        
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

def main():
    # 创建应用窗口
    root = ThemedTk(theme="arc")
    app = ResumeValidationUI(root)
    
    # 设置窗口图标（可选）
    icon_path = os.path.join(base_dir, 'resources', 'icons', 'sunline.ico')
    if os.path.exists(icon_path):
        try:
            root.iconbitmap(icon_path)
        except:
            pass
    
    # 运行主循环
    root.mainloop()

if __name__ == "__main__":
    main()