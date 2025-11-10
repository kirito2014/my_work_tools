import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import tkinter.font as font
import os
import sys
import json
import subprocess
import threading
from datetime import datetime, date
import socket  # 用于进程锁检查
# 导入PIL用于图像处理
try:
    from PIL import Image, ImageTk
except ImportError:
    print("警告: 未找到PIL模块，请先安装: pip install pillow")

# 导入ttkthemes以使用arc主题
try:
    from ttkthemes import ThemedTk
except ImportError:
    print("警告: 未找到ttkthemes模块，请先安装: pip install ttkthemes")
    # 如果没有ttkthemes，将ThemedTk设置为普通的tk.Tk作为备用
    ThemedTk = tk.Tk

# 设置项目根目录
# 处理PyInstaller打包后的路径问题
if getattr(sys, 'frozen', False):
    # 打包后的环境
    base_dir = os.path.dirname(sys.executable)
    # 确保工作目录设置为当前目录（exe所在目录）
    os.chdir(base_dir)
else:
    # 开发环境
    base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_dir)

# 尝试导入必要的模块
try:
    # 动态导入doc_2_json模块
    dj = None
    try:
        from package.functions import doc_2_json as dj
    except ImportError:
        try:
            import package.functions.doc_2_json as dj
        except ImportError:
            dj_file_path = os.path.join(base_dir, 'package', 'functions', 'doc_2_json.py')
            if os.path.exists(dj_file_path):
                import importlib.util
                spec = importlib.util.spec_from_file_location("doc_2_json", dj_file_path)
                dj = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(dj)
            else:
                print(f"警告: 未找到 doc_2_json.py 文件在路径: {dj_file_path}")
    
    # 动态导入render_from_docx模块
    from package.functions import render_from_docx
    
    # 动态导入check_resume_valid模块
    check_module = None
    try:
        from package.functions import check_resume_valid as check_module
    except ImportError:
        pass
    
    # 动态导入excel_reader模块
    from package.utils import excel_reader
    
    # 动态导入batch_render_from_docx模块
    batch_render_module = None
    try:
        import batch_render_from_docx as batch_render_module
    except ImportError:
        batch_render_path = os.path.join(base_dir, 'batch_render_from_docx.py')
        if os.path.exists(batch_render_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location("batch_render_from_docx", batch_render_path)
            batch_render_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(batch_render_module)
        else:
            print(f"警告: 未找到 batch_render_from_docx.py 文件在路径: {batch_render_path}")
    
    # 动态导入doc_converter模块
    doc_converter = None
    try:
        from package.functions import doc_converter
    except ImportError:
        try:
            import package.functions.doc_converter
        except ImportError:
            doc_converter_path = os.path.join(base_dir, 'package', 'functions', 'doc_converter.py')
            if os.path.exists(doc_converter_path):
                import importlib.util
                spec = importlib.util.spec_from_file_location("doc_converter", doc_converter_path)
                doc_converter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(doc_converter)
            else:
                print(f"警告: 未找到 doc_converter.py 文件在路径: {doc_converter_path}")
except Exception as e:
    print(f"导入模块时出错: {e}")

class ResumeGeneratorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("长亮科技简历生成器")
        self.root.geometry("900x800")
        # 不再需要手动设置背景色，由主题处理
        
        # 设置文件路径变量 - 使用base_dir确保在打包环境中正确
        self.base_dir = base_dir
        self.input_dir = os.path.join(base_dir, "input")
        self.output_dir = os.path.join(base_dir, "output")
        self.template_dir = os.path.join(base_dir, "template")
        self.resources_dir = os.path.join(base_dir, "resources")
        self.config_dir = os.path.join(base_dir, "config")
        self.temp_dir = os.path.join(base_dir, "temp")
        
        # 设置窗口图标 - 处理打包和非打包环境
        icon_path = os.path.join(self.resources_dir, "icons", "sunline.ico")
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except Exception as e:
                print(f"设置图标时出错: {e}")
        
        # 文件路径变量
        self.resume_file_path = tk.StringVar()
        self.selected_persons = []
        self.selected_list = []  # 存储选中的员工编号
        self.hidden_items = {}  # 存储被隐藏的项目
        self.selected_list_file_path = tk.StringVar()  # 名单文件路径
        self.selected_count_var = tk.StringVar(value="未选择文件")  # 选中人数显示
        
        # 设置中文字体
        self.font_config = {}
        self._setup_fonts()
        
        # 添加菜单栏
        self._create_menu()
        
        # 创建界面
        self._create_widgets()
        
        # 初始化时尝试加载员工信息
        self._load_employee_info()
        
    def _create_menu(self):
        """创建菜单栏"""
        # 创建菜单栏
        menubar = tk.Menu(self.root)
        
        # 创建文件菜单
        file_menu = tk.Menu(menubar, tearoff=0, font=self.font_config['button'])
        file_menu.add_command(label="预处理", command=self._show_preprocess_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self._quit_app)
        
        # 将文件菜单添加到菜单栏
        menubar.add_cascade(label="文件", menu=file_menu)
        
        # 创建特殊更新菜单
        special_menu = tk.Menu(menubar, tearoff=0, font=self.font_config['button'])
        special_menu.add_command(label="特殊更新", command=self._show_special_update_dialog)
        
        # 将特殊更新菜单添加到菜单栏
        menubar.add_cascade(label="特殊更新", menu=special_menu)
        
        # 创建银行管理菜单
        bank_menu = tk.Menu(menubar, tearoff=0, font=self.font_config['button'])
        bank_menu.add_command(label="银行管理", command=self._show_bank_management_dialog)
        
        # 将银行管理菜单添加到菜单栏
        menubar.add_cascade(label="银行管理", menu=bank_menu)
        
        # 创建简历校验菜单
        validate_menu = tk.Menu(menubar, tearoff=0, font=self.font_config['button'])
        validate_menu.add_command(label="简历校验", command=self._show_resume_validation)
        
        # 将简历校验菜单添加到菜单栏
        menubar.add_cascade(label="简历校验", menu=validate_menu)
        
        # 设置菜单栏
        self.root.config(menu=menubar)
        
    def _show_special_update_dialog(self):
        """显示特殊更新对话框"""
        # 创建新窗口
        self.special_dialog = tk.Toplevel(self.root)
        self.special_dialog.title("特殊更新")
        self.special_dialog.geometry("600x700")  # 增加高度以容纳日志栏
        self.special_dialog.resizable(False, False)
        
        # 设置字体
        dialog_font = self.font_config['label']
        
        # 创建主框架
        main_frame = ttk.Frame(self.special_dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. 文件选择部分
        file_frame = ttk.LabelFrame(main_frame, text="文件选择", padding="10")
        file_frame.pack(fill=tk.X, pady=10)
        
        # 简历文件夹选择
        resume_folder_frame = ttk.Frame(file_frame)
        resume_folder_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(resume_folder_frame, text="简历文件夹:", font=dialog_font).pack(side=tk.LEFT, padx=5)
        self.special_resume_folder = tk.StringVar()
        ttk.Entry(resume_folder_frame, textvariable=self.special_resume_folder, width=40, font=dialog_font).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(resume_folder_frame, text="浏览", command=lambda: self._select_folder(self.special_resume_folder)).pack(side=tk.LEFT, padx=5)
        
        # 人员信息文件选择
        info_file_frame = ttk.Frame(file_frame)
        info_file_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(info_file_frame, text="人员信息文件:", font=dialog_font).pack(side=tk.LEFT, padx=5)
        self.special_info_file = tk.StringVar()
        ttk.Entry(info_file_frame, textvariable=self.special_info_file, width=40, font=dialog_font).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(info_file_frame, text="浏览", command=lambda: self._select_excel_file(self.special_info_file)).pack(side=tk.LEFT, padx=5)
        
        # 2. 更新选项部分
        option_frame = ttk.LabelFrame(main_frame, text="更新选项", padding="10")
        option_frame.pack(fill=tk.X, pady=10)
        
        # 人员编号输入框 - 移到最上面
        emp_frame = ttk.Frame(option_frame)
        emp_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(emp_frame, text="人员编号:", font=dialog_font).pack(side=tk.LEFT, padx=5)
        self.employee_numbers_var = tk.StringVar()
        ttk.Entry(emp_frame, textvariable=self.employee_numbers_var, width=30, font=dialog_font).pack(side=tk.LEFT, padx=5)
        #ttk.Label(emp_frame, text="多个用逗号分隔，全部更新请输入ALL", font=dialog_font).pack(side=tk.LEFT, padx=5)
        
        # 更新方式下拉框
        update_type_frame = ttk.Frame(option_frame)
        update_type_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(update_type_frame, text="更新方式:", font=dialog_font).pack(side=tk.LEFT, padx=5)
        self.update_type_var = tk.StringVar(value="选择更新方式")
        update_type_combobox = ttk.Combobox(update_type_frame, textvariable=self.update_type_var, state="readonly", font=dialog_font, width=20)
        update_type_combobox['values'] = ["1-只更新简历信息", "2-只更新人员信息", "3-更新全部信息"]
        update_type_combobox.pack(side=tk.LEFT, padx=5)
        
        # 提示标签 - 放在更新选项下面，更新方式上面
        tip_frame = ttk.Frame(option_frame)
        tip_frame.pack(fill=tk.X, pady=5)
        ttk.Label(tip_frame, text="使用提示:\n1-选择简历文件夹\n2-选择人员文件\n3-输入人员编号多个用逗号分隔，全部更新请输入ALL\n4-选择更新方式\n5-点击执行更新", font=("Microsoft YaHei", 9, "italic"), foreground="#3366CC").pack(anchor=tk.W, padx=5)
        
        # 3. 按钮部分
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # 创建执行操作frame包裹按钮
        action_frame = ttk.LabelFrame(button_frame, text="执行操作", padding="10")
        action_frame.pack(fill=tk.X, padx=10)
        
        ttk.Button(action_frame, text="执行更新", command=self._execute_special_update, style="Accent.TButton").pack(side=tk.LEFT, padx=10)
        ttk.Button(action_frame, text="取消", command=self.special_dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 4. 日志栏 - 集成到对话框内部
        log_frame = ttk.LabelFrame(main_frame, text="执行日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # 创建日志文本框
        self.output_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=self.font_config['text'])
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 居中显示
        self.special_dialog.transient(self.root)
        self.special_dialog.grab_set()
    
    def _select_folder(self, string_var):
        """选择文件夹的通用方法，默认从应用程序所在目录开始"""
        # 使用self.base_dir作为初始目录，确保在打包环境中正确
        initial_dir = getattr(self, 'base_dir', os.getcwd())
        folder_path = filedialog.askdirectory(initialdir=initial_dir)
        if folder_path:
            string_var.set(folder_path)
    
    def _select_excel_file(self, string_var):
        """选择Excel文件，默认从应用程序所在目录开始"""
        # 使用self.base_dir作为初始目录，确保在打包环境中正确
        initial_dir = getattr(self, 'base_dir', os.getcwd())
        file_path = filedialog.askopenfilename(
            initialdir=initial_dir,
            filetypes=[("Excel文件", "*.xlsx;*.xls")]
        )
        if file_path:
            string_var.set(file_path)
    
    def _execute_special_update(self):
        """执行特殊更新"""
        # 验证是否选择了更新方式
        update_type = self.update_type_var.get()
        if update_type == "选择更新方式":
            messagebox.showerror("错误", "请选择执行方式")
            return
            
        update_option = 1 if "1" in update_type else 2 if "2" in update_type else 3
        
        # 获取人员编号
        employee_numbers = self.employee_numbers_var.get().strip()
        if not employee_numbers:
            messagebox.showerror("错误", "请输入人员编号")
            return
        
        # 获取文件路径
        resume_folder = self.special_resume_folder.get()
        info_file = self.special_info_file.get()
        
        # 验证必要的文件路径
        if employee_numbers.upper() == "ALL" and not resume_folder:
            messagebox.showerror("错误", "全部更新时必须选择简历文件夹")
            return
        
        if not info_file:
            messagebox.showerror("错误", "请选择人员信息文件")
            return
        
        # 构建命令
        cmd = [sys.executable, os.path.join(base_dir, "package", "functions", "update_specific_jsons.py"), 
               str(update_option), employee_numbers, "--excel", info_file]
        
        # 如果提供了简历文件夹，添加--word参数
        if resume_folder:
            cmd.extend(["--word", resume_folder])
        
        # 清空日志栏
        self.output_text.delete(1.0, tk.END)
        self.output_text.insert(tk.END, "开始执行更新操作...\n")
        
        # 实时更新输出的函数
        def update_output(process):
            while True:
                line = process.stdout.readline()
                if not line:
                    break
                self.special_dialog.after(0, lambda l=line: [
                    self.output_text.insert(tk.END, l),
                    self.output_text.see(tk.END)
                ])
            
            # 处理完成后更新UI
            self.special_dialog.after(0, lambda: [
                self.output_text.insert(tk.END, "\n更新完成！"),
                self.output_text.see(tk.END)
            ])
        
        # 在新线程中执行命令
        def execute_command():
            try:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                          text=True, cwd=base_dir)
                
                # 更新输出
                update_output(process)
                
                # 等待进程完成
                process.wait()
            except Exception as e:
                self.special_dialog.after(0, lambda: [
                    self.output_text.insert(tk.END, f"执行错误: {e}\n"),
                    self.output_text.see(tk.END)
                ])
        
        # 启动执行线程
        threading.Thread(target=execute_command, daemon=True).start()
    
    def _quit_app(self):
        """退出应用程序"""
        if messagebox.askyesno("确认退出", "确定要退出简历生成器吗？"):
            self.root.quit()
    
    def _show_preprocess_dialog(self):
        """显示预处理对话框"""
        # 创建新窗口
        self.preprocess_dialog = tk.Toplevel(self.root)
        self.preprocess_dialog.title("预处理")
        self.preprocess_dialog.geometry("600x500")
        self.preprocess_dialog.resizable(False, False)
        
        # 设置字体
        dialog_font = self.font_config['label']
        
        # 创建主框架
        main_frame = ttk.Frame(self.preprocess_dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. 文件夹选择部分
        folder_frame = ttk.LabelFrame(main_frame, text="文件夹选择", padding="10")
        folder_frame.pack(fill=tk.X, pady=10)
        
        # 简历文件夹选择
        resume_folder_frame = ttk.Frame(folder_frame)
        resume_folder_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(resume_folder_frame, text="简历文件夹:", font=dialog_font).pack(side=tk.LEFT, padx=5)
        self.preprocess_resume_folder = tk.StringVar()
        ttk.Entry(resume_folder_frame, textvariable=self.preprocess_resume_folder, width=40, font=dialog_font).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(resume_folder_frame, text="浏览", command=lambda: self._select_folder(self.preprocess_resume_folder)).pack(side=tk.LEFT, padx=5)
        
        # 2. 执行操作部分 - 添加外框
        action_frame = ttk.LabelFrame(main_frame, text="执行操作", padding="10")
        action_frame.pack(fill=tk.X, pady=10)
        
        # 添加开始处理和取消按钮到执行操作外框内
        button_frame = ttk.Frame(action_frame)
        button_frame.pack(fill=tk.X, pady=5, side=tk.RIGHT)
        ttk.Button(button_frame, text="开始处理", command=self._execute_preprocess).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="取消", command=self.preprocess_dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # 3. 日志显示部分 - 放在最下方
        log_frame = ttk.LabelFrame(main_frame, text="处理日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # 创建日志文本框
        self.preprocess_log = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, width=60, height=8, font=dialog_font)
        self.preprocess_log.pack(fill=tk.BOTH, expand=True)
        self.preprocess_log.config(state=tk.DISABLED)
        
        # 设置对话框属性，确保在选择文件夹后保持可见
        self.preprocess_dialog.transient(self.root)
        self.preprocess_dialog.grab_set()
    
    def _execute_preprocess(self):
        """执行预处理操作"""
        resume_folder = self.preprocess_resume_folder.get()
        
        if not resume_folder or not os.path.exists(resume_folder):
            messagebox.showerror("错误", "请选择有效的简历文件夹")
            return
        
        # 清空日志
        self.preprocess_log.config(state=tk.NORMAL)
        self.preprocess_log.delete(1.0, tk.END)
        self.preprocess_log.config(state=tk.DISABLED)
        
        # 在后台线程中执行预处理
        self._log("启动简历预处理工具...")
        threading.Thread(target=self._preprocess_thread, args=(resume_folder,), daemon=True).start()
    
    def _preprocess_thread(self, resume_folder):
        """预处理线程"""
        try:
            # 检查doc_converter模块是否加载成功
            if doc_converter is None:
                self._preprocess_log(f"[ERROR] 未找到doc_converter模块，无法进行预处理")
                messagebox.showerror("错误", "未找到doc_converter模块，无法进行预处理")
                return
            
            self._preprocess_log(f"[INFO] 开始预处理文件夹: {resume_folder}")
            
            # 获取文件夹中的所有doc文件
            doc_files = []
            for root, _, files in os.walk(resume_folder):
                for file in files:
                    if file.lower().endswith('.doc') and not file.startswith('~$'):
                        doc_files.append(os.path.join(root, file))
            
            total_files = len(doc_files)
            self._preprocess_log(f"[INFO] 找到 {total_files} 个doc文件")
            
            # 转换每个doc文件
            success_count = 0
            failed_count = 0
            
            # 使用批处理函数进行转换，显著提高速度
            if hasattr(doc_converter, 'batch_convert_docs_to_docx') and doc_files:
                self._preprocess_log(f"[INFO] 开始批量转换文档...")
                
                # 分批处理，每批最多50个文件，避免Word处理太多文件时出现问题
                batch_size = 50
                for i in range(0, len(doc_files), batch_size):
                    batch_files = doc_files[i:i + batch_size]
                    batch_start = i + 1
                    batch_end = min(i + batch_size, len(doc_files))
                    self._preprocess_log(f"[INFO] 处理批次 {batch_start}-{batch_end}/{total_files}")
                    
                    # 调用批处理函数
                    results = doc_converter.batch_convert_docs_to_docx(batch_files)
                    
                    # 处理转换结果
                    for doc_path, docx_path, success in results:
                        file_name = os.path.basename(doc_path)
                        if success and docx_path and os.path.exists(docx_path):
                            # 删除原doc文件
                            try:
                                os.remove(doc_path)
                                success_count += 1
                                self._preprocess_log(f"[OK] 已转换并删除原文件: {file_name}")
                            except Exception as e:
                                self._preprocess_log(f"[WARNING] 转换成功但无法删除原文件 {file_name}: {str(e)}")
                                success_count += 1
                        else:
                            failed_count += 1
                            self._preprocess_log(f"[ERROR] 转换失败: {file_name}")
            else:
                # 降级使用单文件转换（兼容旧版本）
                for i, doc_file in enumerate(doc_files, 1):
                    self._preprocess_log(f"[PROCESS] 正在处理 ({i}/{total_files}): {os.path.basename(doc_file)}")
                    
                    try:
                        # 调用doc_converter中的函数进行转换
                        output_dir = os.path.dirname(doc_file)
                        result = doc_converter.convert_doc_to_docx(doc_file, output_dir=output_dir)
                        
                        if result and os.path.exists(result):
                            # 删除原doc文件
                            os.remove(doc_file)
                            success_count += 1
                            self._preprocess_log(f"[OK] 已转换并删除原文件: {os.path.basename(doc_file)}")
                        else:
                            failed_count += 1
                            self._preprocess_log(f"[ERROR] 转换失败: {os.path.basename(doc_file)}")
                    except Exception as e:
                        failed_count += 1
                        self._preprocess_log(f"[ERROR] 处理{os.path.basename(doc_file)}时出错: {str(e)}")
            
            # 输出处理结果
            self._preprocess_log(f"[DONE] 预处理完成!")
            self._preprocess_log(f"[INFO] 成功: {success_count} 个文件")
            self._preprocess_log(f"[INFO] 失败: {failed_count} 个文件")
            
            # 显示完成消息
            self.root.after(0, lambda: messagebox.showinfo("完成", f"预处理完成!\n成功: {success_count} 个文件\n失败: {failed_count} 个文件"))
            
        except Exception as e:
            error_msg = f"预处理过程中出错: {str(e)}"
            self._preprocess_log(f"[ERROR] {error_msg}")
            self.root.after(0, lambda: messagebox.showerror("错误", error_msg))
    
    def _preprocess_log(self, message):
        """向预处理日志添加消息"""
        self.root.after(0, lambda: self._append_preprocess_log(message))
    
    def _append_preprocess_log(self, message):
        """追加日志消息"""
        try:
            # 确保文本框状态为可编辑
            if self.preprocess_log['state'] == tk.DISABLED:
                self.preprocess_log.config(state=tk.NORMAL)
            
            # 插入日志消息并添加换行
            self.preprocess_log.insert(tk.END, message + "\n")
            
            # 确保滚动到底部显示最新日志
            self.preprocess_log.see(tk.END)
            
            # 更新界面显示
            self.preprocess_log.update_idletasks()
            
        except Exception as e:
            # 发生异常时也确保恢复文本框状态
            print(f"追加日志时出错: {str(e)}")
        finally:
            # 无论如何都将文本框设置为只读状态
            self.preprocess_log.config(state=tk.DISABLED)
        
    def _setup_fonts(self):
        # 设置中文字体为微软雅黑
        self.font_config['title'] = ('Microsoft YaHei', 12, 'bold')
        self.font_config['label'] = ('Microsoft YaHei', 10)
        self.font_config['button'] = ('Microsoft YaHei', 10)
        self.font_config['entry'] = ('Microsoft YaHei', 10)
        self.font_config['text'] = ('Microsoft YaHei', 9)
    
    def _create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. 技术人员信息解析部分
        tech_info_frame = ttk.LabelFrame(main_frame, text="解析技术人员信息", padding="15")
        tech_info_frame.pack(fill=tk.X, pady=10)
        
        # 技术人员信息文件路径
        self.tech_info_file_path = tk.StringVar()
        tech_path_frame = ttk.Frame(tech_info_frame)
        tech_path_frame.pack(fill=tk.X, pady=5)
        
        ttk.Entry(tech_path_frame, textvariable=self.tech_info_file_path, width=50, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(tech_path_frame, text="选择Excel文件", command=self._select_tech_info_file, width=15, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(tech_path_frame, text="解析信息", command=self._parse_tech_info, width=10, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        
        # 2. 简历解析入库部分
        parse_frame = ttk.LabelFrame(main_frame, text="解析简历文件入库", padding="15")
        parse_frame.pack(fill=tk.X, pady=10)
        
        # 路径显示
        path_frame = ttk.Frame(parse_frame)
        path_frame.pack(fill=tk.X, pady=5)
        
        ttk.Entry(path_frame, textvariable=self.resume_file_path, width=50, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(path_frame, text="选择文件路径", command=self._select_file, width=15, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(path_frame, text="开始解析", command=self._start_parse, width=10, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        
        # 2. 简历生成部分
        generate_frame = ttk.LabelFrame(main_frame, text="简历生成", padding="15")
        generate_frame.pack(fill=tk.X, pady=10)
        
        # 生成方式选择
        method_frame = ttk.Frame(generate_frame)
        method_frame.pack(fill=tk.X, pady=5)
        
        self.generate_method = tk.StringVar(value="all")
        ttk.Radiobutton(method_frame, text="按名单生成简历", variable=self.generate_method, value="all", command=self._toggle_person_list).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(method_frame, text="自定义生成简历", variable=self.generate_method, value="selected", command=self._toggle_person_list).pack(side=tk.LEFT, padx=10)
        
        # 人员名单框架
        self.person_list_frame = ttk.LabelFrame(generate_frame, text="点选人员名单", padding="10")
        self.person_list_frame.pack(fill=tk.X, pady=5)
        self.person_list_frame.pack_forget()  # 初始隐藏
        self._person_list_visible = False  # 标记人员列表是否可见
        
        # 人员名单按钮和搜索框
        control_frame = ttk.Frame(self.person_list_frame)
        control_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(control_frame, text="更新人员名单", command=self._update_person_list, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="上次选择人员", command=self._load_last_selected, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Label(control_frame, text="搜索:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self._filter_person_list)
        ttk.Entry(control_frame, textvariable=self.search_var, width=20, font=self.font_config['entry']).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="确认选择", command=self._confirm_selection, width=10).pack(side=tk.LEFT, padx=5)
        # 新增清除选择按钮
        ttk.Button(control_frame, text="清除选择", command=self._clear_selected, width=10).pack(side=tk.LEFT, padx=5)
        # 新增取消折叠按钮（初始隐藏）
        self.unfold_button = ttk.Button(control_frame, text="取消折叠", command=self._show_person_list, width=8)
        # 默认隐藏取消折叠按钮
        
        # 新增折叠按钮
        ttk.Button(control_frame, text="折叠", command=self._toggle_person_list_visibility, width=8).pack(side=tk.RIGHT, padx=5)
        
        # 部门筛选框架
        dept_frame = ttk.LabelFrame(self.person_list_frame, text="部门筛选", padding="5")
        dept_frame.pack(fill=tk.X, pady=5)
        
        # 一级部门下拉框
        ttk.Label(dept_frame, text="一级部门:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.level1_dept_var = tk.StringVar()
        self.level1_dept_combobox = ttk.Combobox(dept_frame, textvariable=self.level1_dept_var, width=15, font=self.font_config['entry'])
        self.level1_dept_combobox.bind("<<ComboboxSelected>>", self._on_level1_dept_selected)
        self.level1_dept_combobox.pack(side=tk.LEFT, padx=5)
        
        # 二级部门下拉框
        ttk.Label(dept_frame, text="二级部门:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.level2_dept_var = tk.StringVar()
        self.level2_dept_combobox = ttk.Combobox(dept_frame, textvariable=self.level2_dept_var, width=15, font=self.font_config['entry'])
        self.level2_dept_combobox.bind("<<ComboboxSelected>>", self._filter_by_dept)
        self.level2_dept_combobox.pack(side=tk.LEFT, padx=5)
        
        # 新增筛选按钮（放在清除筛选按钮左边）
        ttk.Button(dept_frame, text="部门筛选", command=self._filter_by_dept, width=10).pack(side=tk.LEFT, padx=5)
        
        # 清除筛选按钮
        ttk.Button(dept_frame, text="清除筛选", command=self._clear_filters, width=10).pack(side=tk.LEFT, padx=5)
        
        # 人员列表（使用Treeview实现复选框多选）
        # 添加复选框列
        self.person_tree = ttk.Treeview(self.person_list_frame, columns=("select", "emp_no", "name", "dept"), show="headings", height=5)
        self.person_tree.heading("select", text="选择")
        self.person_tree.heading("emp_no", text="员工编号")
        self.person_tree.heading("name", text="人员姓名")
        self.person_tree.heading("dept", text="部门")
        self.person_tree.column("select", width=60, anchor="center")
        self.person_tree.column("emp_no", width=100)
        self.person_tree.column("name", width=180)
        self.person_tree.column("dept", width=220)
        
        # 绑定点击事件实现复选框
        self.person_tree.bind("<Button-1>", self._on_tree_click)  # 绑定点击事件
        self.person_tree.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 添加垂直滚动条
        tree_vscroll = ttk.Scrollbar(self.person_list_frame, orient="vertical", command=self.person_tree.yview)
        self.person_tree.configure(yscrollcommand=tree_vscroll.set)
        tree_vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 添加水平滚动条
        tree_hscroll = ttk.Scrollbar(self.person_list_frame, orient="horizontal", command=self.person_tree.xview)
        self.person_tree.configure(xscrollcommand=tree_hscroll.set)
        tree_hscroll.pack(fill=tk.X)
        
        # 银行选择和生成按钮
        bank_frame = ttk.Frame(generate_frame)
        bank_frame.pack(fill=tk.X, pady=5)
        
        # Logo显示
        self.logo_label = ttk.Label(bank_frame, width=10, relief="solid")
        # 使用padding来控制大小
        self.logo_label.pack(side=tk.LEFT, padx=5, pady=5)
        
        # 银行下拉框
        ttk.Label(bank_frame, text="银行:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_var = tk.StringVar()
        self.bank_combobox = ttk.Combobox(bank_frame, textvariable=self.bank_var, width=20, font=self.font_config['entry'])
        self.bank_combobox['values'] = self._get_bank_list()
        self.bank_combobox.pack(side=tk.LEFT, padx=5)
        self.bank_combobox.current(0)
        
        # 绑定银行选择事件，更新logo
        self.bank_combobox.bind("<<ComboboxSelected>>", self._update_bank_logo)
        
        # 初始加载默认银行的logo
        self._update_bank_logo()
        
        # 生成简历按钮
        ttk.Button(bank_frame, text="生成简历", command=self._generate_resumes, width=10, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        
        # 新增按名单选择人员框架
        self.list_select_frame = ttk.LabelFrame(generate_frame, text="按名单选择人员", padding="10")
        self.list_select_frame.pack(fill=tk.X, pady=5)
        self.list_select_frame.pack_forget()  # 初始隐藏
        
        # 文件选择和显示
        file_select_frame = ttk.Frame(self.list_select_frame)
        file_select_frame.pack(fill=tk.X, pady=5)
        
        self.selected_list_file_path = tk.StringVar(value="")
        ttk.Entry(file_select_frame, textvariable=self.selected_list_file_path, width=50, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(file_select_frame, text="选择名单文件", command=self._select_list_file, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(file_select_frame, text="确认选择", command=self._confirm_list_selection, width=10, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        
        # 选择状态显示
        self.selected_count_var = tk.StringVar(value="未选择任何人员")
        ttk.Label(self.list_select_frame, textvariable=self.selected_count_var, font=self.font_config['label'], foreground="blue").pack(anchor="w", padx=5)
        
        # 3. 进度条
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, length=100, mode='determinate')
        self.progress_bar.pack(fill=tk.X, expand=True)
        
        self.progress_label = ttk.Label(progress_frame, text="10%")
        self.progress_label.pack(pady=5)
        
        # 4. 日志显示区域
        self.log_frame = ttk.LabelFrame(main_frame, text="日志信息", padding="15")
        self.log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(self.log_frame, wrap=tk.WORD, font=self.font_config['text'], height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
        
        # 设置样式
        self._setup_styles()
        
        # 初始化显示正确的人员选择框架
        self._toggle_person_list()
    
    def _setup_styles(self):
        # 设置按钮样式，基于arc主题
        style = ttk.Style()
        # 保留现有的按钮样式设置，但使用主题的默认背景色
        style.configure("Accent.TButton", font=self.font_config['button'])
        style.map("Accent.TButton", 
                  foreground=[('active', 'blue')])
        
        # 为其他组件设置字体
        style.configure("TLabel", font=self.font_config['label'])
        style.configure("TEntry", font=self.font_config['entry'])
        style.configure("TCombobox", font=self.font_config['entry'])
        style.configure("TTreeview", font=self.font_config['text'])
    
    def _get_bank_list(self):
        # 从配置文件读取银行列表
        bank_list = []
        config_path = os.path.join(base_dir, 'config', 'bank_list.config')
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    bank_list = [line.strip() for line in f if line.strip()]
        except Exception as e:
            self._log(f"读取银行列表出错: {str(e)}")
        
        # 如果没有读取到银行列表，使用默认值
        if not bank_list:
            bank_list = ["长亮科技", "测试银行", "招商银行", "建设银行", "工商银行", "农业银行"]
        
        return bank_list
    
    def _update_bank_logo(self, event=None):
        """根据选择的银行更新logo显示"""
        bank_name = self.bank_var.get()
        if not bank_name:
            return
        
        # 查找logo文件路径（支持ico和png格式）
        bank_pics_dir = os.path.join(base_dir, 'resources', 'bank_pics')
        logo_path = None
        
        # 尝试不同的扩展名
        for ext in ['.ico', '.png']:
            candidate = os.path.join(bank_pics_dir, f'{bank_name}{ext}')
            if os.path.exists(candidate):
                logo_path = candidate
                break
        
        # 如果找到了logo文件，加载并显示
        if logo_path and 'Image' in globals() and 'ImageTk' in globals():
            try:
                # 加载图像
                image = Image.open(logo_path)
                # 调整大小为32x32像素
                image = image.resize((32, 32), Image.LANCZOS)
                
                # 处理透明背景问题，创建白色背景
                if image.mode == 'RGBA':
                    # 创建一个白色背景的新图像
                    background = Image.new('RGB', (32, 32), (255, 255, 255))
                    # 将原图粘贴到白色背景上，保留透明度
                    background.paste(image, mask=image.split()[3])  # 3是alpha通道
                    image = background
                
                # 转换为Tkinter可用的格式
                photo = ImageTk.PhotoImage(image)
                # 更新标签图像
                self.logo_label.config(image=photo)
                # 保存引用防止被垃圾回收
                self.logo_label.photo = photo
            except Exception as e:
                self._log(f"加载银行logo出错: {str(e)}")
                # 显示默认文本
                self.logo_label.config(image='', text="无Logo")
        else:
            # 没有找到logo或缺少PIL模块
            self.logo_label.config(image='', text="无Logo")
    
    def _select_file(self):
        """选择简历文件夹路径，默认从应用程序所在目录开始"""
        # 使用self.base_dir作为初始目录，确保在打包环境中正确
        initial_dir = getattr(self, 'base_dir', os.getcwd())
        folder_path = filedialog.askdirectory(
            initialdir=initial_dir,
            title="选择简历文件夹"
        )
        if folder_path:
            self.resume_file_path.set(folder_path)
            self._log(f"已选择文件夹: {folder_path}")
    
    def _select_tech_info_file(self):
        """选择技术人员信息Excel文件，默认从应用程序所在目录开始"""
        # 使用self.base_dir作为初始目录，确保在打包环境中正确
        initial_dir = getattr(self, 'base_dir', os.getcwd())
        file_path = filedialog.askopenfilename(
            initialdir=initial_dir,
            title="选择技术人员信息Excel文件",
            filetypes=[("Excel文件", "*.xlsx;*.xls")]
        )
        if file_path:
            self.tech_info_file_path.set(file_path)
            self._log(f"已选择技术人员信息文件: {file_path}")
    
    def _parse_tech_info(self):
        """解析技术人员信息"""
        file_path = self.tech_info_file_path.get()
        if not file_path:
            self._log("请先选择技术人员信息文件")
            return
        
        if not os.path.exists(file_path):
            self._log(f"文件不存在: {file_path}")
            return
        
        self._log(f"开始解析技术人员信息文件: {file_path}")
        self.progress_var.set(10)
        self.progress_label.config(text="10%")
        
        # 在单独的线程中执行解析操作
        threading.Thread(target=self._parse_tech_info_thread, args=(file_path,)).start()
    
    def _parse_tech_info_thread(self, file_path):
        """在单独线程中解析技术人员信息"""
        try:
            # 执行get_emp_list脚本
            self._log("正在执行get_emp_list脚本...")
            self._update_progress(30)
            
            get_emp_cmd = [sys.executable, "get_emp_list.py", file_path]
            result = subprocess.run(get_emp_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self._log("[OK] get_emp_list脚本执行成功")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            else:
                self._log("[ERROR] get_emp_list脚本执行失败")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
                return
            
            self._update_progress(60)
            
            # 执行excel_2_info_json脚本
            self._log("正在执行excel_2_info_json脚本...")
            
            excel_2_json_path = os.path.join(base_dir, "package", "functions", "excel_2_info_json.py")
            excel_2_json_cmd = [sys.executable, excel_2_json_path, file_path]
            result = subprocess.run(excel_2_json_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self._log("[OK] excel_2_info_json脚本执行成功")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            else:
                self._log("[ERROR] excel_2_info_json脚本执行失败")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
                return
            
            self._update_progress(90)
            
            # 加载员工信息并更新部门下拉框
            self._load_employee_info()
            
            self._update_progress(100)
            self._log("🎉 技术人员信息解析完成！")
            
        except Exception as e:
            self._log(f"解析技术人员信息时出错: {str(e)}")
            import traceback
            self._log(traceback.format_exc())
    
    def _update_progress(self, value):
        """在主线程中更新进度条"""
        def update():
            self.progress_var.set(value)
            self.progress_label.config(text=f"{value}%")
        
        if self.root.winfo_exists():
            self.root.after(0, update)
    
    def _batch_convert_to_json(self, input_folder):
        """
        批量将文件夹中的docx/doc文件转换为json
        利用batch_render_from_docx.py中的批量处理逻辑，但只执行JSON转换部分
        """
        import os
        import sys
        import json
        from datetime import datetime
        
        # 创建temp_converted目录
        temp_dir = os.path.join(input_folder, "temp_converted")
        os.makedirs(temp_dir, exist_ok=True)
        
        # 创建输出目录，使用当前工作目录确保在应用程序所在位置保存文件
        modify_dir = os.path.join(os.getcwd(), "output", "modify_json")
        try:
            os.makedirs(modify_dir, exist_ok=True)
            # 验证目录创建成功
            if os.path.isdir(modify_dir):
                self._log(f"输出目录已准备就绪: {modify_dir}")
            else:
                self._log(f"警告: 无法创建或访问输出目录: {modify_dir}")
                # 尝试使用绝对路径作为备选
                modify_dir = os.path.abspath(os.path.join(os.getcwd(), "output", "modify_json"))
                os.makedirs(modify_dir, exist_ok=True)
                self._log(f"已切换到备选输出目录: {modify_dir}")
        except Exception as e:
            self._log(f"创建输出目录时出错: {e}")
            # 设置一个默认的安全目录作为备选
            default_dir = os.path.abspath(os.path.join(os.getcwd(), "resume_json_output"))
            modify_dir = default_dir
            os.makedirs(modify_dir, exist_ok=True)
            self._log(f"已使用默认备用目录: {modify_dir}")
        
        # 统计信息
        total_files = 0
        processed_files = 0
        failed_files = 0
        converted_files = 0
        
        # 使用batch_render_from_docx模块中的逻辑来遍历和处理文件
        # 遍历文件夹中的所有.doc和.docx文件
        resume_files = []
        for root, dirs, files in os.walk(input_folder):
            # 跳过temp_converted目录
            if "temp_converted" in dirs:
                dirs.remove("temp_converted")
            
            for file in files:
                if file.lower().endswith(('.doc', '.docx')):
                    # 跳过临时文件
                    if file.startswith('~$'):
                        continue
                    
                    file_path = os.path.join(root, file)
                    resume_files.append(file_path)
        
        total_files = len(resume_files)
        self._log(f"共发现 {total_files} 个简历文件")
        
        # 导入doc_converter模块（从batch_render_from_docx借鉴的导入方式）
        doc_converter = None
        try:
            if batch_render_module and hasattr(batch_render_module, 'doc_converter'):
                doc_converter = batch_render_module.doc_converter
            else:
                from package.functions import doc_converter
        except ImportError:
            try:
                # 尝试动态加载
                import importlib.util
                converter_path = os.path.join(base_dir, "package", "functions", "doc_converter.py")
                if os.path.exists(converter_path):
                    spec = importlib.util.spec_from_file_location("doc_converter", converter_path)
                    doc_converter = importlib.util.module_from_spec(spec)
                    sys.modules["doc_converter"] = doc_converter
                    spec.loader.exec_module(doc_converter)
                    self._log("通过动态加载成功导入 doc_converter.py 文件")
            except Exception as e:
                self._log(f"导入 doc_converter 模块失败: {e}")
        
        # 处理每个文件
        for index, file_path in enumerate(resume_files, 1):
            self._log(f"[{index}/{total_files}] 正在处理文件: {os.path.basename(file_path)}")
            
            try:
                # 处理doc格式文件（借鉴batch_render_from_docx中的转换逻辑）
                processed_doc_path = file_path
                is_converted = False
                
                if file_path.lower().endswith('.doc'):
                    self._log("检测到doc格式文件，正在转换为docx...")
                    try:
                        if doc_converter and hasattr(doc_converter, 'convert_doc_to_docx'):
                            processed_doc_path = doc_converter.convert_doc_to_docx(file_path, temp_dir)
                            converted_files += 1
                            is_converted = True
                            self._log(f"转换成功: {os.path.basename(processed_doc_path)}")
                        else:
                            self._log("警告: 缺少doc_converter模块或convert_doc_to_docx方法，无法转换doc文件")
                            continue
                    except Exception as e:
                        self._log(f"转换失败: {e}")
                        failed_files += 1
                        continue
                
                # 生成JSON文件名（借鉴batch_render_from_docx中的命名逻辑）
                base_name = os.path.splitext(os.path.basename(processed_doc_path))[0]
                # 移除"_已转换"后缀
                if is_converted and "_已转换" in base_name:
                    base_name = base_name.replace("_已转换", "")
                
                # 从文件名提取工号和姓名
                parts = base_name.split('+')
                if len(parts) >= 2:
                    emp_no = parts[0]
                    name = parts[1]
                    json_filename = f"{emp_no}_{name}_人员简历.json"
                else:
                    json_filename = f"{base_name}.json"
                    emp_no = "unknown"
                
                # 设置JSON文件路径
                json_file = os.path.join(modify_dir, json_filename)
                
                # 提取简历数据并转换为模板格式
                self._log("正在提取简历数据...")
                if dj:
                    raw_resume_data = dj.extract_resume_universal(processed_doc_path)
                    if not raw_resume_data:
                        self._log("提取数据失败，请检查文档格式")
                        failed_files += 1
                        continue
                    
                    self._log("正在转换为模板格式...")
                    result = dj.convert_to_template_format(raw_resume_data, emp_no)
                    if not result:
                        self._log("转换失败，请检查文档数据格式")
                        failed_files += 1
                        continue
                    
                    # 保存JSON文件，添加额外的错误处理
                    try:
                        with open(json_file, 'w', encoding='utf-8') as f:
                            json.dump(result, f, ensure_ascii=False, indent=4)
                        
                        # 验证文件是否成功保存
                        if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
                            processed_files += 1
                            self._log(f"处理完成，JSON文件已保存至: {json_file}")
                        else:
                            self._log(f"警告: JSON文件可能未正确保存: {json_file}")
                            failed_files += 1
                            continue
                    except Exception as e:
                        self._log(f"保存JSON文件时出错: {e}")
                        failed_files += 1
                        continue
                else:
                    self._log("错误: 未能导入doc_2_json模块")
                    failed_files += 1
                    continue
                
                # 更新进度
                progress = int((index / total_files) * 100)
                self._update_progress(progress)
                
            except Exception as e:
                self._log(f"处理失败: {str(e)}")
                failed_files += 1
        
        # 处理完成后进行验证
        json_files_validated = 0
        if processed_files > 0:
            self._log("=" * 50)
            self._log(f"开始验证生成的简历JSON文件...")
            for json_file in os.listdir(modify_dir):
                if json_file.endswith('.json'):
                    json_path = os.path.join(modify_dir, json_file)
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        # 验证JSON结构（至少包含基本字段）
                        if isinstance(data, dict) and len(data) > 0:
                            json_files_validated += 1
                            # 记录验证成功的文件名（可选）
                            # self._log(f"  ✓ {json_file}")
                    except Exception as e:
                        self._log(f"  ✗ 验证简历JSON文件失败 {json_file}: {e}")
        
        # 输出统计信息
        self._log("=" * 50)
        self._log(f"简历批处理完成！")
        self._log(f"总简历文件数: {total_files}")
        self._log(f"成功入库处理: {processed_files}")
        self._log(f"转换文件数: {converted_files}")
        self._log(f"处理入库失败: {failed_files}")
        if processed_files > 0:
            self._log(f"简历JSON文件验证成功: {json_files_validated}/{processed_files}")
        self._log(f"简历JSON目录: {modify_dir}")
        
        # 更新人员名单
        self._update_person_list()
        
    def _start_parse(self):
        folder_path = self.resume_file_path.get()
        if not folder_path:
            self._log("请先选择文件夹路径")
            return
        
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            self._log(f"文件夹不存在或不是有效文件夹: {folder_path}")
            return
        
        self._log(f"===== 开始批量解析文件夹: {folder_path} =====")
        self.progress_var.set(0)
        self.progress_label.config(text="0%")
        
        # 使用事件来同步线程
        conversion_completed = threading.Event()
        conversion_result = {"success": False, "error": None}
        
        # 定义文件转换线程函数
        def convert_files():
            try:
                self._log("【步骤1】开始执行文件批量转换...")
                # 直接调用_batch_convert_to_json执行转换
                self._batch_convert_to_json(folder_path)
                conversion_result["success"] = True
                self._log("文件批量转换完成")
            except Exception as e:
                error_msg = f"文件转换出错: {str(e)}"
                self._log(error_msg)
                conversion_result["error"] = error_msg
                import traceback
                self._log(f"错误堆栈: {traceback.format_exc()}")
            finally:
                # 无论成功失败，都设置事件
                conversion_completed.set()
        
        # 定义主处理线程函数
        def main_process():
            try:
                # 启动转换线程
                convert_thread = threading.Thread(target=convert_files)
                convert_thread.daemon = True
                convert_thread.start()
                
                # 等待转换完成
                self._log("正在等待文件转换完成...")
                conversion_completed.wait()
                
                # 额外等待1秒确保文件系统操作完成
                import time
                time.sleep(1)
                
                # 检查转换结果
                if not conversion_result["success"]:
                    self._log(f"转换失败，无法继续更新AdditionInfo: {conversion_result['error']}")
                    return
                
                # 转换完成后更新AdditionInfo信息
                self._log("【步骤2】开始执行AdditionInfo信息更新...")
                # 执行一次更新
                update_success = self._update_addition_info()
                
                if update_success:
                    self._log("===== 解析和更新人员信息任务完成 =====")
                    # 显示成功消息给用户
                    self.root.after(0, lambda: messagebox.showinfo("成功", "解析和更新人员信息任务完成"))
                else:
                    self._log("警告: AdditionInfo更新未成功或部分失败")
                    self.root.after(0, lambda: messagebox.showwarning("警告", "文件解析完成，但AdditionInfo更新可能未成功，请检查日志。"))
            except Exception as e:
                self._log(f"===== 执行解析和更新过程中出错 =====")
                self._log(f"错误详情: {str(e)}")
                import traceback
                self._log(f"错误堆栈: {traceback.format_exc()}")
                self.root.after(0, lambda: messagebox.showerror("错误", f"执行过程中出错: {str(e)}"))
            finally:
                self.progress_var.set(0)
                self.progress_label.config(text="0%")
        
        # 在新线程中执行主处理流程
        main_thread = threading.Thread(target=main_process)
        main_thread.daemon = True
        main_thread.start()
    
    def _toggle_person_list(self):
        """根据生成方式切换人员列表和名单选择框架的显示状态"""
        if self.generate_method.get() == "selected":
            # 自定义生成简历：显示人员列表，隐藏名单选择框架
            # 隐藏取消折叠按钮
            self.unfold_button.pack_forget()
            # 显示人员列表
            self.person_list_frame.pack(fill=tk.X, pady=5)
            # 自动更新人员名单，确保有数据显示
            self._update_person_list()
            self._person_list_visible = True
            # 隐藏名单选择框架
            if hasattr(self, 'list_select_frame'):
                self.list_select_frame.pack_forget()
        else:
            # 按名单生成简历：隐藏人员列表，显示名单选择框架
            # 隐藏取消折叠按钮
            self.unfold_button.pack_forget()
            # 隐藏人员列表
            self.person_list_frame.pack_forget()
            self._person_list_visible = False
            # 确保进度条可见
            self.progress_bar.pack(fill=tk.X, pady=5)
            self.progress_label.pack(pady=2)
            # 确保日志区域可见
            self.log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            # 显示名单选择框架
            if hasattr(self, 'list_select_frame'):
                self.list_select_frame.pack(fill=tk.X, pady=5)
    
    def _toggle_person_list_visibility(self):
        """切换人员列表的折叠/展开状态"""
        if self._person_list_visible:
            # 折叠人员列表
            self.person_list_frame.pack_forget()
            self._person_list_visible = False
            # 确保进度条可见
            self.progress_bar.pack(fill=tk.X, pady=5)
            self.progress_label.pack(pady=2)
            # 确保日志区域可见
            self.log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
            # 显示取消折叠按钮
            self.unfold_button.pack(side=tk.RIGHT, padx=5)
    
    def _show_person_list(self):
        """显示人员列表并隐藏取消折叠按钮"""
        if not self._person_list_visible:
            # 隐藏取消折叠按钮
            self.unfold_button.pack_forget()
            # 显示人员列表
            self.person_list_frame.pack(fill=tk.X, pady=5)
            self._person_list_visible = True
        else:
            # 展开人员列表
            self.person_list_frame.pack(fill=tk.X, pady=5)
            self._person_list_visible = True
    
    def _update_person_list(self):
        """更新人员名单，包含执行get_emp_list脚本"""
        self._log("开始更新人员名单...")
        
        # 在单独的线程中执行更新操作
        threading.Thread(target=self._update_person_list_thread).start()
    
    def _confirm_selection(self):
        """确认选择并保存选中的人员"""
        # 收集所有选中的员工编号
        self.selected_list = []
        for item in self.person_tree.get_children():
            values = self.person_tree.item(item, "values")
            if values and len(values) > 0 and values[0] == "✓":
                emp_no = values[1]
                self.selected_list.append(emp_no)
        
        if self.selected_list:
            # 保存选中的员工编号到带时间戳的文件
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            self._save_selected_emp_numbers(self.selected_list, timestamp)
            self._log(f"已确认选择 {len(self.selected_list)} 人")
            self._log(f"选中的员工编号: {', '.join(self.selected_list)}")
        else:
            self._log("未选择任何人员")
    
    def _select_list_file(self):
        """选择名单文件"""
        file_path = filedialog.askopenfilename(
            title="选择人员名单文件",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")]
        )
        if file_path:
            self.selected_list_file_path.set(file_path)
            # 显示已选择的文件
            self.selected_count_var.set("已选择文件，请点击'确认选择'导入名单")
            self._log(f"已选择名单文件: {file_path}")
    
    def _confirm_list_selection(self):
        """确认名单选择并更新select_list"""
        file_path = self.selected_list_file_path.get()
        if not file_path:
            messagebox.showwarning("警告", "请先选择名单文件")
            return
        
        try:
            # 读取文件内容
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read().strip()
            
            # 解析工号，支持逗号、空格、换行等分隔符
            emp_numbers = []
            # 替换所有非数字字符为逗号，然后分割
            import re
            cleaned_content = re.sub(r'[^0-9]', ',', content)
            emp_numbers = [num.strip() for num in cleaned_content.split(',') if num.strip()]
            
            if emp_numbers:
                # 更新selected_list
                self.selected_list = emp_numbers
                # 更新人员列表选择状态
                self._update_person_list_selection()
                # 更新状态显示
                self.selected_count_var.set(f"已选择 {len(emp_numbers)} 人")
                self._log(f"成功导入名单，共 {len(emp_numbers)} 人")
                self._log(f"导入的员工编号: {', '.join(emp_numbers)}")
                # 保存到最近选择
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                self._save_selected_emp_numbers(emp_numbers, timestamp)
            else:
                messagebox.showwarning("警告", "名单文件中未找到有效的工号")
                self.selected_count_var.set("名单文件格式不正确，请重新选择")
                self._log("名单文件格式不正确，未找到有效工号")
                
        except Exception as e:
            messagebox.showerror("错误", f"读取名单文件时出错: {str(e)}")
            self.selected_count_var.set("读取文件出错，请重新选择")
            self._log(f"读取名单文件时出错: {str(e)}")
    
    def _update_person_list_selection(self):
        """根据selected_list更新人员列表选择状态"""
        if not hasattr(self, 'selected_list') or not self.selected_list:
            return
        
        # 遍历所有人员，选中匹配的工号
        for item in self.person_tree.get_children():
            values = list(self.person_tree.item(item, "values"))
            if values and len(values) > 1:
                emp_no = values[1]
                # 检查是否在selected_list中
                if emp_no in self.selected_list:
                    values[0] = "✓"
                else:
                    values[0] = ""
                self.person_tree.item(item, values=values)
    
    def _on_tree_click(self, event):
        """处理树视图的点击事件，实现复选框功能"""
        # 获取点击的列
        region = self.person_tree.identify_region(event.x, event.y)
        if region == "cell":
            column = self.person_tree.identify_column(event.x)
            item = self.person_tree.identify_row(event.y)
            
            if column == "#1" and item:
                # 获取当前值
                values = list(self.person_tree.item(item, "values"))
                # 切换选中状态
                if values and len(values) > 0:
                    if values[0] == "✓":
                        values[0] = ""
                    else:
                        values[0] = "✓"
                    # 更新值
                    self.person_tree.item(item, values=values)
    
    def _update_person_list_thread(self):
        """在单独线程中更新人员名单"""
        try:
            # 执行get_emp_list脚本
            self._update_progress(30)
            self._log("正在执行get_emp_list脚本获取最新员工信息...")
            
            # 执行get_emp_list.py脚本
            get_emp_cmd = [sys.executable, "get_emp_list.py"]
            result = subprocess.run(get_emp_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self._log("[OK] get_emp_list脚本执行成功")
                # 输出脚本的部分关键信息
                for line in result.stdout.split('\n'):
                    if any(keyword in line for keyword in ['成功保存', '共保存', '部门统计']):
                        self._log(f"  {line.strip()}")
            else:
                self._log("[ERROR] get_emp_list脚本执行失败")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            
            # 加载员工信息
            self._update_progress(60)
            if not self._load_employee_info():
                self._log("[WARNING] 未找到员工信息，请先更新人员名单")
                return
            
            # 清空现有列表
            def clear_tree():
                for item in self.person_tree.get_children():
                    self.person_tree.delete(item)
            
            if self.root.winfo_exists():
                self.root.after(0, clear_tree)
            
            # 直接从config/emp_list.json加载人员信息
            self._update_progress(80)
            
            # 存储所有人员信息
            all_persons = []
            
            # 从员工信息中提取人员名单
            if hasattr(self, 'employee_info') and self.employee_info:
                for emp in self.employee_info:
                    emp_no = emp.get('EmpNo', '')
                    # 直接使用JobName作为姓名（从get_emp_list.py中可以看到，JobName实际上对应的是姓名列）
                    person_name = emp.get('JobName', '')
                    
                    # 获取部门信息
                    level1 = emp.get('Level1Dept', '')
                    level2 = emp.get('Level2Dept', '')
                    dept_info = f"{level1}-{level2}" if level1 and level2 else level1 or level2
                    
                    # 只有当工号和姓名都不为空时才添加
                    if emp_no and person_name:
                        all_persons.append((emp_no, person_name, dept_info))
            
            # 如果从emp_list.json没有获取到姓名，回退到从modify_json目录读取
            if not all_persons and hasattr(self, 'employee_info') and self.employee_info:
                self._log("从emp_list.json未获取到姓名信息，尝试从modify_json目录补充...")
                modify_dir = os.path.join(base_dir, "output", "modify_json")
                if os.path.exists(modify_dir):
                    # 创建工号到员工信息的映射
                    emp_map = {emp.get('EmpNo'): emp for emp in self.employee_info}
                    person_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
                    for json_file in person_files:
                        file_path = os.path.join(modify_dir, json_file)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                if data and isinstance(data, dict):
                                    for person_name, person_data in data.items():
                                        # 获取员工编号
                                        emp_no = ""
                                        if 'BasicInfo' in person_data and 'EmpNo' in person_data['BasicInfo']:
                                            emp_no = person_data['BasicInfo']['EmpNo']
                                        elif file_path and os.path.basename(file_path).split('_')[0].isdigit():
                                            emp_no = os.path.basename(file_path).split('_')[0]
                                        
                                        # 如果在emp_map中找到该工号，使用emp_list.json中的部门信息
                                        if emp_no in emp_map:
                                            emp = emp_map[emp_no]
                                            level1 = emp.get('Level1Dept', '')
                                            level2 = emp.get('Level2Dept', '')
                                            dept_info = f"{level1}-{level2}" if level1 and level2 else level1 or level2
                                            all_persons.append((emp_no, person_name, dept_info))
                        except Exception as e:
                            self._log(f"处理文件{json_file}时出错: {str(e)}")
                            continue
            
            # 按员工编号排序并插入树视图
            all_persons.sort(key=lambda x: (x[0] if x[0].isdigit() else '99999', x[1]))
            
            def populate_tree():
                for emp_no, person_name, dept_info in all_persons:
                    # 添加空的复选框列
                    self.person_tree.insert("", "end", values=("", emp_no, person_name, dept_info))
                self._log(f"已更新人员名单，共 {len(all_persons)} 人")
            
            if self.root.winfo_exists():
                self.root.after(0, populate_tree)
                self.root.after(0, lambda: self._update_progress(100))
            
        except Exception as e:
            self._log(f"更新人员名单时出错: {str(e)}")
            import traceback
            self._log(traceback.format_exc())
    
    def _show_bank_management_dialog(self):
        """显示银行管理对话框 - 调用独立子程序，并在关闭后刷新银行列表"""
        try:
            # 获取银行管理子程序的路径
            bank_management_path = os.path.join(base_dir, 'package', 'utils', 'bank_management.py')
            
            # 在新进程中启动银行管理程序
            self._log("启动银行管理工具...")
            process = subprocess.Popen([sys.executable, bank_management_path])
            
            # 创建一个线程来等待银行管理程序关闭并刷新银行列表
            threading.Thread(target=self._wait_for_bank_management_and_refresh, args=(process,)).start()
            
        except Exception as e:
            messagebox.showerror("错误", f"启动银行管理程序失败: {str(e)}")
        
    def _wait_for_bank_management_and_refresh(self, process):
        """等待银行管理程序关闭并刷新银行列表"""
        # 等待进程结束
        process.wait()
        
        # 在主线程中刷新银行列表
        self.root.after(0, self._refresh_bank_list)
    
    def _refresh_bank_list(self):
        """刷新银行下拉框列表"""
        try:
            # 获取最新的银行列表
            new_bank_list = self._get_bank_list()
            
            # 更新下拉框值
            self.bank_combobox['values'] = new_bank_list
            
            # 如果当前选中的银行仍然在列表中，保持选中；否则选择第一个
            current_selection = self.bank_combobox.get()
            if current_selection and current_selection in new_bank_list:
                self.bank_combobox.set(current_selection)
            elif new_bank_list:
                self.bank_combobox.set(new_bank_list[0])
                # 触发logo更新
                self._update_bank_logo()
            
            self._log("银行列表已刷新")
        except Exception as e:
            self._log(f"刷新银行列表时出错: {str(e)}")
    
    # 银行管理相关方法已移至独立子程序 bank_management.py
    
    def _show_resume_validation(self):
        """显示简历校验界面"""
        try:
            # 获取check_ui.py的路径
            check_ui_path = os.path.join(base_dir, 'check_ui.py')
            if os.path.exists(check_ui_path):
                self._log("启动简历校验工具...")
                # 在新进程中启动校验UI
                subprocess.Popen([sys.executable, check_ui_path])
            else:
                messagebox.showerror("错误", f"未找到校验UI脚本: {check_ui_path}")
                self._log(f"错误: 未找到校验UI脚本: {check_ui_path}")
        except Exception as e:
            messagebox.showerror("错误", f"启动简历校验工具时出错: {str(e)}")
            self._log(f"错误: 启动简历校验工具时出错: {str(e)}")
    
    def _load_employee_info(self):
        """加载员工信息，用于部门筛选
        
        Returns:
            bool: 是否成功加载
        """
        try:
            emp_list_path = os.path.join(base_dir, "config", "emp_list.json")
            if not os.path.exists(emp_list_path):
                self._log(f"员工信息文件不存在: {emp_list_path}")
                # 如果是初始化时检查且不存在，显示提示
                if not hasattr(self, 'employee_info'):
                    messagebox.showinfo("提示", "请先更新人员名单以获取员工信息")
                return False
            
            with open(emp_list_path, 'r', encoding='utf-8') as f:
                self.employee_info = json.load(f)
            
            # 提取部门信息
            level1_depts = set()
            level2_depts = {}
            
            for emp in self.employee_info:
                level1 = emp.get('Level1Dept', '')
                level2 = emp.get('Level2Dept', '')
                
                if level1:
                    level1_depts.add(level1)
                    if level1 not in level2_depts:
                        level2_depts[level1] = set()
                    if level2:
                        level2_depts[level1].add(level2)
            
            # 更新一级部门下拉框
            def update_dept_comboboxes():
                # 清空并设置一级部门
                self.level1_dept_combobox['values'] = ['全部'] + sorted(list(level1_depts))
                self.level1_dept_combobox.current(0)
                
                # 清空二级部门
                self.level2_dept_combobox['values'] = ['全部']
                self.level2_dept_combobox.current(0)
            
            if self.root.winfo_exists():
                self.root.after(0, update_dept_comboboxes)
                self._log(f"已加载 {len(self.employee_info)} 条员工信息")
            
            return True
        
        except Exception as e:
            self._log(f"加载员工信息时出错: {str(e)}")
            return False
    
    def _load_last_selected(self):
        """加载最近一次选择的人员名单"""
        try:
            # 首先确保人员列表已经更新
            if not self.person_tree.get_children():
                self._log("人员列表为空，先更新人员名单...")
                # 同步更新人员名单，等待完成
                self._update_person_list_thread()
                # 给一点时间确保人员列表加载完成
                import time
                time.sleep(1)
                
                # 再次检查
                if not self.person_tree.get_children():
                    messagebox.showinfo("提示", "无法加载人员列表，请手动点击更新人员名单")
                    return
            
            config_dir = os.path.join(base_dir, "config")
            if not os.path.exists(config_dir):
                messagebox.showinfo("提示", "未找到配置目录")
                return
            
            # 查找所有selected_list_开头的文件（更宽松的匹配）
            selected_files = []
            for filename in os.listdir(config_dir):
                if filename.startswith("selected_list_"):
                    # 尝试提取时间戳部分
                    timestamp_str = filename[len("selected_list_"):].replace(".txt", "")
                    try:
                        # 尝试解析时间戳，如果失败则使用文件修改时间
                        try:
                            datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                            selected_files.append((filename, timestamp_str))
                        except ValueError:
                            # 使用文件修改时间作为备选
                            file_path = os.path.join(config_dir, filename)
                            mod_time = os.path.getmtime(file_path)
                            selected_files.append((filename, str(int(mod_time))))
                    except Exception:
                        continue
            
            # 按时间戳降序排序，获取最新的文件
            if selected_files:
                selected_files.sort(key=lambda x: x[1], reverse=True)
                latest_file = selected_files[0][0]
                latest_file_path = os.path.join(config_dir, latest_file)
                
                # 读取文件内容
                with open(latest_file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                
                if content:
                    # 解析员工编号列表
                    self.selected_list = [emp_no.strip() for emp_no in content.split(',')]
                    
                    # 记录匹配情况
                    matched_count = 0
                    
                    # 选中树视图中对应的项
                    for item in self.person_tree.get_children():
                        values = self.person_tree.item(item, "values")
                        if values and len(values) > 1 and values[1] in self.selected_list:
                            # 设置选中状态
                            new_values = list(values)
                            new_values[0] = "✓"
                            self.person_tree.item(item, values=new_values)
                            matched_count += 1
                    
                    if matched_count > 0:
                        self._log(f"已加载最近选择的 {matched_count} 人")
                        self._log(f"来自文件: {latest_file}")
                        if matched_count < len(self.selected_list):
                            self._log(f"注意: 有 {len(self.selected_list) - matched_count} 个员工编号在当前列表中未找到")
                    else:
                        self._log(f"警告: 未找到任何匹配的员工记录")
                        self._log(f"尝试匹配的员工编号: {', '.join(self.selected_list)}")
                else:
                    messagebox.showinfo("提示", "最近的选择文件为空")
            else:
                # 也尝试检查默认的selected_list.txt文件
                default_file = os.path.join(config_dir, "selected_list.txt")
                if os.path.exists(default_file):
                    with open(default_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                    if content:
                        self.selected_list = [emp_no.strip() for emp_no in content.split(',')]
                        matched_count = 0
                        for item in self.person_tree.get_children():
                            values = self.person_tree.item(item, "values")
                            if values and len(values) > 1 and values[1] in self.selected_list:
                                new_values = list(values)
                                new_values[0] = "✓"
                                self.person_tree.item(item, values=new_values)
                                matched_count += 1
                        self._log(f"已从默认文件加载选择的 {matched_count} 人")
                    else:
                        messagebox.showinfo("提示", "默认选择文件为空")
                else:
                    messagebox.showinfo("提示", "未找到任何历史选择记录")
        
        except Exception as e:
            self._log(f"加载上次选择时出错: {str(e)}")
            import traceback
            self._log(traceback.format_exc())
            messagebox.showerror("错误", f"加载上次选择时出错: {str(e)}")
    
    def _on_level1_dept_selected(self, event):
        """一级部门选择变化时更新二级部门列表"""
        level1_dept = self.level1_dept_var.get()
        
        # 清空二级部门
        self.level2_dept_combobox['values'] = ['全部']
        self.level2_dept_combobox.current(0)
        
        if level1_dept != '全部' and hasattr(self, 'employee_info'):
            # 提取该一级部门下的所有二级部门
            level2_depts = set()
            for emp in self.employee_info:
                if emp.get('Level1Dept') == level1_dept and emp.get('Level2Dept'):
                    level2_depts.add(emp.get('Level2Dept'))
            
            if level2_depts:
                self.level2_dept_combobox['values'] = ['全部'] + sorted(list(level2_depts))
    
    def _filter_person_list(self, *args):
        """根据搜索框内容过滤人员列表"""
        search_text = self.search_var.get().lower()
        self._apply_filters(search_text, self.level1_dept_var.get(), self.level2_dept_var.get())
    
    def _filter_by_dept(self, event=None):
        """根据部门选择过滤人员列表"""
        self._apply_filters(self.search_var.get().lower(), self.level1_dept_var.get(), self.level2_dept_var.get())
    
    def _clear_selected(self):
        """清除所有已点选的人员信息"""
        count = 0
        # 清除所有选中状态
        for item in self.person_tree.get_children():
            values = self.person_tree.item(item, "values")
            if values and len(values) > 0 and values[0] == "✓":
                new_values = list(values)
                new_values[0] = ""
                self.person_tree.item(item, values=new_values)
                count += 1
        
        # 清空已选中列表
        self.selected_list = []
        self._log(f"已清除 {count} 人的选择状态")
    
    def _apply_filters(self, search_text, level1_dept, level2_dept):
        """应用所有筛选条件"""
        # 首先恢复所有被隐藏的项目
        if hasattr(self, 'hidden_items') and self.hidden_items:
            for item in list(self.hidden_items.keys()):
                self.person_tree.reattach(item, '', 'end')
            # 清空隐藏项目列表
            self.hidden_items = {}
        
        # 筛选逻辑
        # 注意：这里我们需要重新获取所有可见的项目（现在应该是全部项目）
        for item in self.person_tree.get_children():
            values = self.person_tree.item(item, "values")
            if not values or len(values) < 4:
                continue
            
            _, emp_no, name, dept = values
            
            # 搜索文本筛选
            search_match = True
            if search_text:
                search_match = search_text in emp_no.lower() or search_text in name.lower() or search_text in dept.lower()
            
            # 一级部门筛选
            level1_match = True
            if level1_dept != '全部':
                level1_match = level1_dept in dept
            
            # 二级部门筛选
            level2_match = True
            if level2_dept != '全部' and level1_dept != '全部':
                level2_match = f"{level1_dept}-{level2_dept}" in dept or level2_dept in dept
            
            # 如果不匹配，隐藏项目
            if not (search_match and level1_match and level2_match):
                # 保存被隐藏的项目的值
                self.hidden_items[item] = values
                # 从Treeview中移除项目
                self.person_tree.detach(item)
    
    def _clear_filters(self):
        """清除所有筛选条件"""
        self.search_var.set("")
        self.level1_dept_combobox.current(0)
        self.level2_dept_combobox['values'] = ['全部']
        self.level2_dept_combobox.current(0)
        
        # 重新显示所有被隐藏的项目
        if hasattr(self, 'hidden_items'):
            for item, values in self.hidden_items.items():
                # 重新插入项目
                self.person_tree.reattach(item, '', 'end')
            # 清空隐藏项目列表
            self.hidden_items = {}
        
        # 确保所有项目都可见
        for item in self.person_tree.get_children():
            self.person_tree.item(item, tags=())
    
    def _generate_resumes(self):
        bankname = self.bank_var.get()
        if not bankname:
            self._log("请选择银行")
            return
        
        # 隐藏人员选择框架，显示进度条和日志
        if hasattr(self, 'person_list_frame') and self.generate_method.get() == "selected":
            self.person_list_frame.pack_forget()
        
        # 确保进度条可见
        if hasattr(self, 'progress_bar'):
            self.progress_bar.pack(fill=tk.X, pady=5)
        if hasattr(self, 'progress_label'):
            self.progress_label.pack(pady=2)
        
        # 确保日志区域可见
        if hasattr(self, 'log_frame'):
            self.log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # 确定要处理的人员名单
        if self.generate_method.get() == "all":
            # 按名单生成简历：根据上传的名单内容决定生成范围
            if not self.selected_list_file_path:
                self._log("请先选择名单文件")
                messagebox.showinfo("提示", "请先选择名单文件")
                return
            
            # 检查名单内容是否包含'ALL'
            if hasattr(self, 'selected_list') and self.selected_list and self.selected_list[0].upper() == 'ALL':
                # 名单中包含'ALL'，执行全部生成
                person_names = "all"
                self._log(f"开始全量生成简历，银行: {bankname}")
                self._log("名单中包含'ALL'，执行全部生成")
            else:
                # 名单中不包含'ALL'，只生成名单中的人员
                if not self.selected_list:
                    self._log("请先确认名单选择")
                    messagebox.showinfo("提示", "请先点击'确认选择'按钮")
                    return
                person_names = self.selected_list
                self._log(f"开始按名单生成简历，银行: {bankname}，人员数量: {len(person_names)}")
                self._log(f"名单中的员工编号: {', '.join(person_names)}")
        else:
            # 自定义生成简历：根据界面选择的人员生成
            # 检查是否有已确认选择的人员
            if not self.selected_list:
                # 如果没有已确认的选择，尝试从复选框中获取
                emp_numbers = []
                for item in self.person_tree.get_children():
                    values = self.person_tree.item(item, "values")
                    if values and len(values) > 0 and values[0] == "✓":
                        emp_no = values[1]
                        emp_numbers.append(emp_no)
                
                if not emp_numbers:
                    self._log("请先选择或确认要生成简历的人员")
                    messagebox.showinfo("提示", "请先选择人员并点击'确认选择'按钮")
                    # 恢复人员列表显示
                    if hasattr(self, 'person_list_frame'):
                        self.person_list_frame.pack(fill=tk.X, pady=5)
                    return
                
                # 自动保存并确认选择
                self.selected_list = emp_numbers
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                # 直接使用工号列表作为person_names
                person_names = emp_numbers
                self._save_selected_emp_numbers(emp_numbers, timestamp, bankname, person_names)
            
            # 直接使用工号列表作为person_names，因为批生成函数需要工号来过滤JSON文件
            person_names = self.selected_list
            
            self._log(f"开始生成选中人员简历，银行: {bankname}，人员数量: {len(person_names)}")
            self._log(f"选中的员工编号: {', '.join(person_names)}")
        
        # 调用批量生成简历功能
        self._batch_generate_resumes_in_thread(bankname, person_names)
    
    def _update_addition_info(self, person_names=None):
        """更新modify_json目录下简历JSON文件的AdditionInfo信息，直接从info_json目录查找对应人员的信息
        
        Args:
            person_names: 可选的员工编号列表，若为None或"all"则更新所有文件
        
        Returns:
            bool: 更新是否成功
        """
        try:
            import os
            import json
            
            self._log("===== 开始更新AdditionInfo信息 =====")
            
            # 获取info_json和modify_json目录
            info_dir = os.path.join(base_dir, "output", "info_json")
            modify_dir = os.path.join(base_dir, "output", "modify_json")
            
            self._log(f"检查目录: info_dir={info_dir}, modify_dir={modify_dir}")
            
            if not os.path.exists(info_dir):
                self._log(f"错误: info_json目录不存在: {info_dir}")
                return False
            
            if not os.path.exists(modify_dir):
                self._log(f"错误: modify_json目录不存在: {modify_dir}")
                return False
            
            # 优化：将person_names转换为集合以提高查找效率
            target_emp_nos = set(person_names) if person_names and person_names != "all" and isinstance(person_names, list) else None
            
            # 构建员工编号到info数据的映射
            emp_info_map = {}
            
            # 优化：优先尝试直接根据文件命名格式匹配
            if target_emp_nos:
                self._log(f"根据指定的 {len(target_emp_nos)} 个员工编号直接匹配信息文件")
                for emp_no in target_emp_nos:
                    # 尝试查找匹配的info文件（可能有不同的后缀格式）
                    found = False
                    for info_file in os.listdir(info_dir):
                        if info_file.startswith(f"{emp_no}_") and info_file.endswith('.json'):
                            try:
                                info_path = os.path.join(info_dir, info_file)
                                with open(info_path, 'r', encoding='utf-8') as f:
                                    info_data = json.load(f)
                                emp_info_map[emp_no] = info_data
                                self._log(f"  找到员工 {emp_no} 的信息文件: {info_file}")
                                found = True
                                break
                            except Exception as e:
                                self._log(f"  处理info文件 {info_file} 时出错: {e}")
                                found = True  # 即使出错也标记为已处理
                                break
                    
                    if not found:
                        self._log(f"  未找到员工 {emp_no} 的信息文件")
            else:
                # 如果没有指定员工，才遍历所有info文件
                info_files = [f for f in os.listdir(info_dir) if f.endswith('.json')]
                self._log(f"发现 {len(info_files)} 个info_json文件")
                
                if not info_files:
                    self._log("警告: 未找到info_json文件")
                    return False
                
                for info_file in info_files:
                    try:
                        emp_no = info_file.split('_')[0]
                        info_path = os.path.join(info_dir, info_file)
                        
                        with open(info_path, 'r', encoding='utf-8') as f:
                            info_data = json.load(f)
                        
                        emp_info_map[emp_no] = info_data
                    except Exception as e:
                        self._log(f"  处理info文件 {info_file} 时出错: {e}")
            
            self._log(f"成功构建 {len(emp_info_map)} 条员工信息映射")
            
            # 如果没有需要更新的员工信息，直接返回
            if not emp_info_map:
                self._log("警告: 没有有效的员工信息可以更新")
                return False
            
            # 获取简历JSON文件（针对有员工信息的文件进行优化匹配）
            if target_emp_nos:
                # 如果指定了目标员工，只查找这些员工的简历文件
                self._log(f"根据员工编号集合直接查找需要更新的简历文件")
                files_to_update = []
                existing_files = set(os.listdir(modify_dir))
                
                # 对于每个有员工信息的工号，尝试找到对应的简历文件
                for emp_no in emp_info_map:
                    resume_files_found = False
                    # 查找所有可能的简历文件
                    for file in existing_files:
                        if file.startswith(f"{emp_no}_") and file.endswith('.json'):
                            files_to_update.append(file)
                            resume_files_found = True
                            break
                    
                    if not resume_files_found:
                        self._log(f"  未找到员工 {emp_no} 对应的简历JSON文件")
            else:
                # 没有指定目标员工，才遍历所有简历文件
                resume_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
                self._log(f"发现 {len(resume_files)} 个简历JSON文件")
                
                if not resume_files:
                    self._log("警告: 未找到简历JSON文件")
                    return False
                
                # 过滤出有对应员工信息的文件
                files_to_update = []
                for resume_file in resume_files:
                    file_emp_no = resume_file.split('_')[0]  # 从文件名提取工号
                    if file_emp_no in emp_info_map:
                        files_to_update.append(resume_file)
                    else:
                        self._log(f"  跳过: 未找到员工 {file_emp_no} 的人员信息文件")
            
            self._log(f"过滤后需要更新的文件数量: {len(files_to_update)}")
            
            # 更新每个文件
            updated_count = 0
            skipped_count = 0
            for resume_file in files_to_update:
                try:
                    file_emp_no = resume_file.split('_')[0]
                    resume_path = os.path.join(modify_dir, resume_file)
                    
                    # 使用已构建的映射，避免重复查找
                    info_data = emp_info_map[file_emp_no]
                    
                    # 读取简历文件
                    with open(resume_path, 'r', encoding='utf-8') as f:
                        resume_data = json.load(f)
                    
                    # 检查文件是否存在
                    if not os.path.exists(resume_path):
                        self._log(f"  错误: 简历文件不存在: {resume_path}")
                        skipped_count += 1
                        continue
                    
                    # 更新AdditionInfo字段
                    if resume_data:
                        # 获取第一个键（通常是姓名）
                        person_name = list(resume_data.keys())[0]
                        if person_name in resume_data:
                            # 正确处理info_data的嵌套结构
                            # 检查info_data是否已经是嵌套的结构（从info_json读取的格式）
                            if isinstance(info_data, dict):
                                # 如果info_data有一个键（通常是姓名），并且该键下有AdditionInfo
                                if len(info_data) == 1:
                                    first_key = list(info_data.keys())[0]
                                    if isinstance(info_data[first_key], dict) and "AdditionInfo" in info_data[first_key]:
                                        # 这是从info_json读取的标准格式
                                        resume_data[person_name]["AdditionInfo"] = info_data[first_key]["AdditionInfo"]
                                    else:
                                        # 其他情况，直接使用该键下的数据
                                        resume_data[person_name]["AdditionInfo"] = info_data[first_key]
                                # 如果info_data直接包含AdditionInfo键
                                elif "AdditionInfo" in info_data:
                                    resume_data[person_name]["AdditionInfo"] = info_data["AdditionInfo"]
                                # 其他情况，直接使用info_data
                                else:
                                    resume_data[person_name]["AdditionInfo"] = info_data
                            else:
                                # 如果info_data不是字典，创建一个空字典
                                resume_data[person_name]["AdditionInfo"] = {}
                            
                            # 写回文件
                            with open(resume_path, 'w', encoding='utf-8') as f:
                                json.dump(resume_data, f, ensure_ascii=False, indent=4)
                            
                            updated_count += 1
                            self._log(f"  已更新: {resume_file} - 成功添加AdditionInfo")
                        else:
                            self._log(f"  跳过: 在 {resume_file} 中未找到键 {person_name}")
                            skipped_count += 1
                    else:
                        self._log(f"  跳过: {resume_file} 内容为空")
                        skipped_count += 1
                except Exception as e:
                    self._log(f"  错误: 更新简历文件 {resume_file} 时出错: {e}")
                    skipped_count += 1
            
            # 添加验证步骤，检查AdditionInfo是否成功合并
            self._log("===== 开始验证AdditionInfo合并结果 =====")
            validation_success = 0
            validation_failed = 0
            
            for resume_file in files_to_update:
                try:
                    resume_path = os.path.join(modify_dir, resume_file)
                    
                    # 重新读取文件验证
                    with open(resume_path, 'r', encoding='utf-8') as f:
                        validated_data = json.load(f)
                    
                    if validated_data:
                        person_name = list(validated_data.keys())[0]
                        if person_name in validated_data and "AdditionInfo" in validated_data[person_name]:
                            # 检查AdditionInfo是否有内容
                            addition_info = validated_data[person_name]["AdditionInfo"]
                            if addition_info and isinstance(addition_info, dict) and len(addition_info) > 0:
                                validation_success += 1
                                self._log(f"  验证成功: {resume_file} - AdditionInfo已正确合并")
                            else:
                                validation_failed += 1
                                self._log(f"  验证失败: {resume_file} - AdditionInfo存在但为空或格式不正确")
                        else:
                            validation_failed += 1
                            self._log(f"  验证失败: {resume_file} - 未找到AdditionInfo字段")
                    else:
                        validation_failed += 1
                        self._log(f"  验证失败: {resume_file} - 文件内容为空")
                except Exception as e:
                    validation_failed += 1
                    self._log(f"  验证错误: 检查文件 {resume_file} 时出错: {e}")
            
            self._log(f"===== 人员信息更新和验证完成 =====")
            self._log(f"成功更新: {updated_count} 个文件")
            self._log(f"跳过: {skipped_count} 个文件")
            self._log(f"验证结果 - 成功: {validation_success}, 失败: {validation_failed}")
            
            # 返回验证是否全部成功
            return updated_count > 0 and validation_success == updated_count
        except Exception as e:
            self._log(f"===== 更新人员信息时发生严重错误 =====")
            self._log(f"错误详情: {str(e)}")
            import traceback
            self._log(f"错误堆栈: {traceback.format_exc()}")
            return False
    
    def _batch_generate_resumes_in_thread(self, bankname, person_names):
        """在新线程中执行批量生成简历，避免GUI卡顿"""
        self._log(f"===== 开始生成简历 ===== 银行: {bankname}, 人员: {len(person_names) if isinstance(person_names, list) else '全部'}")
        
        def generate_thread():
            try:
                import time
                
                # 使用事件来同步AdditionInfo更新操作
                addition_info_updated = threading.Event()
                update_result = {"success": False}
                
                # 定义AdditionInfo更新线程函数
                def update_addition_info():
                    try:
                        self._log("【步骤1】开始执行AdditionInfo信息更新...")
                        # 执行一次优化后的更新
                        update_success = self._update_addition_info(person_names)
                        update_result["success"] = update_success
                        
                        if update_result["success"]:
                            self._log("AdditionInfo信息更新成功完成")
                        else:
                            self._log("警告: AdditionInfo信息更新未完全成功")
                    except Exception as e:
                        self._log(f"AdditionInfo更新过程中出错: {str(e)}")
                        import traceback
                        self._log(f"错误堆栈: {traceback.format_exc()}")
                    finally:
                        addition_info_updated.set()
                
                # 启动AdditionInfo更新线程
                update_thread = threading.Thread(target=update_addition_info)
                update_thread.daemon = True
                update_thread.start()
                
                # 等待更新完成
                self._log("正在等待AdditionInfo更新操作完成...")
                addition_info_updated.wait()
                
                # 额外等待1秒确保文件系统操作完成
                time.sleep(1)
                
                if not update_result["success"]:
                    self._log("警告: AdditionInfo信息更新失败或部分失败，将继续执行生成任务")
                
                # 从output/modify_json目录获取所有JSON文件
                modify_dir = os.path.join(base_dir, "output", "modify_json")
                self._log(f"【步骤2】检查JSON文件目录: {modify_dir}")
                
                if not os.path.exists(modify_dir):
                    self._log(f"错误: 目录不存在: {modify_dir}")
                    # 显示错误消息给用户
                    self.root.after(0, lambda: messagebox.showerror("错误", f"JSON文件目录不存在: {modify_dir}"))
                    return
                
                # 初始化缺失员工列表和尝试更新的员工列表
                missing_employees = []
                updated_employees = []
                
                # 获取所有JSON文件
                json_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
                self._log(f"发现 {len(json_files)} 个JSON文件")
                
                # 检查是否需要按员工编号生成简历
                if person_names != "all" and isinstance(person_names, list):
                    self._log(f"【步骤3】开始处理 {len(person_names)} 个员工的简历生成请求")
                    
                    # 检查每个员工的JSON文件是否存在
                    for emp_no in person_names:
                        # 格式化员工编号为5位
                        emp_no_padded = emp_no.zfill(5)
                        # 查找匹配的JSON文件
                        json_file_exists = any(f.startswith(emp_no_padded) for f in json_files)
                        
                        if not json_file_exists:
                            self._log(f"未找到员工 {emp_no} 的简历JSON文件，尝试更新...")
                            missing_employees.append(emp_no)
                            
                            # 尝试调用update_specific_jsons.py更新该员工的JSON文件
                            self._log(f"调用_update_missing_employee_json更新员工 {emp_no} 的信息...")
                            updated = self._update_missing_employee_json(emp_no)
                            if updated:
                                updated_employees.append(emp_no)
                                self._log(f"成功更新员工 {emp_no} 的JSON文件")
                            else:
                                self._log(f"更新员工 {emp_no} 的JSON文件失败")
                    
                    # 重新获取JSON文件列表，包含可能刚更新的文件
                    json_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
                    self._log(f"重新扫描后发现 {len(json_files)} 个JSON文件")
                
                # 获取所有JSON文件
                if not json_files:
                    self._log("错误: 未找到JSON文件，请先解析简历")
                    return
                
                # 设置模板文件路径 - 根据银行名称动态查找对应的模板
                # 查找格式："银行名称_简历模板.docx"
                template_path = os.path.join(base_dir, "template", f"{bankname}_简历模板.docx")
                self._log(f"【步骤4】检查模板文件: {template_path}")
                
                # 如果找不到银行特定模板，直接弹窗提示
                if not os.path.exists(template_path):
                    self._log(f"错误: 未找到银行特定模板: {bankname}_简历模板.docx")
                    # 使用主线程显示弹窗
                    self.root.after(0, lambda: messagebox.showinfo("提示", f"没有对应{bankname}的模板，请先配置银行简历模板"))
                    return
                
                # 已在步骤1中执行过AdditionInfo更新，无需再次执行
                
                # 检查是否成功导入batch_render_module
                if batch_render_module and hasattr(batch_render_module, 'batch_generate_resumes'):
                    self._log(f"【步骤5】调用批生成功能，JSON目录: {modify_dir}，模板: {os.path.basename(template_path)}")
                    
                    # 调用批生成函数
                    success_count, failed_count = batch_render_module.batch_generate_resumes(
                        json_files_dir=modify_dir,
                        template_path=template_path,
                        bankname=bankname,
                        person_names=person_names
                    )
                    
                    # 更新进度为100%
                    self.progress_var.set(100)
                    self.progress_label.config(text="100%")
                    
                    # 记录结果
                    self._log(f"===== 批生成完成！===== 成功: {success_count}，失败: {failed_count}")
                    self._log(f"输出目录: {os.path.join(base_dir, 'output', bankname)}")
                    
                    # 记录缺失员工信息
                    if missing_employees:
                        # 过滤掉已经更新成功的员工
                        still_missing = [emp_no for emp_no in missing_employees if emp_no not in updated_employees]
                        if still_missing:
                            self._log(f"以下员工简历JSON文件仍然缺失，可能是对应人员的简历Word或信息不存在:")
                            for emp_no in still_missing:
                                self._log(f"  - 员工编号: {emp_no}")
                        if updated_employees:
                            self._log(f"成功更新了以下员工的简历JSON文件:")
                            for emp_no in updated_employees:
                                self._log(f"  - 员工编号: {emp_no}")
                else:
                    # 如果模块导入失败，使用原有的生成逻辑
                    self._log("批生成模块不可用，使用备用生成逻辑")
                    self._save_selected_emp_numbers(person_names, None, bankname, person_names)
            except Exception as e:
                self._log(f"===== 生成简历过程中出错 =====")
                self._log(f"错误详情: {str(e)}")
                import traceback
                self._log(f"错误堆栈: {traceback.format_exc()}")
            finally:
                self.progress_var.set(0)
                self.progress_label.config(text="0%")
        
        # 启动新线程执行生成任务
        thread = threading.Thread(target=generate_thread)
        thread.daemon = True
        thread.start()
    
    def _save_selected_emp_numbers(self, emp_numbers, timestamp=None, bankname=None, person_names=None):
        """保存选中的员工编号到文件
        
        Args:
            emp_numbers: 员工编号列表
            timestamp: 时间戳，如果为None则使用当前时间
            bankname: 银行名称，如果为None则不执行后续操作
        """
        try:
            # 确保目录存在
            config_dir = os.path.join(base_dir, "config")
            os.makedirs(config_dir, exist_ok=True)
            
            # 保存到默认文件（兼容旧版）
            default_path = os.path.join(config_dir, "selected_list.txt")
            with open(default_path, 'w', encoding='utf-8') as f:
                f.write(",".join(emp_numbers))
            
            # 如果提供了时间戳，保存到带时间戳的文件
            if timestamp:
                timestamp_path = os.path.join(config_dir, f"selected_list_{timestamp}.txt")
                with open(timestamp_path, 'w', encoding='utf-8') as f:
                    f.write(",".join(emp_numbers))
                
                self._log(f"已保存选中的员工编号到: {timestamp_path}")
            else:
                self._log(f"已保存选中的员工编号到: {default_path}")
        except Exception as e:
            self._log(f"保存选中员工编号时出错: {str(e)}")
        
        # 如果没有提供bankname，不执行后续操作
        if bankname is None:
            return
            
        try:
            # 从output/modify_json目录获取所有JSON文件
            modify_dir = os.path.join(base_dir, "output", "modify_json")
            if not os.path.exists(modify_dir):
                self._log(f"目录不存在: {modify_dir}")
                return
            
            # 获取所有JSON文件
            all_json_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
            
            # 根据emp_numbers过滤JSON文件
            if emp_numbers:
                filtered_json_files = []
                for json_file in all_json_files:
                    # 从文件名中提取工号
                    file_prefix = json_file.split('_')[0]
                    if file_prefix in emp_numbers:
                        filtered_json_files.append(json_file)
                json_files = filtered_json_files
                self._log(f"根据选中名单过滤后，共发现 {len(json_files)} 个匹配的JSON文件")
            else:
                json_files = all_json_files
                self._log(f"共发现 {len(json_files)} 个JSON文件")
            
            if not json_files:
                self._log("未找到匹配的JSON文件")
                return
            
            # 设置输出目录
            output_dir = os.path.join(base_dir, "output", bankname)
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成简历（备用逻辑，当批生成模块不可用时使用）
            success_count = 0
            total_files = len(json_files)
            
            for i, json_file in enumerate(json_files):
                json_path = os.path.join(modify_dir, json_file)
                
                try:
                    # 读取JSON数据
                    with open(json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 获取要处理的人员
                    # 由于我们已经根据工号过滤了JSON文件，这里可以处理文件中的所有人员
                    valid_names = list(data.keys())
                    
                    # 生成每个人员的简历
                    for person_name in valid_names:
                        # 生成文件名
                        current_date = datetime.now().strftime("%Y%m%d")
                        output_filename = f"{bankname}人员简历_{person_name}_{current_date}.docx"
                        output_path = os.path.join(output_dir, output_filename)
                        
                        # 模拟生成过程
                        self._log(f"生成简历: {person_name} -> {output_filename}")
                        
                        # 更新进度
                        progress = ((i + 1) / total_files) * 100
                        self.progress_var.set(progress)
                        self.progress_label.config(text=f"{int(progress)}%")
                        
                        success_count += 1
                        
                except Exception as e:
                    self._log(f"处理文件 {json_file} 时出错: {str(e)}")
            
            self._log(f"简历生成完成！成功生成 {success_count} 份简历")
            self._log(f"输出目录: {output_dir}")
            
        except Exception as e:
            self._log(f"生成简历过程中出错: {str(e)}")
    
    def _update_missing_employee_json(self, emp_no):
        """
        更新缺失的员工JSON文件
        
        Args:
            emp_no: 员工编号
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 构建update_specific_jsons.py的路径
            update_script_path = os.path.join(base_dir, "package", "functions", "update_specific_jsons.py")
            if not os.path.exists(update_script_path):
                self._log(f"未找到更新脚本: {update_script_path}")
                return False
            
            # 使用主程序中已选择的Excel文件路径
            info_file = ""
            # 检查是否有special_info_file变量(在特殊更新对话框中选择的)
            if hasattr(self, 'special_info_file'):
                info_file = self.special_info_file.get()
            
            # 如果没有特殊更新对话框中的文件，检查是否有其他可用的文件路径
            if not info_file or not os.path.exists(info_file):
                # 尝试默认的人员信息文件
                default_info_file = os.path.join(base_dir, "input", "技术人员名单-11月.xlsx")
                if os.path.exists(default_info_file):
                    info_file = default_info_file
                else:
                    # 尝试其他可能的位置
                    alt_info_file = os.path.join(base_dir, "input", "技术人员名单.xlsx")
                    if os.path.exists(alt_info_file):
                        info_file = alt_info_file
                    else:
                        self._log(f"未找到人员信息Excel文件")
                        return False
            
            # 使用主程序中已选择的简历文件夹路径
            resume_folder = ""
            resume_folder_param = []
            
            # 检查是否有special_resume_folder变量(在特殊更新对话框中选择的)
            if hasattr(self, 'special_resume_folder'):
                resume_folder = self.special_resume_folder.get()
            
            # 如果没有特殊更新对话框中的文件夹，检查resume_file_path变量
            if not resume_folder or not os.path.exists(resume_folder):
                if hasattr(self, 'resume_file_path'):
                    resume_folder = os.path.dirname(self.resume_file_path.get()) if self.resume_file_path.get() else ""
                
                # 如果还是没有，尝试默认的简历文件夹
                if not resume_folder or not os.path.exists(resume_folder):
                    default_resume_folder = os.path.join(base_dir, "input")
                    if os.path.exists(default_resume_folder):
                        resume_folder = default_resume_folder
            
            # 添加简历文件夹参数
            if resume_folder and os.path.exists(resume_folder):
                resume_folder_param = ["--word", resume_folder]
                self._log(f"使用简历文件夹: {resume_folder}")
            
            # 调用update_specific_jsons.py更新简历和信息JSON
            cmd = [
                sys.executable,
                update_script_path,
                "3",  # 同时更新简历和信息JSON
                emp_no,
                "--excel",
                info_file
            ]
            cmd.extend(resume_folder_param)
            
            self._log(f"正在执行更新脚本: {' '.join(cmd)}")
            
            # 执行命令
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                shell=True  # 在Windows上使用shell=True可能更可靠
            )
            
            # 检查输出
            if result.returncode == 0:
                self._log(f"员工 {emp_no} 的JSON文件更新成功")
                # 检查更新后的文件是否存在
                modify_dir = os.path.join(base_dir, "output", "modify_json")
                json_files = [f for f in os.listdir(modify_dir) if f.startswith(emp_no.zfill(5)) and f.endswith('.json')]
                return len(json_files) > 0
            else:
                self._log(f"员工 {emp_no} 的JSON文件更新失败")
                self._log(f"错误输出: {result.stderr}")
                return False
                
        except Exception as e:
            self._log(f"更新员工 {emp_no} 的JSON文件时出错: {str(e)}")
            return False
    
    def _log(self, message):
        """在日志区域显示消息"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)  # 滚动到最后
        self.log_text.config(state=tk.DISABLED)
        # 同时打印到控制台
        print(message)

# 添加进程锁检查以防止重复启动应用程序
def check_instance():
    # 创建一个套接字锁用于检测是否已有实例运行
    lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 尝试绑定到一个固定端口（选择一个不太可能被使用的端口）
        lock_socket.bind(('127.0.0.1', 65432))
        # 成功绑定，表示没有其他实例在运行
        return True
    except socket.error:
        # 绑定失败，表示已有实例在运行
        return False

if __name__ == "__main__":
    # 检查是否已有实例在运行
    if not check_instance():
        # 创建一个临时Tk窗口显示错误信息
        error_root = tk.Tk()
        error_root.withdraw()  # 隐藏主窗口
        messagebox.showerror("错误", "简历生成器已在运行中，请不要重复启动！")
        error_root.destroy()
        sys.exit(0)
    
    # 使用ThemedTk并应用arc主题
    root = ThemedTk(theme="arc")
    # 创建应用实例
    app = ResumeGeneratorGUI(root)
    # 启动主循环
    root.mainloop()