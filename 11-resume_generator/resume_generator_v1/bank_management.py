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

# 获取程序所在目录作为基础目录
if getattr(sys, 'frozen', False):
    # 如果是打包后的exe文件
    base_dir = os.path.dirname(sys.executable)
else:
    # 如果是直接运行的Python脚本
    base_dir = os.path.dirname(os.path.abspath(__file__))

class BankManagementDialog:
    """银行管理对话框"""
    
    def __init__(self, parent=None):
        """初始化银行管理对话框"""
        # 创建主窗口，根据是否有父窗口选择窗口类型
        if parent:
              self.root = tk.Toplevel(parent)
              self.root.transient(parent)  # 设置为父窗口的临时窗口
              self.root.grab_set()  # 模态化，阻止父窗口交互
              self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        else:
            # 如果没有父窗口，使用ThemedTk并应用arc主题
            self.root = ThemedTk(theme="arc")
        
        self.root.title("银行管理")
        self.root.geometry("700x800")
        
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
        
        # 存储当前选中的银行
        self.selected_bank = ""
        
        # 初始化界面
        self._init_ui()
        # 加载银行列表
        self._load_bank_list()
        
        # 设置窗口关闭事件，用于刷新主界面银行下拉框
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # 设置窗口属性
        if parent:
            self.root.transient(parent)
            self.root.grab_set()
    
    def _init_ui(self):
        """初始化用户界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. 图像文件选择部分
        image_frame = ttk.LabelFrame(main_frame, text="图像文件选择", padding="10")
        image_frame.pack(fill=tk.X, pady=10)
        
        ttk.Label(image_frame, text="图像路径:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_png_path = tk.StringVar()
        ttk.Entry(image_frame, textvariable=self.bank_png_path, width=50, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Button(image_frame, text="浏览", command=self._select_png_file).pack(side=tk.LEFT, padx=5)
        
        # 2. 银行名称输入部分
        name_frame = ttk.LabelFrame(main_frame, text="银行信息", padding="10")
        name_frame.pack(fill=tk.X, pady=10)
        
        ttk.Label(name_frame, text="银行名称:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_name = tk.StringVar()
        ttk.Entry(name_frame, textvariable=self.bank_name, width=50, font=self.font_config['entry']).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        ttk.Label(name_frame, text="(如: 恒丰银行)", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        
        # 3. 银行列表部分
        bank_list_frame = ttk.LabelFrame(main_frame, text="银行列表", padding="10")
        bank_list_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # 创建银行列表树状视图
        self.bank_tree = ttk.Treeview(bank_list_frame, columns=('bank_name'), show='headings')
        self.bank_tree.heading('bank_name', text='银行名称')
        self.bank_tree.column('bank_name', width=300, anchor='w')
        self.bank_tree.pack(fill=tk.BOTH, expand=True, side=tk.LEFT, padx=5)
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(bank_list_frame, orient=tk.VERTICAL, command=self.bank_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.bank_tree.configure(yscroll=scrollbar.set)
        
        # 绑定选择事件
        self.bank_tree.bind('<<TreeviewSelect>>', self._on_bank_select)
        
        # 4. 执行操作部分
        action_frame = ttk.LabelFrame(main_frame, text="执行操作", padding="10")
        action_frame.pack(fill=tk.X, pady=10)
        
        button_frame = ttk.Frame(action_frame)
        button_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(button_frame, text="添加/更新银行", command=self._add_bank, style="Accent.TButton").pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="修改选中银行", command=self._modify_bank).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="删除选中银行", command=self._delete_bank).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="关闭", command=self._on_closing).pack(side=tk.RIGHT, padx=10)
        
        # 5. 日志栏
        log_frame = ttk.LabelFrame(main_frame, text="处理日志", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.bank_log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=self.font_config['text'], height=8)
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
        """选择图像文件（支持PNG和JPG格式）"""
        # 确保窗口在对话框打开前获得焦点
        self.root.lift()
        self.root.focus_force()
        
        file_path = filedialog.askopenfilename(
            filetypes=[
                ("图片文件", "*.png;*.jpg;*.jpeg"),
                ("PNG图片", "*.png"),
                ("JPG图片", "*.jpg;*.jpeg")
            ],
            parent=self.root,
            title="选择银行图标"
        )
        
        if file_path:
            # 确保路径使用正斜杠或双反斜杠，避免显示问题
            file_path = file_path.replace('/', '\\')
            self.bank_png_path.set(file_path)
            # 选择后再次确保窗口保持焦点
            self.root.lift()
            self.root.focus_force()
    
    def _load_bank_list(self):
        """加载银行列表到树状视图"""
        # 清空现有数据
        for item in self.bank_tree.get_children():
            self.bank_tree.delete(item)
        
        # 读取银行列表配置
        bank_list_config = os.path.join(base_dir, 'config', 'bank_list.config')
        try:
            if os.path.exists(bank_list_config):
                with open(bank_list_config, 'r', encoding='utf-8') as f:
                    bank_list = [line.strip() for line in f.readlines() if line.strip()]
                    for bank in sorted(bank_list):
                        self.bank_tree.insert('', tk.END, values=(bank,))
                self._log(f"已加载 {len(bank_list)} 个银行")
            else:
                self._log("银行配置文件不存在，显示为空列表")
        except Exception as e:
            self._log(f"加载银行列表出错: {str(e)}")
    
    def _on_bank_select(self, event):
        """银行列表选择事件处理"""
        selected_items = self.bank_tree.selection()
        if selected_items:
            item = selected_items[0]
            self.selected_bank = self.bank_tree.item(item, 'values')[0]
            self.bank_name.set(self.selected_bank)
            # 尝试加载对应的logo文件
            ico_path = os.path.join(base_dir, 'resources', 'bank_pics', f"{self.selected_bank}.ico")
            if os.path.exists(ico_path):
                # 由于我们不能直接设置ICO文件到PNG路径输入框，这里不设置路径
                self.bank_png_path.set("")
            else:
                self.bank_png_path.set("")
    
    def _log(self, message):
        """在日志区域显示消息"""
        print(f"[银行管理] {message}")  # 添加print日志用于调试
        self.bank_log_text.insert(tk.END, message + "\n")
        self.bank_log_text.see(tk.END)
    
    def _add_bank(self):
        """添加银行信息并转换图标"""
        # 获取输入值
        png_path = self.bank_png_path.get().strip()
        bank_name = self.bank_name.get().strip()
        
        # 验证输入
        if not png_path:
            messagebox.showerror("错误", "请选择图像文件")
            return
        
        if not bank_name:
            messagebox.showerror("错误", "请输入银行名称")
            return
        
        if not os.path.exists(png_path):
            messagebox.showerror("错误", "选择的图像文件不存在")
            return
        
        # 检查文件格式
        file_ext = os.path.splitext(png_path)[1].lower()
        if file_ext not in ['.png', '.jpg', '.jpeg']:
            messagebox.showerror("错误", "请选择PNG或JPG格式的图像文件")
            return
        
        # 清空日志
        self.bank_log_text.delete(1.0, tk.END)
        self.bank_log_text.insert(tk.END, f"开始处理银行信息: {bank_name}\n")
        
        # 定义转换函数
        def convert_icon():
            try:
                print(f"[银行管理] 开始转换图标: {png_path} -> {bank_name}.ico")
                
                # 设置输出路径
                bank_pics_dir = os.path.join(base_dir, 'resources', 'bank_pics')
                os.makedirs(bank_pics_dir, exist_ok=True)
                print(f"[银行管理] 创建目录: {bank_pics_dir}")
                
                # 输出ICO文件路径
                ico_filename = f"{bank_name}.ico"
                ico_path = os.path.join(bank_pics_dir, ico_filename)
                print(f"[银行管理] 输出路径: {ico_path}")
                
                # 记录日志
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"输出路径: {ico_path}\n"))
                
                # 根据程序运行模式选择不同的转换方式
                print(f"[银行管理] 程序模式: {'打包为exe' if getattr(sys, 'frozen', False) else 'Python脚本'}")
                
                # 直接导入icon_converter模块而不是通过subprocess调用
                converter_module = None
                converter_path = os.path.join(base_dir, 'package', 'utils', 'icon_converter.py')
                print(f"[银行管理] converter_path: {converter_path}, 是否存在: {os.path.exists(converter_path)}")
                
                # 尝试动态导入模块
                try:
                    # 如果是exe模式，尝试直接导入
                    if getattr(sys, 'frozen', False):
                        print("[银行管理] 尝试直接导入icon_converter模块")
                        import importlib.util
                        spec = importlib.util.spec_from_file_location("icon_converter", converter_path)
                        if spec and spec.loader:
                            converter_module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(converter_module)
                            print("[银行管理] 成功导入icon_converter模块")
                        else:
                            print("[银行管理] 无法加载icon_converter模块，使用备用方法")
                    else:
                        print("[银行管理] 脚本模式，使用subprocess调用")
                except Exception as e:
                    print(f"[银行管理] 导入icon_converter模块失败: {str(e)}")
                
                # 根据是否成功导入模块和程序模式选择不同的执行方式
                conversion_success = False
                error_message = ""
                
                if getattr(sys, 'frozen', False):
                    # exe模式 - 优先使用直接导入方式
                    print("[银行管理] EXE模式 - 尝试使用直接导入转换")
                    
                    # 如果模块导入失败，尝试直接实现简单的转换逻辑
                    if not converter_module or not hasattr(converter_module, 'convert_png_to_ico'):
                        print("[银行管理] EXE模式 - 尝试直接实现转换逻辑")
                        try:
                            # 直接导入PIL库进行转换
                            from PIL import Image, ImageDraw
                            
                            print(f"[银行管理] EXE模式 - 使用PIL直接转换: {png_path} -> {ico_path}")
                            
                            # 基本的PNG到ICO转换逻辑
                            with Image.open(png_path) as img:
                                # 调整图像大小
                                img_resized = img.resize((32, 32), Image.LANCZOS)
                                
                                # 保存为ICO文件
                                img_resized.save(ico_path, format='ICO', sizes=[(32, 32)])
                            
                            print(f"[银行管理] EXE模式 - 转换完成: {ico_path}")
                            conversion_success = os.path.exists(ico_path)
                        except Exception as e:
                            error_message = f"EXE模式直接转换失败: {str(e)}"
                            print(f"[银行管理] {error_message}")
                            import traceback
                            print(f"[银行管理] 异常栈: {traceback.format_exc()}")
                    else:
                        # 使用导入的模块直接调用函数
                        print("[银行管理] EXE模式 - 使用导入的模块执行转换")
                        try:
                            result = converter_module.convert_png_to_ico(png_path, ico_path, size=32)
                            print(f"[银行管理] EXE模式 - 转换结果: {result}")
                            conversion_success = os.path.exists(ico_path)
                        except Exception as e:
                            error_message = f"EXE模式模块转换失败: {str(e)}"
                            print(f"[银行管理] {error_message}")
                else:
                    # 脚本模式 - 使用subprocess调用
                    print("[银行管理] 脚本模式 - 使用subprocess执行转换")
                    # 调用icon_converter.py进行转换
                    cmd = [sys.executable, converter_path, png_path, ico_path, '--size', '32']
                    print(f"[银行管理] 执行命令: {' '.join(cmd)}")
                    
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
                    
                    print(f"[银行管理] 转换输出: {stdout}")
                    if stderr:
                        print(f"[银行管理] 转换错误: {stderr}")
                        error_message = stderr
                    
                    self.root.after(0, lambda: self.bank_log_text.insert(tk.END, stdout))
                    if stderr:
                        self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"错误: {stderr}\n"))
                    
                    # 检查转换是否成功
                    conversion_success = (process.returncode == 0 and os.path.exists(ico_path))
                
                if conversion_success:
                    print("[银行管理] 转换成功，更新银行列表配置")
                    # 更新银行列表配置
                    bank_list_config = os.path.join(base_dir, 'config', 'bank_list.config')
                    os.makedirs(os.path.dirname(bank_list_config), exist_ok=True)
                    print(f"[银行管理] 银行列表配置路径: {bank_list_config}")
                    
                    # 读取现有银行列表
                    bank_list = []
                    if os.path.exists(bank_list_config):
                        print("[银行管理] 读取现有银行列表")
                        with open(bank_list_config, 'r', encoding='utf-8') as f:
                            bank_list = [line.strip() for line in f.readlines() if line.strip()]
                    
                    # 添加新银行（如果不存在）
                    if bank_name not in bank_list:
                        print(f"[银行管理] 添加新银行: {bank_name}")
                        bank_list.append(bank_name)
                        # 保存银行列表
                        with open(bank_list_config, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(bank_list))
                        print(f"[银行管理] 银行列表已保存，共 {len(bank_list)} 个银行")
                        self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"\n银行列表已更新，当前包含 {len(bank_list)} 个银行\n"))
                    else:
                        self.root.after(0, lambda: self.bank_log_text.insert(tk.END, "\n银行名称已存在于配置中，已更新图标\n"))
                    
                    # 重新加载银行列表
                    self.root.after(500, self._load_bank_list)
                    
                    self.root.after(0, lambda: [
                        self.bank_log_text.insert(tk.END, "\n✓ 银行添加/更新成功！"),
                        self.bank_log_text.see(tk.END)
                    ])
                    self.root.after(500, lambda: messagebox.showinfo("成功", "银行添加/更新成功！"))
                else:
                    print(f"[银行管理] 转换失败，ICO文件不存在: {ico_path}")
                    if error_message:
                        print(f"[银行管理] 错误详情: {error_message}")
                    self.root.after(0, lambda: [
                        self.bank_log_text.insert(tk.END, "\n[ERR] 银行添加失败，请查看日志！"),
                        self.bank_log_text.see(tk.END)
                    ])
                    self.root.after(500, lambda: messagebox.showerror("失败", "银行添加失败，请查看日志"))
                    
            except Exception as e:
                print(f"[银行管理] 处理异常: {str(e)}")
                import traceback
                print(f"[银行管理] 异常栈: {traceback.format_exc()}")
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"\n[ERR] 处理出错: {str(e)}\n"))
                self.root.after(0, lambda: self.bank_log_text.insert(tk.END, f"异常栈: {traceback.format_exc()}\n"))
                self.root.after(500, lambda: messagebox.showerror("错误", f"处理出错: {str(e)}"))
        
        # 在新线程中执行转换
        threading.Thread(target=convert_icon, daemon=True).start()
    
    def _modify_bank(self):
        """修改选中的银行名称"""
        if not self.selected_bank:
            messagebox.showerror("错误", "请先从列表中选择要修改的银行")
            return
        
        new_bank_name = self.bank_name.get().strip()
        if not new_bank_name:
            messagebox.showerror("错误", "请输入新的银行名称")
            return
        
        if self.selected_bank == new_bank_name:
            messagebox.showinfo("提示", "银行名称未更改")
            return
        
        try:
            # 读取银行列表配置
            bank_list_config = os.path.join(base_dir, 'config', 'bank_list.config')
            
            # 读取现有银行列表
            bank_list = []
            if os.path.exists(bank_list_config):
                with open(bank_list_config, 'r', encoding='utf-8') as f:
                    bank_list = [line.strip() for line in f.readlines() if line.strip()]
            
            # 检查新名称是否已存在
            if new_bank_name in bank_list:
                messagebox.showerror("错误", f"银行名称 '{new_bank_name}' 已存在")
                return
            
            # 更新银行列表
            if self.selected_bank in bank_list:
                bank_list[bank_list.index(self.selected_bank)] = new_bank_name
                
                # 保存银行列表
                with open(bank_list_config, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(bank_list))
                
                # 更新logo文件
                old_ico_path = os.path.join(base_dir, 'resources', 'bank_pics', f"{self.selected_bank}.ico")
                new_ico_path = os.path.join(base_dir, 'resources', 'bank_pics', f"{new_bank_name}.ico")
                
                if os.path.exists(old_ico_path):
                    os.rename(old_ico_path, new_ico_path)
                    self._log(f"Logo文件已从 {self.selected_bank}.ico 重命名为 {new_bank_name}.ico")
                
                self._log(f"银行 '{self.selected_bank}' 已成功修改为 '{new_bank_name}'")
                messagebox.showinfo("成功", "银行名称修改成功")
                
                # 重新加载银行列表
                self._load_bank_list()
                # 重置选择
                self.selected_bank = ""
            else:
                self._log(f"银行 '{self.selected_bank}' 不在配置文件中")
                messagebox.showerror("错误", "银行不存在")
                
        except Exception as e:
            error_msg = f"修改银行名称时出错: {str(e)}"
            self._log(error_msg)
            messagebox.showerror("错误", error_msg)
    
    def _delete_bank(self):
        """删除选中的银行"""
        if not self.selected_bank:
            messagebox.showerror("错误", "请先从列表中选择要删除的银行")
            return
        
        # 确认删除
        if not messagebox.askyesno("确认删除", f"确定要删除银行 '{self.selected_bank}' 吗？\n删除后将无法恢复。"):
            return
        
        try:
            # 读取银行列表配置
            bank_list_config = os.path.join(base_dir, 'config', 'bank_list.config')
            
            # 读取现有银行列表
            bank_list = []
            if os.path.exists(bank_list_config):
                with open(bank_list_config, 'r', encoding='utf-8') as f:
                    bank_list = [line.strip() for line in f.readlines() if line.strip()]
            
            # 删除银行
            if self.selected_bank in bank_list:
                bank_list.remove(self.selected_bank)
                
                # 保存银行列表
                with open(bank_list_config, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(bank_list))
                
                # 删除logo文件
                ico_path = os.path.join(base_dir, 'resources', 'bank_pics', f"{self.selected_bank}.ico")
                if os.path.exists(ico_path):
                    os.remove(ico_path)
                    self._log(f"Logo文件 {self.selected_bank}.ico 已删除")
                
                self._log(f"银行 '{self.selected_bank}' 已成功从配置文件中删除")
                messagebox.showinfo("成功", f"银行 '{self.selected_bank}' 删除成功")
                
                # 重新加载银行列表
                self._load_bank_list()
                # 重置输入框和选择
                self.bank_name.set("")
                self.bank_png_path.set("")
                self.selected_bank = ""
            else:
                self._log(f"银行 '{self.selected_bank}' 不在配置文件中")
                messagebox.showerror("错误", "银行不存在")
                
        except Exception as e:
            error_msg = f"删除银行时出错: {str(e)}"
            self._log(error_msg)
            messagebox.showerror("错误", error_msg)
    
    def _on_closing(self):
        """窗口关闭事件处理 - 用于通知主界面刷新银行列表"""
        # 创建刷新标志文件，用于通知主界面需要刷新银行列表
        refresh_flag_path = os.path.join(base_dir, 'config', 'refresh_bank_list.flag')
        try:
            with open(refresh_flag_path, 'w', encoding='utf-8') as f:
                f.write('1')
        except Exception as e:
            print(f"创建刷新标志文件失败: {str(e)}")
        
        # 关闭窗口
        self.root.destroy()

    def _on_close(self):
        """处理窗口关闭事件"""
        self.root.grab_release()
        self.root.destroy()
    
    def run(self):
        """运行对话框（独立模式）"""
        if not hasattr(self.root, 'master') or not self.root.master:
            self.root.mainloop()
        else:
            self.root.deiconify()


def main():
    """主函数"""
    app = BankManagementDialog()
    app.run()


if __name__ == "__main__":
    main()