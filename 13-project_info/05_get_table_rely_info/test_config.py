import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import yaml
import os

class ConfigFormApp:
    def __init__(self, root, config_path="config.yaml"):
        self.root = root
        self.config_path = config_path
        self.config = self.load_config()
        
        self.root.title("SQL依赖关系分析工具 - 配置编辑器")
        self.root.geometry("800x700")
        
        # 创建主框架
        self.main_frame = ttk.Frame(root, padding="20")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置滚动条
        self.canvas = tk.Canvas(self.main_frame)
        self.scrollbar = ttk.Scrollbar(self.main_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)
        
        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )
        
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        # 创建标签页
        self.notebook = ttk.Notebook(self.scrollable_frame)
        self.notebook.pack(fill="both", expand=True, pady=10)
        
        # 创建各个配置页
        self.create_processing_tab()
        self.create_projects_tab()
        self.create_templates_tab()
        self.create_regex_tab()
        self.create_filters_tab()
        self.create_output_tab()
        self.create_progress_tab()
        
        # 控制按钮
        self.create_control_buttons()
        
        # 布局滚动区域
        self.canvas.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(0, weight=1)
        
        # 为根窗口添加权重配置，确保内容能够完全显示
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            messagebox.showerror("错误", f"配置文件 {self.config_path} 未找到！")
            return {}
        except yaml.YAMLError as e:
            messagebox.showerror("错误", f"配置文件格式错误: {e}")
            return {}
    
    def create_processing_tab(self):
        """创建处理配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="处理配置")
        
        frame = ttk.LabelFrame(tab, text="SQL文件处理配置", padding="15")
        frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 去除后缀配置
        ttk.Label(frame, text="是否去除后缀:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.remove_suffix_var = tk.StringVar(value=self.config.get('processing', {}).get('remove_suffix', 'Y'))
        ttk.Combobox(frame, textvariable=self.remove_suffix_var, values=['Y', 'N'], width=5, state="readonly").grid(row=0, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(frame, text="后缀标识符:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.suffix_identifier_var = tk.StringVar(value=self.config.get('processing', {}).get('suffix_identifier', '_PC'))
        ttk.Entry(frame, textvariable=self.suffix_identifier_var, width=20).grid(row=1, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(frame, text="筛选模式（用于筛选来源库和表名）:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.filter_schema_var = tk.StringVar(value=self.config.get('processing', {}).get('filter_schema', 'AGL'))
        ttk.Entry(frame, textvariable=self.filter_schema_var, width=20).grid(row=2, column=1, sticky=tk.W, pady=5)
        
        # 填充空白
        for i in range(3):
            frame.rowconfigure(i, weight=1)
        frame.columnconfigure(1, weight=1)
    
    def create_projects_tab(self):
        """创建项目配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="项目配置")
        
        # 创建Treeview显示项目
        columns = ("项目名称", "前缀", "主题", "描述")
        self.project_tree = ttk.Treeview(tab, columns=columns, show="headings", height=10)
        
        for col in columns:
            self.project_tree.heading(col, text=col)
            self.project_tree.column(col, width=100)
        
        # 填充数据
        projects = self.config.get('projects', {})
        for proj_name, proj_data in projects.items():
            self.project_tree.insert("", "end", values=(
                proj_name,
                proj_data.get('prefix', ''),
                proj_data.get('theme', ''),
                proj_data.get('description', '')
            ))
        
        # 创建滚动条
        scrollbar = ttk.Scrollbar(tab, orient="vertical", command=self.project_tree.yview)
        self.project_tree.configure(yscrollcommand=scrollbar.set)
        
        # 布局
        self.project_tree.pack(side=tk.LEFT, fill="both", expand=True, padx=10, pady=10)
        scrollbar.pack(side=tk.RIGHT, fill="y", pady=10)
        
        # 编辑按钮
        btn_frame = ttk.Frame(tab)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="添加项目", command=self.add_project).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="编辑项目", command=self.edit_project).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="删除项目", command=self.delete_project).pack(side=tk.LEFT, padx=5)
    
    def create_templates_tab(self):
        """创建文件模板配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="文件模板")
        
        notebook = ttk.Notebook(tab)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        templates = self.config.get('file_templates', {})
        self.template_vars = {}
        
        for template_key, template_data in templates.items():
            template_tab = ttk.Frame(notebook)
            notebook.add(template_tab, text=template_data.get('name', template_key))
            
            frame = ttk.LabelFrame(template_tab, text=f"{template_data.get('name')} 配置", padding="15")
            frame.pack(fill="both", expand=True, padx=10, pady=10)
            
            row = 0
            # 模板名称
            ttk.Label(frame, text="模板名称:").grid(row=row, column=0, sticky=tk.W, pady=5)
            name_var = tk.StringVar(value=template_data.get('name', ''))
            ttk.Entry(frame, textvariable=name_var, width=30).grid(row=row, column=1, sticky=tk.W, pady=5)
            row += 1
            
            # 表名行号
            ttk.Label(frame, text="表名所在行号（0-based）:").grid(row=row, column=0, sticky=tk.W, pady=5)
            table_line_var = tk.StringVar(value=str(template_data.get('lines', {}).get('table_name', 8)))
            ttk.Entry(frame, textvariable=table_line_var, width=10).grid(row=row, column=1, sticky=tk.W, pady=5)
            row += 1
            
            # 开发人员行号
            ttk.Label(frame, text="开发人员所在行号:").grid(row=row, column=0, sticky=tk.W, pady=5)
            dev_line_var = tk.StringVar(value=str(template_data.get('lines', {}).get('developer', 14)))
            ttk.Entry(frame, textvariable=dev_line_var, width=10).grid(row=row, column=1, sticky=tk.W, pady=5)
            row += 1
            
            # 分隔符
            ttk.Label(frame, text="分隔符:").grid(row=row, column=0, sticky=tk.W, pady=5)
            delimiter_var = tk.StringVar(value=template_data.get('delimiter', ':'))
            ttk.Entry(frame, textvariable=delimiter_var, width=10).grid(row=row, column=1, sticky=tk.W, pady=5)
            row += 1
            
            # 文件匹配模式
            ttk.Label(frame, text="文件匹配模式（正则）:").grid(row=row, column=0, sticky=tk.W, pady=5)
            pattern_var = tk.StringVar(value=template_data.get('file_pattern', ''))
            ttk.Entry(frame, textvariable=pattern_var, width=50).grid(row=row, column=1, sticky=tk.W, pady=5)
            row += 1
            
            # 保存变量
            self.template_vars[template_key] = {
                'name': name_var,
                'table_line': table_line_var,
                'dev_line': dev_line_var,
                'delimiter': delimiter_var,
                'pattern': pattern_var
            }
            
            for i in range(row):
                frame.rowconfigure(i, weight=1)
            frame.columnconfigure(1, weight=1)
    
    def create_regex_tab(self):
        """创建正则表达式配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="正则表达式")
        
        # 表引用正则
        ref_frame = ttk.LabelFrame(tab, text="表引用正则表达式", padding="15")
        ref_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.ref_regex_vars = []
        ref_patterns = self.config.get('regex_patterns', {}).get('table_reference', [])
        
        ttk.Label(ref_frame, text="表引用匹配模式:").grid(row=0, column=0, sticky=tk.W, pady=5)
        
        for i, pattern in enumerate(ref_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(ref_frame, textvariable=var, width=80).grid(row=i+1, column=0, sticky=(tk.W, tk.E), pady=2)
            self.ref_regex_vars.append(var)
        
        # 添加/删除按钮
        ref_btn_frame = ttk.Frame(ref_frame)
        ref_btn_frame.grid(row=len(ref_patterns)+1, column=0, sticky=tk.W, pady=10)
        ttk.Button(ref_btn_frame, text="添加模式", command=self.add_ref_pattern).pack(side=tk.LEFT, padx=5)
        ttk.Button(ref_btn_frame, text="删除最后一条", command=self.remove_last_ref_pattern).pack(side=tk.LEFT, padx=5)
        
        # 清理模式
        cleanup_frame = ttk.LabelFrame(tab, text="表名清理模式", padding="15")
        cleanup_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.cleanup_vars = []
        cleanup_patterns = self.config.get('regex_patterns', {}).get('table_cleanup', [])
        
        ttk.Label(cleanup_frame, text="表名清理后缀模式:").grid(row=0, column=0, sticky=tk.W, pady=5)
        
        for i, pattern in enumerate(cleanup_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(cleanup_frame, textvariable=var, width=30).grid(row=i+1, column=0, sticky=tk.W, pady=2)
            self.cleanup_vars.append(var)
        
        cleanup_btn_frame = ttk.Frame(cleanup_frame)
        cleanup_btn_frame.grid(row=len(cleanup_patterns)+1, column=0, sticky=tk.W, pady=10)
        ttk.Button(cleanup_btn_frame, text="添加清理模式", command=self.add_cleanup_pattern).pack(side=tk.LEFT, padx=5)
        ttk.Button(cleanup_btn_frame, text="删除最后一条", command=self.remove_last_cleanup_pattern).pack(side=tk.LEFT, padx=5)
        
        # 文件扩展名
        ext_frame = ttk.LabelFrame(tab, text="文件扩展名", padding="15")
        ext_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ttk.Label(ext_frame, text="SQL文件扩展名正则:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.file_ext_var = tk.StringVar(value=self.config.get('regex_patterns', {}).get('file_extension', '\\.(hql|sql)$'))
        ttk.Entry(ext_frame, textvariable=self.file_ext_var, width=30).grid(row=0, column=1, sticky=tk.W, pady=5)
    
    def create_filters_tab(self):
        """创建过滤规则配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="过滤规则")
        
        # 基础过滤选项
        basic_frame = ttk.LabelFrame(tab, text="基础过滤选项", padding="15")
        basic_frame.pack(fill="x", padx=10, pady=5)
        
        self.exclude_self_var = tk.BooleanVar(value=self.config.get('filters', {}).get('exclude_self_reference', True))
        ttk.Checkbutton(basic_frame, text="排除自引用", variable=self.exclude_self_var).grid(row=0, column=0, sticky=tk.W, pady=5)
        
        self.exclude_same_layer_var = tk.BooleanVar(value=self.config.get('filters', {}).get('exclude_same_layer', True))
        ttk.Checkbutton(basic_frame, text="排除同层依赖", variable=self.exclude_same_layer_var).grid(row=1, column=0, sticky=tk.W, pady=5)
        
        # 排除模式
        exclude_frame = ttk.LabelFrame(tab, text="排除模式（正则表达式）", padding="15")
        exclude_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.exclude_vars = []
        exclude_patterns = self.config.get('filters', {}).get('exclude_patterns', [])
        
        ttk.Label(exclude_frame, text="排除的表名前缀模式:").grid(row=0, column=0, sticky=tk.W, pady=5)
        
        for i, pattern in enumerate(exclude_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(exclude_frame, textvariable=var, width=30).grid(row=i+1, column=0, sticky=tk.W, pady=2)
            self.exclude_vars.append(var)
        
        exclude_btn_frame = ttk.Frame(exclude_frame)
        exclude_btn_frame.grid(row=len(exclude_patterns)+1, column=0, sticky=tk.W, pady=10)
        ttk.Button(exclude_btn_frame, text="添加排除模式", command=self.add_exclude_pattern).pack(side=tk.LEFT, padx=5)
        ttk.Button(exclude_btn_frame, text="删除最后一条", command=self.remove_last_exclude_pattern).pack(side=tk.LEFT, padx=5)
        
        # 包含模式
        include_frame = ttk.LabelFrame(tab, text="包含模式（正则表达式）", padding="15")
        include_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        self.include_vars = []
        include_patterns = self.config.get('filters', {}).get('include_patterns', [])
        
        ttk.Label(include_frame, text="包含的表名模式（留空表示全部）:").grid(row=0, column=0, sticky=tk.W, pady=5)
        
        for i, pattern in enumerate(include_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(include_frame, textvariable=var, width=30).grid(row=i+1, column=0, sticky=tk.W, pady=2)
            self.include_vars.append(var)
        
        include_btn_frame = ttk.Frame(include_frame)
        include_btn_frame.grid(row=len(include_patterns)+1, column=0, sticky=tk.W, pady=10)
        ttk.Button(include_btn_frame, text="添加包含模式", command=self.add_include_pattern).pack(side=tk.LEFT, padx=5)
        ttk.Button(include_btn_frame, text="删除最后一条", command=self.remove_last_include_pattern).pack(side=tk.LEFT, padx=5)
    
    def create_output_tab(self):
        """创建输出配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="输出配置")
        
        notebook = ttk.Notebook(tab)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 基础列配置
        basic_tab = ttk.Frame(notebook)
        notebook.add(basic_tab, text="基础列")
        self.create_column_grid(basic_tab, 'basic_columns')
        
        # 扩展列配置
        extended_tab = ttk.Frame(notebook)
        notebook.add(extended_tab, text="扩展列")
        self.create_column_grid(extended_tab, 'extended_columns')
    
    def create_column_grid(self, parent, column_type):
        """创建列配置网格"""
        columns = self.config.get('output', {}).get(column_type, [])
        self.column_vars = {}
        
        # 创建Treeview
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        columns_def = ["列名", "显示标题", "宽度", "隐藏"]
        tree = ttk.Treeview(tree_frame, columns=columns_def, show="headings", height=10)
        
        for col in columns_def:
            tree.heading(col, text=col)
            tree.column(col, width=100)
        
        # 填充数据
        for col_data in columns:
            tree.insert("", "end", values=(
                col_data.get('name', ''),
                col_data.get('title', ''),
                col_data.get('width', ''),
                "是" if col_data.get('hidden', False) else "否"
            ))
        
        # 滚动条
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        
        tree.pack(side=tk.LEFT, fill="both", expand=True)
        scrollbar.pack(side=tk.RIGHT, fill="y")
        
        # 按钮
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill="x", padx=10, pady=5)
        
        ttk.Button(btn_frame, text="添加列", 
                  command=lambda: self.add_column(tree, column_type)).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="编辑列", 
                  command=lambda: self.edit_column(tree, column_type)).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="删除列", 
                  command=lambda: self.delete_column(tree)).pack(side=tk.LEFT, padx=5)
    
    def create_progress_tab(self):
        """创建进度条配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="进度条配置")
        
        frame = ttk.LabelFrame(tab, text="进度条显示配置", padding="15")
        frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        progress_config = self.config.get('progress', {})
        
        ttk.Label(frame, text="进度条长度:").grid(row=0, column=0, sticky=tk.W, pady=10)
        self.bar_length_var = tk.IntVar(value=progress_config.get('bar_length', 30))
        ttk.Spinbox(frame, from_=10, to=100, textvariable=self.bar_length_var, width=10).grid(row=0, column=1, sticky=tk.W, pady=10)
        
        ttk.Label(frame, text="显示百分比:").grid(row=1, column=0, sticky=tk.W, pady=10)
        self.show_percentage_var = tk.BooleanVar(value=progress_config.get('show_percentage', True))
        ttk.Checkbutton(frame, text="是", variable=self.show_percentage_var).grid(row=1, column=1, sticky=tk.W, pady=10)
        
        ttk.Label(frame, text="更新频率（处理多少个文件更新一次）:").grid(row=2, column=0, sticky=tk.W, pady=10)
        self.update_freq_var = tk.IntVar(value=progress_config.get('update_frequency', 1))
        ttk.Spinbox(frame, from_=1, to=100, textvariable=self.update_freq_var, width=10).grid(row=2, column=1, sticky=tk.W, pady=10)
    
    def create_control_buttons(self):
        """创建控制按钮"""
        btn_frame = ttk.Frame(self.scrollable_frame)
        btn_frame.pack(fill="x", pady=20)
        
        ttk.Button(btn_frame, text="保存配置", command=self.save_config, width=15).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="加载配置", command=self.load_config_file, width=15).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="重置表单", command=self.reset_form, width=15).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="退出", command=self.root.quit, width=15).pack(side=tk.LEFT, padx=10)
    
    # 以下是一些辅助方法（按钮命令函数）
    def add_project(self):
        """添加项目对话框"""
        # 创建添加项目的对话框
        dialog = tk.Toplevel(self.root)
        dialog.title("添加项目")
        dialog.geometry("400x300")
        dialog.transient(self.root)  # 设置为主窗口的子窗口
        dialog.grab_set()  # 模态对话框
        
        # 创建表单控件
        ttk.Label(dialog, text="项目名称:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        name_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=name_var, width=30).grid(row=0, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="前缀:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=10)
        prefix_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=prefix_var, width=30).grid(row=1, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="主题:").grid(row=2, column=0, sticky=tk.W, padx=10, pady=10)
        theme_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=theme_var, width=30).grid(row=2, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="描述:").grid(row=3, column=0, sticky=tk.NW, padx=10, pady=10)
        desc_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=desc_var, width=30).grid(row=3, column=1, padx=10, pady=10)
        
        # 保存按钮回调函数
        def save_new_project():
            name = name_var.get().strip()
            if not name:
                messagebox.showerror("错误", "项目名称不能为空！")
                return
                
            # 检查项目是否已存在
            if name in self.config.get('projects', {}):
                messagebox.showerror("错误", f"项目 '{name}' 已存在！")
                return
                
            # 更新配置
            if 'projects' not in self.config:
                self.config['projects'] = {}
                
            self.config['projects'][name] = {
                'prefix': prefix_var.get().strip(),
                'theme': theme_var.get().strip(),
                'description': desc_var.get().strip()
            }
            
            # 更新Treeview显示
            self.project_tree.insert("", "end", values=(name, prefix_var.get(), theme_var.get(), desc_var.get()))
            
            # 关闭对话框
            dialog.destroy()
            messagebox.showinfo("成功", f"项目 '{name}' 添加成功！")
        
        # 创建按钮
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="保存", command=save_new_project).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 调整窗口布局
        dialog.grid_columnconfigure(1, weight=1)
    
    def edit_project(self):
        """编辑项目对话框"""
        # 获取选中的项目
        selected_items = self.project_tree.selection()
        if not selected_items:
            messagebox.showwarning("警告", "请先选择要编辑的项目！")
            return
        
        selected_item = selected_items[0]  # 只处理第一个选中的项目
        item_values = self.project_tree.item(selected_item, "values")
        old_name = item_values[0]  # 获取原项目名称
        
        # 创建编辑项目的对话框
        dialog = tk.Toplevel(self.root)
        dialog.title("编辑项目")
        dialog.geometry("400x300")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 创建表单控件并填充现有数据
        ttk.Label(dialog, text="项目名称:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        name_var = tk.StringVar(value=old_name)
        ttk.Entry(dialog, textvariable=name_var, width=30).grid(row=0, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="前缀:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=10)
        prefix_var = tk.StringVar(value=item_values[1] if len(item_values) > 1 else "")
        ttk.Entry(dialog, textvariable=prefix_var, width=30).grid(row=1, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="主题:").grid(row=2, column=0, sticky=tk.W, padx=10, pady=10)
        theme_var = tk.StringVar(value=item_values[2] if len(item_values) > 2 else "")
        ttk.Entry(dialog, textvariable=theme_var, width=30).grid(row=2, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="描述:").grid(row=3, column=0, sticky=tk.NW, padx=10, pady=10)
        desc_var = tk.StringVar(value=item_values[3] if len(item_values) > 3 else "")
        ttk.Entry(dialog, textvariable=desc_var, width=30).grid(row=3, column=1, padx=10, pady=10)
        
        # 保存按钮回调函数
        def save_edited_project():
            new_name = name_var.get().strip()
            if not new_name:
                messagebox.showerror("错误", "项目名称不能为空！")
                return
                
            # 检查项目名称是否已被其他项目使用
            projects = self.config.get('projects', {})
            if new_name != old_name and new_name in projects:
                messagebox.showerror("错误", f"项目 '{new_name}' 已存在！")
                return
                
            # 更新配置
            # 如果项目名称改变，需要删除旧项目并添加新项目
            if new_name != old_name:
                # 复制旧项目数据
                project_data = projects[old_name].copy()
                # 删除旧项目
                del projects[old_name]
                # 添加新项目
                projects[new_name] = project_data
            
            # 更新项目数据
            projects[new_name] = {
                'prefix': prefix_var.get().strip(),
                'theme': theme_var.get().strip(),
                'description': desc_var.get().strip()
            }
            
            # 更新Treeview显示
            self.project_tree.item(selected_item, values=(new_name, prefix_var.get(), theme_var.get(), desc_var.get()))
            
            # 关闭对话框
            dialog.destroy()
            messagebox.showinfo("成功", f"项目 '{new_name}' 已更新！")
        
        # 创建按钮
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=4, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="保存", command=save_edited_project).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 调整窗口布局
        dialog.grid_columnconfigure(1, weight=1)
    
    def delete_project(self):
        """删除项目"""
        # 获取选中的项目
        selected_items = self.project_tree.selection()
        if not selected_items:
            messagebox.showwarning("警告", "请先选择要删除的项目！")
            return
        
        selected_item = selected_items[0]  # 只处理第一个选中的项目
        item_values = self.project_tree.item(selected_item, "values")
        project_name = item_values[0]  # 获取项目名称
        
        # 确认删除
        confirm = messagebox.askyesno("确认删除", f"确定要删除项目 '{project_name}' 吗？")
        if not confirm:
            return
        
        # 从配置中删除项目
        projects = self.config.get('projects', {})
        if project_name in projects:
            del projects[project_name]
        
        # 从Treeview中删除项目
        self.project_tree.delete(selected_item)
        
        messagebox.showinfo("成功", f"项目 '{project_name}' 已删除！")
    
    def add_ref_pattern(self):
        """添加引用模式"""
        if not hasattr(self, 'ref_pattern_vars'):
            self.ref_pattern_vars = []
        
        # 创建新的引用模式变量和输入框
        new_var = tk.StringVar()
        self.ref_pattern_vars.append(new_var)
        
        # 获取引用模式框架
        for child in self.regex_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "引用模式":
                ref_frame = child
                break
        else:
            return  # 如果找不到引用模式框架，直接返回
        
        # 创建新的输入行
        row_count = len(ref_frame.grid_slaves()) // 2  # 每行有两个控件
        
        ttk.Label(ref_frame, text=f"模式 {row_count + 1}:", width=10).grid(row=row_count, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(ref_frame, textvariable=new_var, width=50).grid(row=row_count, column=1, padx=5, pady=5)
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def remove_last_ref_pattern(self):
        """删除最后一条引用模式"""
        if not hasattr(self, 'ref_pattern_vars') or len(self.ref_pattern_vars) == 0:
            messagebox.showinfo("提示", "没有可删除的引用模式！")
            return
        
        # 移除最后一个变量
        self.ref_pattern_vars.pop()
        
        # 获取引用模式框架
        for child in self.regex_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "引用模式":
                ref_frame = child
                break
        else:
            return
        
        # 获取所有控件并移除最后两个（标签和输入框）
        widgets = ref_frame.grid_slaves()
        if len(widgets) >= 2:
            widgets[-1].destroy()  # 输入框
            widgets[-2].destroy()  # 标签
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def add_cleanup_pattern(self):
        """添加清理模式"""
        if not hasattr(self, 'cleanup_pattern_vars'):
            self.cleanup_pattern_vars = []
        
        # 创建新的清理模式变量和输入框
        new_var = tk.StringVar()
        self.cleanup_pattern_vars.append(new_var)
        
        # 获取清理模式框架
        for child in self.regex_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "清理模式":
                cleanup_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(cleanup_frame.grid_slaves()) // 2
        
        ttk.Label(cleanup_frame, text=f"模式 {row_count + 1}:", width=10).grid(row=row_count, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(cleanup_frame, textvariable=new_var, width=50).grid(row=row_count, column=1, padx=5, pady=5)
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def remove_last_cleanup_pattern(self):
        """删除最后一条清理模式"""
        if not hasattr(self, 'cleanup_pattern_vars') or len(self.cleanup_pattern_vars) == 0:
            messagebox.showinfo("提示", "没有可删除的清理模式！")
            return
        
        # 移除最后一个变量
        self.cleanup_pattern_vars.pop()
        
        # 获取清理模式框架
        for child in self.regex_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "清理模式":
                cleanup_frame = child
                break
        else:
            return
        
        # 获取所有控件并移除最后两个
        widgets = cleanup_frame.grid_slaves()
        if len(widgets) >= 2:
            widgets[-1].destroy()
            widgets[-2].destroy()
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def add_exclude_pattern(self):
        """添加排除模式"""
        if not hasattr(self, 'exclude_pattern_vars'):
            self.exclude_pattern_vars = []
        
        # 创建新的排除模式变量和输入框
        new_var = tk.StringVar()
        self.exclude_pattern_vars.append(new_var)
        
        # 获取排除模式框架
        for child in self.filters_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "排除模式":
                exclude_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(exclude_frame.grid_slaves()) // 2
        
        ttk.Label(exclude_frame, text=f"模式 {row_count + 1}:", width=10).grid(row=row_count, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(exclude_frame, textvariable=new_var, width=50).grid(row=row_count, column=1, padx=5, pady=5)
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def remove_last_exclude_pattern(self):
        """删除最后一条排除模式"""
        if not hasattr(self, 'exclude_pattern_vars') or len(self.exclude_pattern_vars) == 0:
            messagebox.showinfo("提示", "没有可删除的排除模式！")
            return
        
        # 移除最后一个变量
        self.exclude_pattern_vars.pop()
        
        # 获取排除模式框架
        for child in self.filters_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "排除模式":
                exclude_frame = child
                break
        else:
            return
        
        # 获取所有控件并移除最后两个
        widgets = exclude_frame.grid_slaves()
        if len(widgets) >= 2:
            widgets[-1].destroy()
            widgets[-2].destroy()
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def add_include_pattern(self):
        """添加包含模式"""
        if not hasattr(self, 'include_pattern_vars'):
            self.include_pattern_vars = []
        
        # 创建新的包含模式变量和输入框
        new_var = tk.StringVar()
        self.include_pattern_vars.append(new_var)
        
        # 获取包含模式框架
        for child in self.filters_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "包含模式":
                include_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(include_frame.grid_slaves()) // 2
        
        ttk.Label(include_frame, text=f"模式 {row_count + 1}:", width=10).grid(row=row_count, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(include_frame, textvariable=new_var, width=50).grid(row=row_count, column=1, padx=5, pady=5)
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def remove_last_include_pattern(self):
        """删除最后一条包含模式"""
        if not hasattr(self, 'include_pattern_vars') or len(self.include_pattern_vars) == 0:
            messagebox.showinfo("提示", "没有可删除的包含模式！")
            return
        
        # 移除最后一个变量
        self.include_pattern_vars.pop()
        
        # 获取包含模式框架
        for child in self.filters_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "包含模式":
                include_frame = child
                break
        else:
            return
        
        # 获取所有控件并移除最后两个
        widgets = include_frame.grid_slaves()
        if len(widgets) >= 2:
            widgets[-1].destroy()
            widgets[-2].destroy()
        
        # 更新滚动区域
        self.scrollable_frame.update_idletasks()
    
    def add_column(self, tree, column_type):
        """添加列"""
        # 创建添加列的对话框
        dialog = tk.Toplevel(self.root)
        dialog.title(f"添加{column_type}列")
        dialog.geometry("350x200")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 创建表单控件
        ttk.Label(dialog, text="列名:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        name_var = tk.StringVar()
        ttk.Entry(dialog, textvariable=name_var, width=25).grid(row=0, column=1, padx=10, pady=10)
        
        ttk.Label(dialog, text="宽度:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=10)
        width_var = tk.IntVar(value=100)
        ttk.Spinbox(dialog, from_=50, to=500, textvariable=width_var, width=10).grid(row=1, column=1, padx=10, pady=10)
        
        # 保存按钮回调函数
        def save_new_column():
            name = name_var.get().strip()
            if not name:
                messagebox.showerror("错误", "列名不能为空！")
                return
                
            # 检查列是否已存在
            columns = tree['columns']
            if name in columns:
                messagebox.showerror("错误", f"列 '{name}' 已存在！")
                return
                
            # 更新Treeview
            tree['columns'] = list(columns) + [name]
            tree.heading(name, text=name)
            tree.column(name, width=width_var.get())
            
            # 更新配置中的列信息
            if column_type == "结果":
                self.result_columns.append(name)
            elif column_type == "依赖":
                self.dependency_columns.append(name)
            
            # 关闭对话框
            dialog.destroy()
            messagebox.showinfo("成功", f"{column_type}列 '{name}' 添加成功！")
        
        # 创建按钮
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="保存", command=save_new_column).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 调整窗口布局
        dialog.grid_columnconfigure(1, weight=1)
    
    def edit_column(self, tree, column_type):
        """编辑列"""
        # 获取所有列
        columns = list(tree['columns'])
        if not columns:
            messagebox.showwarning("警告", "没有可编辑的列！")
            return
        
        # 创建编辑列的对话框
        dialog = tk.Toplevel(self.root)
        dialog.title(f"编辑{column_type}列")
        dialog.geometry("350x200")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 创建列选择下拉框
        ttk.Label(dialog, text="选择列:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        column_var = tk.StringVar(value=columns[0])
        column_combo = ttk.Combobox(dialog, textvariable=column_var, values=columns, state="readonly", width=22)
        column_combo.grid(row=0, column=1, padx=10, pady=10)
        
        # 创建宽度输入框
        ttk.Label(dialog, text="宽度:").grid(row=1, column=0, sticky=tk.W, padx=10, pady=10)
        width_var = tk.IntVar(value=100)
        ttk.Spinbox(dialog, from_=50, to=500, textvariable=width_var, width=10).grid(row=1, column=1, padx=10, pady=10)
        
        # 根据选中的列设置宽度
        def on_column_select(event):
            selected_col = column_var.get()
            width = tree.column(selected_col, "width")
            width_var.set(width)
        
        column_combo.bind("<<ComboboxSelected>>", on_column_select)
        on_column_select(None)  # 设置初始值
        
        # 保存按钮回调函数
        def save_edited_column():
            column_name = column_var.get()
            new_width = width_var.get()
            
            # 更新列宽度
            tree.column(column_name, width=new_width)
            
            # 关闭对话框
            dialog.destroy()
            messagebox.showinfo("成功", f"{column_type}列 '{column_name}' 已更新！")
        
        # 创建按钮
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="保存", command=save_edited_column).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 调整窗口布局
        dialog.grid_columnconfigure(1, weight=1)
    
    def delete_column(self, tree):
        """删除列"""
        # 获取所有列
        columns = list(tree['columns'])
        if not columns:
            messagebox.showwarning("警告", "没有可删除的列！")
            return
        
        # 创建删除列的对话框
        dialog = tk.Toplevel(self.root)
        dialog.title("删除列")
        dialog.geometry("350x180")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # 创建列选择下拉框
        ttk.Label(dialog, text="选择要删除的列:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        column_var = tk.StringVar(value=columns[0])
        column_combo = ttk.Combobox(dialog, textvariable=column_var, values=columns, state="readonly", width=22)
        column_combo.grid(row=0, column=1, padx=10, pady=10)
        
        # 删除按钮回调函数
        def delete_selected_column():
            column_name = column_var.get()
            
            # 确认删除
            confirm = messagebox.askyesno("确认删除", f"确定要删除列 '{column_name}' 吗？")
            if not confirm:
                return
            
            # 从Treeview中删除列
            new_columns = [col for col in columns if col != column_name]
            tree['columns'] = new_columns
            
            # 删除列的标题
            # 注意：Tkinter的Treeview不直接支持删除列，这里我们通过重新配置列来实现
            # 实际应用中可能需要更复杂的处理
            
            # 关闭对话框
            dialog.destroy()
            messagebox.showinfo("成功", f"列 '{column_name}' 已删除！")
        
        # 创建按钮
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=1, column=0, columnspan=2, pady=20)
        
        ttk.Button(btn_frame, text="删除", command=delete_selected_column).pack(side=tk.LEFT, padx=10)
        ttk.Button(btn_frame, text="取消", command=dialog.destroy).pack(side=tk.LEFT, padx=10)
        
        # 调整窗口布局
        dialog.grid_columnconfigure(1, weight=1)
    
    def save_config(self):
        """保存配置到文件"""
        try:
            # 收集所有表单数据
            new_config = {}
            
            # 添加处理配置
            if hasattr(self, 'remove_suffix_var'):
                new_config['processing'] = {
                    'remove_suffix': self.remove_suffix_var.get(),
                    'suffix_identifier': self.suffix_identifier_var.get() if hasattr(self, 'suffix_identifier_var') else '',
                    'filter_schema': self.filter_schema_var.get() if hasattr(self, 'filter_schema_var') else ''
                }
            
            # 添加项目配置（从UI获取最新数据）
            if hasattr(self, 'project_tree'):
                projects_data = {}
                for item in self.project_tree.get_children():
                    values = self.project_tree.item(item, 'values')
                    if values:
                        project_name = values[0]
                        projects_data[project_name] = {
                            'prefix': values[1] if len(values) > 1 else '',
                            'theme': values[2] if len(values) > 2 else '',
                            'description': values[3] if len(values) > 3 else ''
                        }
                new_config['projects'] = projects_data
            
            # 添加文件模板配置
            if hasattr(self, 'template_vars'):
                new_config['file_templates'] = {}
                for template_key, template_data in self.template_vars.items():
                    new_config['file_templates'][template_key] = {
                        'name': template_data['name'].get(),
                        'lines': {
                            'table_name': int(template_data['table_line'].get()) if template_data['table_line'].get().isdigit() else 8,
                            'developer': int(template_data['dev_line'].get()) if template_data['dev_line'].get().isdigit() else 14
                        },
                        'delimiter': template_data['delimiter'].get(),
                        'file_pattern': template_data['pattern'].get()
                    }
            
            # 添加正则表达式配置
            if hasattr(self, 'ref_regex_vars'):
                new_config['regex_patterns'] = {
                    'table_reference': [var.get() for var in self.ref_regex_vars if var.get().strip()],
                    'table_cleanup': [var.get() for var in self.cleanup_vars if hasattr(self, 'cleanup_vars') and var.get().strip()],
                    'file_extension': self.file_ext_var.get() if hasattr(self, 'file_ext_var') else '\\.(hql|sql)$'
                }
            
            # 添加过滤器配置
            new_config['filters'] = {
                'exclude_self_reference': self.exclude_self_var.get() if hasattr(self, 'exclude_self_var') else True,
                'exclude_same_layer': self.exclude_same_layer_var.get() if hasattr(self, 'exclude_same_layer_var') else True,
                'exclude_patterns': [var.get() for var in self.exclude_vars if hasattr(self, 'exclude_vars') and var.get().strip()],
                'include_patterns': [var.get() for var in self.include_vars if hasattr(self, 'include_vars') and var.get().strip()]
            }
            
            # 添加输出配置（从UI获取最新数据）
            new_config['output'] = {
                'basic_columns': [],
                'extended_columns': []
            }
            
            # 保存输出配置的列信息（从UI界面）
            if hasattr(self, 'notebook'):
                # 查找基本列和扩展列的TreeView
                for child in self.notebook.winfo_children():
                    tab_text = self.notebook.tab(child, 'text')
                    if tab_text == '输出配置':
                        # 找到输出配置选项卡中的notebook
                        for sub_child in child.winfo_children():
                            if isinstance(sub_child, ttk.Notebook):
                                output_notebook = sub_child
                                # 遍历基本列和扩展列选项卡
                                for i, tab_name in enumerate(['基础列', '扩展列']):
                                    # 查找对应的选项卡
                                    for sub_tab in output_notebook.winfo_children():
                                        if output_notebook.tab(sub_tab, 'text') == tab_name:
                                            # 查找TreeView
                                            for widget in sub_tab.winfo_children():
                                                if isinstance(widget, ttk.Frame):
                                                    for tree_widget in widget.winfo_children():
                                                        if isinstance(tree_widget, ttk.Treeview):
                                                            # 收集列数据
                                                            column_type = 'basic_columns' if i == 0 else 'extended_columns'
                                                            for item in tree_widget.get_children():
                                                                values = tree_widget.item(item, 'values')
                                                                if values:
                                                                    column_data = {
                                                                        'name': values[0],
                                                                        'title': values[1] if len(values) > 1 else '',
                                                                        'width': int(values[2]) if len(values) > 2 and values[2].isdigit() else 100,
                                                                        'hidden': values[3] == '是' if len(values) > 3 else False
                                                                    }
                                                                    new_config['output'][column_type].append(column_data)
            
            # 添加进度条配置
            if hasattr(self, 'bar_length_var'):
                new_config['progress'] = {
                    'bar_length': self.bar_length_var.get(),
                    'show_percentage': self.show_percentage_var.get() if hasattr(self, 'show_percentage_var') else True,
                    'update_frequency': self.update_freq_var.get() if hasattr(self, 'update_freq_var') else 1
                }
            
            # 保存到文件
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(new_config, f, default_flow_style=False, allow_unicode=True, indent=2)
            messagebox.showinfo("成功", "配置文件保存成功！")
        except Exception as e:
            messagebox.showerror("错误", f"保存配置文件时出错: {str(e)}")

    
    def load_config_file(self):
        """从文件加载配置"""
        file_path = filedialog.askopenfilename(
            title="选择配置文件",
            filetypes=[("YAML文件", "*.yaml;*.yml"), ("所有文件", "*.*")]
        )
        if file_path:
            self.config_path = file_path
            self.config = self.load_config()
            self.reset_form()
    
    def reset_form(self):
        """重置表单"""
        # 销毁并重新创建界面
        for widget in self.main_frame.winfo_children():
            widget.destroy()
        self.__init__(self.root, self.config_path)

if __name__ == "__main__":
    root = tk.Tk()
    app = ConfigFormApp(root)
    root.mainloop()