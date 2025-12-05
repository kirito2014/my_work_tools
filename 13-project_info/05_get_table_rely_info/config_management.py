import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import yaml
import os
from tkinter.font import Font

# 尝试导入ttkthemes
try:
    from ttkthemes import ThemedTk
    USE_THEMED_TK = True
except ImportError:
    USE_THEMED_TK = False

class ConfigFormApp:
    def __init__(self, root, config_path="config.yaml"):
        self.root = root
        self.config_path = config_path
        
        # 统一的字体设置 - 减小字体大小
        self.default_font = Font(family="微软雅黑", size=9)
        self.label_font = Font(family="微软雅黑", size=9)
        self.title_font = Font(family="微软雅黑", size=9, weight="bold")
        
        # 设置全局字体
        root.option_add("*Font", self.default_font)
        
        # 为ttk组件单独设置样式
        style = ttk.Style()
        style.configure(".", font=("微软雅黑", 9))
        
        # 配置标签页的字体和间距
        style.configure("TNotebook.Tab", font=("微软雅黑", 9), padding=[8, 2])
        style.configure("TLabelFrame", padding=5)
        
        self.config = self.load_config()
        
        self.root.title("SQL依赖关系分析工具 - 配置编辑器")
        self.root.geometry("900x700")  # 适当减小窗口尺寸
        
        # 创建主框架 - 减小内边距
        self.main_frame = ttk.Frame(root, padding="5")
        self.main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 创建主容器框架
        self.container_frame = ttk.Frame(self.main_frame)
        self.container_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=3, pady=3)
        
        # 创建标签页
        self.notebook = ttk.Notebook(self.container_frame)
        self.notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=3, pady=3)
        
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
        
        # 配置权重
        self.container_frame.columnconfigure(0, weight=1)
        self.container_frame.rowconfigure(0, weight=1)
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
    
    def create_processing_tab(self):
        """创建处理配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="处理配置")
        
        frame = ttk.LabelFrame(tab, text="SQL文件处理配置", padding="8")
        frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 减小行间距和内边距
        row_padding = 2
        col_padding = 3
        
        # 去除后缀配置
        ttk.Label(frame, text="是否去除后缀:", font=self.label_font).grid(
            row=0, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        self.remove_suffix_var = tk.StringVar(value=self.config.get('processing', {}).get('remove_suffix', 'Y'))
        ttk.Combobox(frame, textvariable=self.remove_suffix_var, values=['Y', 'N'], 
                     width=5, state="readonly", font=self.default_font).grid(
            row=0, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        
        ttk.Label(frame, text="后缀标识符:", font=self.label_font).grid(
            row=1, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        self.suffix_identifier_var = tk.StringVar(value=self.config.get('processing', {}).get('suffix_identifier', '_PC'))
        ttk.Entry(frame, textvariable=self.suffix_identifier_var, width=20, font=self.default_font).grid(
            row=1, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        
        ttk.Label(frame, text="筛选模式:", font=self.label_font).grid(
            row=2, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        self.filter_schema_var = tk.StringVar(value=self.config.get('processing', {}).get('filter_schema', 'AGL'))
        ttk.Entry(frame, textvariable=self.filter_schema_var, width=20, font=self.default_font).grid(
            row=2, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
        )
        
        # 配置列权重
        frame.columnconfigure(0, weight=0)
        frame.columnconfigure(1, weight=1)
        for i in range(3):
            frame.rowconfigure(i, weight=0)
    
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
    
    def create_projects_tab(self):
        """创建项目配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="项目配置")
        
        # 配置标签页的网格权重
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        tab.rowconfigure(1, weight=0)
        
        # 创建Treeview显示项目 - 减小高度
        columns = ("项目名称", "前缀", "主题", "描述")
        self.project_tree = ttk.Treeview(
            tab, 
            columns=columns, 
            show="headings", 
            height=8,
            style="Custom.Treeview"
        )
        
        # 配置列宽和标题
        col_widths = [100, 80, 100, 150]
        for i, col in enumerate(columns):
            self.project_tree.heading(col, text=col, anchor="w")
            self.project_tree.column(col, width=col_widths[i], anchor="w")
        
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
        tree_scrollbar = ttk.Scrollbar(tab, orient="vertical", command=self.project_tree.yview)
        self.project_tree.configure(yscrollcommand=tree_scrollbar.set)
        
        # 布局Treeview和滚动条 - 减小边距
        self.project_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        tree_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S), pady=5)
        
        # 编辑按钮
        btn_frame = ttk.Frame(tab)
        btn_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), padx=5, pady=3)
        
        ttk.Button(btn_frame, text="添加项目", command=self.add_project).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="编辑项目", command=self.edit_project).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="删除项目", command=self.delete_project).pack(side=tk.LEFT, padx=3)
        
        # 配置权重
        tab.columnconfigure(0, weight=1)
    
    def create_templates_tab(self):
        """创建文件模板配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="文件模板")
        
        # 配置标签页的网格权重
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        
        notebook = ttk.Notebook(tab)
        notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=3, pady=3)
        
        templates = self.config.get('file_templates', {})
        self.template_vars = {}
        
        for template_key, template_data in templates.items():
            template_tab = ttk.Frame(notebook)
            notebook.add(template_tab, text=template_data.get('name', template_key))
            
            # 配置子标签页网格
            template_tab.columnconfigure(0, weight=1)
            template_tab.rowconfigure(0, weight=1)
            
            # 创建Canvas和滚动条
            canvas = tk.Canvas(template_tab, highlightthickness=0)
            scrollbar = ttk.Scrollbar(template_tab, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)
            
            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
            )
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            frame = ttk.LabelFrame(scrollable_frame, text=f"{template_data.get('name')} 配置", padding="8")
            frame.pack(fill="both", expand=True, padx=5, pady=5)
            
            row = 0
            row_padding = 2
            col_padding = 3
            
            # 模板名称
            ttk.Label(frame, text="模板名称:", font=self.label_font).grid(
                row=row, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            name_var = tk.StringVar(value=template_data.get('name', ''))
            ttk.Entry(frame, textvariable=name_var, width=30, font=self.default_font).grid(
                row=row, column=1, sticky=(tk.W, tk.E), pady=row_padding, padx=col_padding
            )
            row += 1
            
            # 表名行号
            ttk.Label(frame, text="表名所在行号:", font=self.label_font).grid(
                row=row, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            table_line_var = tk.StringVar(value=str(template_data.get('lines', {}).get('table_name', 8)))
            ttk.Entry(frame, textvariable=table_line_var, width=10, font=self.default_font).grid(
                row=row, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            row += 1
            
            # 开发人员行号
            ttk.Label(frame, text="开发人员行号:", font=self.label_font).grid(
                row=row, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            dev_line_var = tk.StringVar(value=str(template_data.get('lines', {}).get('developer', 14)))
            ttk.Entry(frame, textvariable=dev_line_var, width=10, font=self.default_font).grid(
                row=row, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            row += 1
            
            # 分隔符
            ttk.Label(frame, text="分隔符:", font=self.label_font).grid(
                row=row, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            delimiter_var = tk.StringVar(value=template_data.get('delimiter', ':'))
            ttk.Entry(frame, textvariable=delimiter_var, width=10, font=self.default_font).grid(
                row=row, column=1, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            row += 1
            
            # 文件匹配模式
            ttk.Label(frame, text="文件匹配模式:", font=self.label_font).grid(
                row=row, column=0, sticky=tk.W, pady=row_padding, padx=col_padding
            )
            pattern_var = tk.StringVar(value=template_data.get('file_pattern', ''))
            ttk.Entry(frame, textvariable=pattern_var, width=50, font=self.default_font).grid(
                row=row, column=1, sticky=(tk.W, tk.E), pady=row_padding, padx=col_padding
            )
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
            frame.columnconfigure(0, weight=0)
            frame.columnconfigure(1, weight=1)
            
            # 布局Canvas和滚动条
            canvas.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
            
            template_tab.columnconfigure(0, weight=1)
            template_tab.rowconfigure(0, weight=1)
        
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
    
    def create_regex_tab(self):
        """创建正则表达式配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="正则表达式")
        
        # 配置标签页的网格权重
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        
        # 创建Canvas和滚动条
        canvas = tk.Canvas(tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # 表引用正则
        ref_frame = ttk.LabelFrame(scrollable_frame, text="表引用正则表达式", padding="8")
        ref_frame.pack(fill="x", padx=5, pady=3)
        
        self.ref_regex_vars = []
        ref_patterns = self.config.get('regex_patterns', {}).get('table_reference', [])
        
        ttk.Label(ref_frame, text="表引用匹配模式:", font=self.label_font).grid(
            row=0, column=0, sticky=tk.W, pady=2, padx=3
        )
        
        for i, pattern in enumerate(ref_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(ref_frame, textvariable=var, width=80, font=self.default_font).grid(
                row=i+1, column=0, sticky=(tk.W, tk.E), pady=1, padx=3
            )
            self.ref_regex_vars.append(var)
        
        # 添加/删除按钮
        ref_btn_frame = ttk.Frame(ref_frame)
        ref_btn_frame.grid(row=len(ref_patterns)+1, column=0, sticky=tk.W, pady=3)
        ttk.Button(ref_btn_frame, text="添加模式", command=self.add_ref_pattern).pack(side=tk.LEFT, padx=2)
        ttk.Button(ref_btn_frame, text="删除最后一条", command=self.remove_last_ref_pattern).pack(side=tk.LEFT, padx=2)
        
        # 清理模式
        cleanup_frame = ttk.LabelFrame(scrollable_frame, text="表名清理模式", padding="8")
        cleanup_frame.pack(fill="x", padx=5, pady=3)
        
        self.cleanup_vars = []
        cleanup_patterns = self.config.get('regex_patterns', {}).get('table_cleanup', [])
        
        ttk.Label(cleanup_frame, text="表名清理后缀模式:", font=self.label_font).grid(
            row=0, column=0, sticky=tk.W, pady=2, padx=3
        )
        
        for i, pattern in enumerate(cleanup_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(cleanup_frame, textvariable=var, width=30, font=self.default_font).grid(
                row=i+1, column=0, sticky=tk.W, pady=1, padx=3
            )
            self.cleanup_vars.append(var)
        
        cleanup_btn_frame = ttk.Frame(cleanup_frame)
        cleanup_btn_frame.grid(row=len(cleanup_patterns)+1, column=0, sticky=tk.W, pady=3)
        ttk.Button(cleanup_btn_frame, text="添加清理模式", command=self.add_cleanup_pattern).pack(side=tk.LEFT, padx=2)
        ttk.Button(cleanup_btn_frame, text="删除最后一条", command=self.remove_last_cleanup_pattern).pack(side=tk.LEFT, padx=2)
        
        # 文件扩展名
        ext_frame = ttk.LabelFrame(scrollable_frame, text="文件扩展名", padding="8")
        ext_frame.pack(fill="x", padx=5, pady=3)
        
        ttk.Label(ext_frame, text="SQL文件扩展名正则:", font=self.label_font).grid(
            row=0, column=0, sticky=tk.W, pady=2, padx=3
        )
        self.file_ext_var = tk.StringVar(value=self.config.get('regex_patterns', {}).get('file_extension', '\\.(hql|sql)$'))
        ttk.Entry(ext_frame, textvariable=self.file_ext_var, width=30, font=self.default_font).grid(
            row=0, column=1, sticky=tk.W, pady=2, padx=3
        )
        
        # 配置权重
        ref_frame.columnconfigure(0, weight=1)
        cleanup_frame.columnconfigure(0, weight=1)
        ext_frame.columnconfigure(0, weight=0)
        ext_frame.columnconfigure(1, weight=1)
        
        # 布局Canvas和滚动条
        canvas.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
    
    def create_filters_tab(self):
        """创建过滤规则配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="过滤规则")
        
        # 基础过滤选项
        basic_frame = ttk.LabelFrame(tab, text="基础过滤选项", padding="8")
        basic_frame.pack(fill="x", padx=5, pady=3)
        
        self.exclude_self_var = tk.BooleanVar(value=self.config.get('filters', {}).get('exclude_self_reference', True))
        ttk.Checkbutton(basic_frame, text="排除自引用", variable=self.exclude_self_var).grid(row=0, column=0, sticky=tk.W, pady=3)
        
        self.exclude_same_layer_var = tk.BooleanVar(value=self.config.get('filters', {}).get('exclude_same_layer', True))
        ttk.Checkbutton(basic_frame, text="排除同层依赖", variable=self.exclude_same_layer_var).grid(row=1, column=0, sticky=tk.W, pady=3)
        
        # 排除模式
        exclude_frame = ttk.LabelFrame(tab, text="排除模式（正则表达式）", padding="8")
        exclude_frame.pack(fill="both", expand=True, padx=5, pady=3)
        
        self.exclude_vars = []
        exclude_patterns = self.config.get('filters', {}).get('exclude_patterns', [])
        
        ttk.Label(exclude_frame, text="排除的表名前缀模式:").grid(row=0, column=0, sticky=tk.W, pady=3)
        
        for i, pattern in enumerate(exclude_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(exclude_frame, textvariable=var, width=30).grid(row=i+1, column=0, sticky=tk.W, pady=1)
            self.exclude_vars.append(var)
        
        exclude_btn_frame = ttk.Frame(exclude_frame)
        exclude_btn_frame.grid(row=len(exclude_patterns)+1, column=0, sticky=tk.W, pady=5)
        ttk.Button(exclude_btn_frame, text="添加排除模式", command=self.add_exclude_pattern).pack(side=tk.LEFT, padx=3)
        ttk.Button(exclude_btn_frame, text="删除最后一条", command=self.remove_last_exclude_pattern).pack(side=tk.LEFT, padx=3)
        
        # 包含模式
        include_frame = ttk.LabelFrame(tab, text="包含模式（正则表达式）", padding="8")
        include_frame.pack(fill="both", expand=True, padx=5, pady=3)
        
        self.include_vars = []
        include_patterns = self.config.get('filters', {}).get('include_patterns', [])
        
        ttk.Label(include_frame, text="包含的表名模式（留空表示全部）:").grid(row=0, column=0, sticky=tk.W, pady=3)
        
        for i, pattern in enumerate(include_patterns):
            var = tk.StringVar(value=pattern)
            ttk.Entry(include_frame, textvariable=var, width=30).grid(row=i+1, column=0, sticky=tk.W, pady=1)
            self.include_vars.append(var)
        
        include_btn_frame = ttk.Frame(include_frame)
        include_btn_frame.grid(row=len(include_patterns)+1, column=0, sticky=tk.W, pady=5)
        ttk.Button(include_btn_frame, text="添加包含模式", command=self.add_include_pattern).pack(side=tk.LEFT, padx=3)
        ttk.Button(include_btn_frame, text="删除最后一条", command=self.remove_last_include_pattern).pack(side=tk.LEFT, padx=3)
    
    def create_output_tab(self):
        """创建输出配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="输出配置")
        
        notebook = ttk.Notebook(tab)
        notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
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
        tree_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        columns_def = ["列名", "显示标题", "宽度", "隐藏"]
        tree = ttk.Treeview(tree_frame, columns=columns_def, show="headings", height=8)
        
        for col in columns_def:
            tree.heading(col, text=col)
            tree.column(col, width=80)
        
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
        btn_frame.pack(fill="x", padx=5, pady=3)
        
        ttk.Button(btn_frame, text="添加列", 
                  command=lambda: self.add_column(tree, column_type)).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="编辑列", 
                  command=lambda: self.edit_column(tree, column_type)).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="删除列", 
                  command=lambda: self.delete_column(tree)).pack(side=tk.LEFT, padx=3)
    
    def create_progress_tab(self):
        """创建进度条配置页"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="进度条配置")
        
        frame = ttk.LabelFrame(tab, text="进度条显示配置", padding="8")
        frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        progress_config = self.config.get('progress', {})
        
        ttk.Label(frame, text="进度条长度:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.bar_length_var = tk.IntVar(value=progress_config.get('bar_length', 30))
        ttk.Spinbox(frame, from_=10, to=100, textvariable=self.bar_length_var, width=10).grid(row=0, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(frame, text="显示百分比:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.show_percentage_var = tk.BooleanVar(value=progress_config.get('show_percentage', True))
        ttk.Checkbutton(frame, text="是", variable=self.show_percentage_var).grid(row=1, column=1, sticky=tk.W, pady=5)
        
        ttk.Label(frame, text="更新频率:", font=self.label_font).grid(row=2, column=0, sticky=tk.W, pady=5)
        self.update_freq_var = tk.IntVar(value=progress_config.get('update_frequency', 1))
        ttk.Spinbox(frame, from_=1, to=100, textvariable=self.update_freq_var, width=10).grid(row=2, column=1, sticky=tk.W, pady=5)
    
    def create_control_buttons(self):
        """创建控制按钮"""
        btn_frame = ttk.Frame(self.container_frame)
        btn_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=5)
        
        # 减小按钮宽度和间距
        ttk.Button(btn_frame, text="保存配置", command=self.save_config, width=12).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="加载配置", command=self.load_config_file, width=12).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="重置表单", command=self.reset_form, width=12).pack(side=tk.LEFT, padx=3)
        ttk.Button(btn_frame, text="退出", command=self.root.quit, width=12).pack(side=tk.LEFT, padx=3)
        
        btn_frame.columnconfigure(0, weight=1)
    
    # 以下是辅助方法（保持不变）
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
        # 修复：使用正确的框架名称和变量名
        if not hasattr(self, 'ref_regex_vars'):
            self.ref_regex_vars = []
        
        # 创建新的引用模式变量
        new_var = tk.StringVar()
        self.ref_regex_vars.append(new_var)
        
        # 获取正则表达式标签页中的Canvas和滚动框架
        for child in self.notebook.winfo_children():
            if self.notebook.tab(child, "text") == "正则表达式":
                regex_tab = child
                break
        else:
            return
        
        # 获取Canvas中的滚动框架
        canvas = regex_tab.winfo_children()[0]
        scrollable_frame = canvas.winfo_children()[0]
        
        # 获取表引用正则表达式框架
        for child in scrollable_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "表引用正则表达式":
                ref_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(ref_frame.grid_slaves()) // 2  # 每行有两个控件
        
        ttk.Entry(ref_frame, textvariable=new_var, width=80, font=self.default_font).grid(
            row=row_count, column=0, sticky=(tk.W, tk.E), pady=1, padx=3
        )
        
        # 更新滚动区域
        scrollable_frame.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox("all"))
    
    def remove_last_ref_pattern(self):
        """删除最后一条引用模式"""
        if hasattr(self, 'ref_regex_vars') and len(self.ref_regex_vars) > 0:
            # 移除最后一个变量
            self.ref_regex_vars.pop()
            
            # 获取正则表达式标签页中的Canvas和滚动框架
            for child in self.notebook.winfo_children():
                if self.notebook.tab(child, "text") == "正则表达式":
                    regex_tab = child
                    break
            else:
                return
            
            # 获取Canvas中的滚动框架
            canvas = regex_tab.winfo_children()[0]
            scrollable_frame = canvas.winfo_children()[0]
            
            # 获取表引用正则表达式框架
            for child in scrollable_frame.winfo_children():
                if isinstance(child, ttk.LabelFrame) and child['text'] == "表引用正则表达式":
                    ref_frame = child
                    break
            else:
                return
            
            # 获取所有输入框
            entries = [widget for widget in ref_frame.winfo_children() if isinstance(widget, ttk.Entry)]
            if entries:
                # 删除最后一个输入框
                entries[-1].destroy()
            
            # 更新滚动区域
            scrollable_frame.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))
    
    # 添加缺失的方法
    def save_config(self):
        """保存配置到文件"""
        try:
            # 先将表单数据同步到self.config
            self.update_processing_config()
            self.update_projects_config()
            self.update_templates_config()
            self.update_regex_config()
            self.update_filters_config()
            self.update_output_config()
            self.update_progress_config()
            
            # 然后保存到文件
            with open(self.config_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, allow_unicode=True)
            messagebox.showinfo("成功", "配置已保存！")
        except Exception as e:
            messagebox.showerror("错误", f"保存配置失败: {e}")
    
    def update_processing_config(self):
        """更新处理配置"""
        if 'processing' not in self.config:
            self.config['processing'] = {}
        
        self.config['processing']['remove_suffix'] = self.remove_suffix_var.get()
        self.config['processing']['suffix_identifier'] = self.suffix_identifier_var.get()
        self.config['processing']['filter_schema'] = self.filter_schema_var.get()
    
    def update_projects_config(self):
        """更新项目配置 - 实际上项目配置在添加/编辑/删除时已经直接更新了self.config"""
        # 项目配置在add_project/edit_project/delete_project方法中已经直接更新了self.config
        # 所以这里不需要额外处理
        pass
    
    def update_templates_config(self):
        """更新模板配置"""
        if 'file_templates' not in self.config:
            self.config['file_templates'] = {}
        
        for template_key, vars_dict in self.template_vars.items():
            if template_key not in self.config['file_templates']:
                self.config['file_templates'][template_key] = {}
            
            self.config['file_templates'][template_key]['name'] = vars_dict['name'].get()
            self.config['file_templates'][template_key]['lines'] = {
                'table_name': int(vars_dict['table_line'].get()),
                'developer': int(vars_dict['dev_line'].get())
            }
            self.config['file_templates'][template_key]['delimiter'] = vars_dict['delimiter'].get()
            self.config['file_templates'][template_key]['file_pattern'] = vars_dict['pattern'].get()
    
    def update_regex_config(self):
        """更新正则表达式配置"""
        if 'regex_patterns' not in self.config:
            self.config['regex_patterns'] = {}
        
        # 更新表引用正则
        self.config['regex_patterns']['table_reference'] = [var.get() for var in self.ref_regex_vars]
        
        # 更新表名清理模式
        self.config['regex_patterns']['table_cleanup'] = [var.get() for var in self.cleanup_vars]
        
        # 更新文件扩展名
        self.config['regex_patterns']['file_extension'] = self.file_ext_var.get()
    
    def update_filters_config(self):
        """更新过滤规则配置"""
        if 'filters' not in self.config:
            self.config['filters'] = {}
        
        self.config['filters']['exclude_self_reference'] = self.exclude_self_var.get()
        self.config['filters']['exclude_same_layer'] = self.exclude_same_layer_var.get()
        self.config['filters']['exclude_patterns'] = [var.get() for var in self.exclude_vars]
        self.config['filters']['include_patterns'] = [var.get() for var in self.include_vars]
    
    def update_output_config(self):
        """更新输出配置 - 目前列配置功能未实现，所以暂时不处理"""
        # 列配置功能目前只是占位，实际功能未实现，所以暂时不处理
        pass
    
    def update_progress_config(self):
        """更新进度条配置"""
        if 'progress' not in self.config:
            self.config['progress'] = {}
        
        self.config['progress']['bar_length'] = self.bar_length_var.get()
        self.config['progress']['show_percentage'] = self.show_percentage_var.get()
        self.config['progress']['update_frequency'] = self.update_freq_var.get()
    
    def load_config_file(self):
        """加载配置文件"""
        file_path = filedialog.askopenfilename(
            filetypes=[("YAML files", "*.yaml"), ("All files", "*.*")],
            title="选择配置文件"
        )
        if file_path:
            self.config_path = file_path
            self.config = self.load_config()
            # 更新表单控件的值
            self.update_form_from_config()
            messagebox.showinfo("成功", "配置已加载！")
    
    def reset_form(self):
        """重置表单"""
        self.config = self.load_config()
        # 更新表单控件的值
        self.update_form_from_config()
        messagebox.showinfo("成功", "表单已重置！")
    
    def update_form_from_config(self):
        """从配置更新表单"""
        # 更新处理配置
        self.remove_suffix_var.set(self.config.get('processing', {}).get('remove_suffix', 'Y'))
        self.suffix_identifier_var.set(self.config.get('processing', {}).get('suffix_identifier', '_PC'))
        self.filter_schema_var.set(self.config.get('processing', {}).get('filter_schema', 'AGL'))
        
        # 更新项目配置 - 需要重新加载Treeview
        self.reload_project_tree()
        
        # 更新过滤规则配置
        self.exclude_self_var.set(self.config.get('filters', {}).get('exclude_self_reference', True))
        self.exclude_same_layer_var.set(self.config.get('filters', {}).get('exclude_same_layer', True))
        
        # 更新进度条配置
        self.bar_length_var.set(self.config.get('progress', {}).get('bar_length', 30))
        self.show_percentage_var.set(self.config.get('progress', {}).get('show_percentage', True))
        self.update_freq_var.set(self.config.get('progress', {}).get('update_frequency', 1))
    
    def reload_project_tree(self):
        """重新加载项目Treeview"""
        # 清空现有数据
        for item in self.project_tree.get_children():
            self.project_tree.delete(item)
        
        # 重新填充数据
        projects = self.config.get('projects', {})
        for proj_name, proj_data in projects.items():
            self.project_tree.insert("", "end", values=(
                proj_name,
                proj_data.get('prefix', ''),
                proj_data.get('theme', ''),
                proj_data.get('description', '')
            ))
    
    def add_cleanup_pattern(self):
        """添加清理模式"""
        if not hasattr(self, 'cleanup_vars'):
            self.cleanup_vars = []
        
        # 创建新的清理模式变量
        new_var = tk.StringVar()
        self.cleanup_vars.append(new_var)
        
        # 获取正则表达式标签页中的Canvas和滚动框架
        for child in self.notebook.winfo_children():
            if self.notebook.tab(child, "text") == "正则表达式":
                regex_tab = child
                break
        else:
            return
        
        # 获取Canvas中的滚动框架
        canvas = regex_tab.winfo_children()[0]
        scrollable_frame = canvas.winfo_children()[0]
        
        # 获取表名清理模式框架
        for child in scrollable_frame.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "表名清理模式":
                cleanup_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(cleanup_frame.grid_slaves()) // 2  # 每行有两个控件
        
        ttk.Entry(cleanup_frame, textvariable=new_var, width=30, font=self.default_font).grid(
            row=row_count, column=0, sticky=tk.W, pady=1, padx=3
        )
        
        # 更新滚动区域
        scrollable_frame.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox("all"))
    
    def remove_last_cleanup_pattern(self):
        """删除最后一条清理模式"""
        if hasattr(self, 'cleanup_vars') and len(self.cleanup_vars) > 0:
            # 移除最后一个变量
            self.cleanup_vars.pop()
            
            # 获取正则表达式标签页中的Canvas和滚动框架
            for child in self.notebook.winfo_children():
                if self.notebook.tab(child, "text") == "正则表达式":
                    regex_tab = child
                    break
            else:
                return
            
            # 获取Canvas中的滚动框架
            canvas = regex_tab.winfo_children()[0]
            scrollable_frame = canvas.winfo_children()[0]
            
            # 获取表名清理模式框架
            for child in scrollable_frame.winfo_children():
                if isinstance(child, ttk.LabelFrame) and child['text'] == "表名清理模式":
                    cleanup_frame = child
                    break
            else:
                return
            
            # 获取所有输入框
            entries = [widget for widget in cleanup_frame.winfo_children() if isinstance(widget, ttk.Entry)]
            if entries:
                # 删除最后一个输入框
                entries[-1].destroy()
            
            # 更新滚动区域
            scrollable_frame.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))
    
    def add_exclude_pattern(self):
        """添加排除模式"""
        if not hasattr(self, 'exclude_vars'):
            self.exclude_vars = []
        
        # 创建新的排除模式变量
        new_var = tk.StringVar()
        self.exclude_vars.append(new_var)
        
        # 获取过滤规则标签页
        for child in self.notebook.winfo_children():
            if self.notebook.tab(child, "text") == "过滤规则":
                filters_tab = child
                break
        else:
            return
        
        # 获取排除模式框架
        for child in filters_tab.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "排除模式（正则表达式）":
                exclude_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(exclude_frame.grid_slaves()) // 2  # 每行有两个控件
        
        ttk.Entry(exclude_frame, textvariable=new_var, width=30).grid(
            row=row_count, column=0, sticky=tk.W, pady=1
        )
    
    def remove_last_exclude_pattern(self):
        """删除最后一条排除模式"""
        if hasattr(self, 'exclude_vars') and len(self.exclude_vars) > 0:
            # 移除最后一个变量
            self.exclude_vars.pop()
            
            # 获取过滤规则标签页
            for child in self.notebook.winfo_children():
                if self.notebook.tab(child, "text") == "过滤规则":
                    filters_tab = child
                    break
            else:
                return
            
            # 获取排除模式框架
            for child in filters_tab.winfo_children():
                if isinstance(child, ttk.LabelFrame) and child['text'] == "排除模式（正则表达式）":
                    exclude_frame = child
                    break
            else:
                return
            
            # 获取所有输入框
            entries = [widget for widget in exclude_frame.winfo_children() if isinstance(widget, ttk.Entry)]
            if entries:
                # 删除最后一个输入框
                entries[-1].destroy()
    
    def add_include_pattern(self):
        """添加包含模式"""
        if not hasattr(self, 'include_vars'):
            self.include_vars = []
        
        # 创建新的包含模式变量
        new_var = tk.StringVar()
        self.include_vars.append(new_var)
        
        # 获取过滤规则标签页
        for child in self.notebook.winfo_children():
            if self.notebook.tab(child, "text") == "过滤规则":
                filters_tab = child
                break
        else:
            return
        
        # 获取包含模式框架
        for child in filters_tab.winfo_children():
            if isinstance(child, ttk.LabelFrame) and child['text'] == "包含模式（正则表达式）":
                include_frame = child
                break
        else:
            return
        
        # 创建新的输入行
        row_count = len(include_frame.grid_slaves()) // 2  # 每行有两个控件
        
        ttk.Entry(include_frame, textvariable=new_var, width=30).grid(
            row=row_count, column=0, sticky=tk.W, pady=1
        )
    
    def remove_last_include_pattern(self):
        """删除最后一条包含模式"""
        if hasattr(self, 'include_vars') and len(self.include_vars) > 0:
            # 移除最后一个变量
            self.include_vars.pop()
            
            # 获取过滤规则标签页
            for child in self.notebook.winfo_children():
                if self.notebook.tab(child, "text") == "过滤规则":
                    filters_tab = child
                    break
            else:
                return
            
            # 获取包含模式框架
            for child in filters_tab.winfo_children():
                if isinstance(child, ttk.LabelFrame) and child['text'] == "包含模式（正则表达式）":
                    include_frame = child
                    break
            else:
                return
            
            # 获取所有输入框
            entries = [widget for widget in include_frame.winfo_children() if isinstance(widget, ttk.Entry)]
            if entries:
                # 删除最后一个输入框
                entries[-1].destroy()
    
    def add_column(self, tree, column_type):
        """添加列"""
        # 这里可以添加添加列的逻辑
        messagebox.showinfo("提示", "添加列功能待实现")
    
    def edit_column(self, tree, column_type):
        """编辑列"""
        # 这里可以添加编辑列的逻辑
        messagebox.showinfo("提示", "编辑列功能待实现")
    
    def delete_column(self, tree):
        """删除列"""
        # 这里可以添加删除列的逻辑
        messagebox.showinfo("提示", "删除列功能待实现")

# 添加主函数，用于启动应用程序
def main():
    # 创建根窗口
    if USE_THEMED_TK:
        root = ThemedTk(theme="arc")
    else:
        root = tk.Tk()
    
    # 创建应用程序实例
    app = ConfigFormApp(root)
    
    # 启动主事件循环
    root.mainloop()

# 当脚本直接运行时，调用主函数
if __name__ == "__main__":
    main()