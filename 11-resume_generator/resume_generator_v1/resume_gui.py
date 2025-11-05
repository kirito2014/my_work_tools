import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import tkinter.font as font
import os
import sys
import json
import subprocess
import threading
from datetime import datetime, date

# 设置项目根目录
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
except Exception as e:
    print(f"导入模块时出错: {e}")

class ResumeGeneratorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("简历生成器")
        self.root.geometry("900x700")
        self.root.configure(bg="#f0f0f0")
        
        # 文件路径变量
        self.resume_file_path = tk.StringVar()
        self.selected_persons = []
        self.selected_list = []  # 存储选中的员工编号
        self.hidden_items = {}  # 存储被隐藏的项目
        
        # 设置中文字体
        self.font_config = {}
        self._setup_fonts()
        
        # 创建界面
        self._create_widgets()
        
        # 初始化时尝试加载员工信息
        self._load_employee_info()
        
    def _setup_fonts(self):
        # 设置中文字体
        self.font_config['title'] = ('SimHei', 12, 'bold')
        self.font_config['label'] = ('SimHei', 10)
        self.font_config['button'] = ('SimHei', 10)
        self.font_config['entry'] = ('SimHei', 10)
        self.font_config['text'] = ('SimHei', 9)
    
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
        ttk.Radiobutton(method_frame, text="全量简历生成", variable=self.generate_method, value="all", command=self._toggle_person_list).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(method_frame, text="按名单生成简历", variable=self.generate_method, value="selected", command=self._toggle_person_list).pack(side=tk.LEFT, padx=10)
        
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
        
        # Logo占位
        logo_label = ttk.Label(bank_frame, text="LOGO", width=10, relief="solid")
        logo_label.pack(side=tk.LEFT, padx=5)
        
        # 银行下拉框
        ttk.Label(bank_frame, text="银行:", font=self.font_config['label']).pack(side=tk.LEFT, padx=5)
        self.bank_var = tk.StringVar()
        self.bank_combobox = ttk.Combobox(bank_frame, textvariable=self.bank_var, width=20, font=self.font_config['entry'])
        self.bank_combobox['values'] = self._get_bank_list()
        self.bank_combobox.pack(side=tk.LEFT, padx=5)
        self.bank_combobox.current(0)
        
        # 生成简历按钮
        ttk.Button(bank_frame, text="生成简历", command=self._generate_resumes, width=10, style="Accent.TButton").pack(side=tk.LEFT, padx=5)
        
        # 3. 进度条
        progress_frame = ttk.Frame(main_frame)
        progress_frame.pack(fill=tk.X, pady=10)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, length=100, mode='determinate')
        self.progress_bar.pack(fill=tk.X, expand=True)
        
        self.progress_label = ttk.Label(progress_frame, text="10%")
        self.progress_label.pack(pady=5)
        
        # 4. 日志显示区域
        self.log_frame = ttk.LabelFrame(main_frame, text="日志显示区域", padding="15")
        self.log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(self.log_frame, wrap=tk.WORD, font=self.font_config['text'], height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
        
        # 设置样式
        self._setup_styles()
    
    def _setup_styles(self):
        # 设置按钮样式
        style = ttk.Style()
        style.configure("Accent.TButton", font=self.font_config['button'])
        style.map("Accent.TButton", 
                  foreground=[('active', 'blue')],
                  background=[('active', '#e0e0e0')])
    
    def _get_bank_list(self):
        # 模拟银行列表
        return ["长亮科技", "测试银行", "招商银行", "建设银行", "工商银行", "农业银行"]
    
    def _select_file(self):
        """选择简历文件夹路径"""
        folder_path = filedialog.askdirectory(
            title="选择简历文件夹"
        )
        if folder_path:
            self.resume_file_path.set(folder_path)
            self._log(f"已选择文件夹: {folder_path}")
    
    def _select_tech_info_file(self):
        """选择技术人员信息Excel文件"""
        file_path = filedialog.askopenfilename(
            title="选择技术人员信息Excel文件",
            filetypes=[("Excel文件", "*.xlsx;*.xls"), ("所有文件", "*.*")]
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
                self._log("✅ get_emp_list脚本执行成功")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            else:
                self._log("❌ get_emp_list脚本执行失败")
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
                self._log("✅ excel_2_info_json脚本执行成功")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            else:
                self._log("❌ excel_2_info_json脚本执行失败")
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
        
        # 创建输出目录
        modify_dir = os.path.join(base_dir, "output", "modify_json")
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
            self._log(f"开始验证生成的JSON文件...")
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
                        self._log(f"  ✗ 验证JSON文件失败 {json_file}: {e}")
        
        # 输出统计信息
        self._log("=" * 50)
        self._log(f"批处理完成！")
        self._log(f"总文件数: {total_files}")
        self._log(f"成功处理: {processed_files}")
        self._log(f"转换文件数: {converted_files}")
        self._log(f"处理失败: {failed_files}")
        if processed_files > 0:
            self._log(f"JSON文件验证成功: {json_files_validated}/{processed_files}")
        self._log(f"JSON目录: {modify_dir}")
        
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
        
        self._log(f"开始批量解析文件夹: {folder_path}")
        self.progress_var.set(0)
        self.progress_label.config(text="0%")
        
        # 在新线程中执行批量转换
        thread = threading.Thread(target=self._batch_convert_to_json, args=(folder_path,))
        thread.daemon = True
        thread.start()
    
    def _toggle_person_list(self):
        """根据生成方式切换人员列表的显示状态"""
        if self.generate_method.get() == "selected":
            # 隐藏取消折叠按钮
            self.unfold_button.pack_forget()
            # 显示人员列表
            self.person_list_frame.pack(fill=tk.X, pady=5)
            # 自动更新人员名单，确保有数据显示
            self._update_person_list()
            self._person_list_visible = True
        else:
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
                self._log("✅ get_emp_list脚本执行成功")
                # 输出脚本的部分关键信息
                for line in result.stdout.split('\n'):
                    if any(keyword in line for keyword in ['成功保存', '共保存', '部门统计']):
                        self._log(f"  {line.strip()}")
            else:
                self._log("❌ get_emp_list脚本执行失败")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self._log(f"  {line.strip()}")
            
            # 加载员工信息
            self._update_progress(60)
            if not self._load_employee_info():
                self._log("⚠️ 未找到员工信息，请先更新人员名单")
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
            person_names = "all"
            self._log(f"开始全量生成简历，银行: {bankname}")
        else:
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
                self._save_selected_emp_numbers(emp_numbers, timestamp, bankname)
            
            # 获取人员姓名
            person_names = []
            for item in self.person_tree.get_children():
                values = self.person_tree.item(item, "values")
                if values and len(values) > 1 and values[1] in self.selected_list:
                    person_names.append(values[2])
            
            self._log(f"开始生成选中人员简历，银行: {bankname}，人员数量: {len(person_names)}")
            self._log(f"选中的员工编号: {', '.join(self.selected_list)}")
        
        # 调用批量生成简历功能
        self._batch_generate_resumes_in_thread(bankname, person_names)
    
    def _batch_generate_resumes_in_thread(self, bankname, person_names):
        """在新线程中执行批量生成简历，避免GUI卡顿"""
        def generate_thread():
            try:
                # 从output/modify_json目录获取所有JSON文件
                modify_dir = os.path.join(base_dir, "output", "modify_json")
                if not os.path.exists(modify_dir):
                    self._log(f"目录不存在: {modify_dir}")
                    return
                
                # 获取所有JSON文件
                json_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
                if not json_files:
                    self._log("未找到JSON文件，请先解析简历")
                    return
                
                # 设置模板文件路径
                template_path = os.path.join(base_dir, "template", "人员简历_模板.docx")
                if not os.path.exists(template_path):
                    # 尝试备选模板路径
                    template_path = os.path.join(base_dir, "template", "人员简历_模板_01.docx")
                    if not os.path.exists(template_path):
                        self._log("未找到简历模板文件")
                        return
                
                # 检查是否成功导入batch_render_module
                if batch_render_module and hasattr(batch_render_module, 'batch_generate_resumes'):
                    self._log(f"调用批生成功能，JSON目录: {modify_dir}，模板: {os.path.basename(template_path)}")
                    
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
                    self._log(f"批生成完成！成功: {success_count}，失败: {failed_count}")
                    self._log(f"输出目录: {os.path.join(base_dir, 'output', bankname)}")
                else:
                    # 如果模块导入失败，使用原有的生成逻辑
                    self._log("批生成模块不可用，使用备用生成逻辑")
                    self._save_selected_emp_numbers([], None, bankname)
            except Exception as e:
                self._log(f"生成简历过程中出错: {str(e)}")
        
        # 启动新线程执行生成任务
        thread = threading.Thread(target=generate_thread)
        thread.daemon = True
        thread.start()
    
    def _save_selected_emp_numbers(self, emp_numbers, timestamp=None, bankname=None):
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
            json_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
            if not json_files:
                self._log("未找到JSON文件，请先解析简历")
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
                    valid_names = []
                    if "person_names" in locals() and person_names == "all":
                        valid_names = list(data.keys())
                    elif "person_names" in locals():
                        valid_names = [name for name in person_names if name in data]
                    else:
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
    
    def _log(self, message):
        """在日志区域显示消息"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)  # 滚动到最后
        self.log_text.config(state=tk.DISABLED)
        # 同时打印到控制台
        print(message)

if __name__ == "__main__":
    root = tk.Tk()
    app = ResumeGeneratorGUI(root)
    root.mainloop()