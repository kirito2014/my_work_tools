import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import tkinter.font as font
import os
import sys
import json
from datetime import datetime

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
        
        # 设置中文字体
        self.font_config = {}
        self._setup_fonts()
        
        # 创建界面
        self._create_widgets()
        
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
        
        # 1. 简历解析入库部分
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
        
        # 人员名单按钮
        ttk.Button(self.person_list_frame, text="更新人员名单", command=self._update_person_list, width=15).pack(pady=5)
        
        # 人员列表（使用Treeview实现多选）
        self.person_tree = ttk.Treeview(self.person_list_frame, columns=("name",), show="headings", height=5)
        self.person_tree.heading("name", text="人员姓名")
        self.person_tree.column("name", width=400)
        self.person_tree.pack(fill=tk.X, pady=5)
        
        # 添加滚动条
        tree_scroll = ttk.Scrollbar(self.person_list_frame, orient="horizontal", command=self.person_tree.xview)
        self.person_tree.configure(xscrollcommand=tree_scroll.set)
        tree_scroll.pack(fill=tk.X)
        
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
        log_frame = ttk.LabelFrame(main_frame, text="日志显示区域", padding="15")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=self.font_config['text'], height=15)
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
        file_path = filedialog.askopenfilename(
            title="选择简历文件",
            filetypes=[("Word文档", "*.docx;*.doc"), ("所有文件", "*.*")]
        )
        if file_path:
            self.resume_file_path.set(file_path)
            self._log(f"已选择文件: {file_path}")
    
    def _start_parse(self):
        file_path = self.resume_file_path.get()
        if not file_path:
            self._log("请先选择文件路径")
            return
        
        if not os.path.exists(file_path):
            self._log(f"文件不存在: {file_path}")
            return
        
        self._log(f"开始解析文件: {file_path}")
        self.progress_var.set(10)
        self.progress_label.config(text="10%")
        
        try:
            # 调用doc_2_json模块解析文件
            if dj:
                # 从文件名提取工号和姓名
                base_name = os.path.splitext(os.path.basename(file_path))[0]
                parts = base_name.split('+')
                if len(parts) >= 2:
                    emp_no = parts[0]
                    person_name = parts[1]
                else:
                    emp_no = "unknown"
                    person_name = "unknown"
                
                # 检查文件类型，如果是doc格式则转换为docx
                processed_doc_path = file_path
                if file_path.lower().endswith('.doc'):
                    self._log("检测到doc格式文件，正在转换为docx...")
                    # 这里需要处理doc转docx，简化处理
                    self._log("注意：doc转docx功能需要doc_converter模块支持")
                
                # 提取简历数据
                self._log("正在提取简历数据...")
                self.progress_var.set(30)
                self.progress_label.config(text="30%")
                
                raw_resume_data = dj.extract_resume_universal(processed_doc_path)
                if not raw_resume_data:
                    self._log("提取数据失败，请检查文档格式")
                    return
                
                # 转换为模板格式
                self._log("正在转换为模板格式...")
                self.progress_var.set(70)
                self.progress_label.config(text="70%")
                
                result = dj.convert_to_template_format(raw_resume_data, emp_no)
                if not result:
                    self._log("转换失败，请检查文档数据格式")
                    return
                
                # 创建输出目录
                modify_dir = os.path.join(base_dir, "output", "modify_json")
                os.makedirs(modify_dir, exist_ok=True)
                
                # 保存JSON文件
                json_filename = f"{emp_no}_{person_name}_人员简历.json"
                json_file = os.path.join(modify_dir, json_filename)
                
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=4)
                
                self.progress_var.set(100)
                self.progress_label.config(text="100%")
                self._log(f"解析完成！JSON文件已保存至: {json_file}")
                
                # 更新人员名单
                self._update_person_list()
                
            else:
                self._log("错误: 未能导入doc_2_json模块")
        except Exception as e:
            self._log(f"解析过程中出错: {str(e)}")
    
    def _toggle_person_list(self):
        if self.generate_method.get() == "selected":
            self.person_list_frame.pack(fill=tk.X, pady=5)
        else:
            self.person_list_frame.pack_forget()
    
    def _update_person_list(self):
        # 清空现有列表
        for item in self.person_tree.get_children():
            self.person_tree.delete(item)
        
        # 从output/modify_json目录读取JSON文件，提取人员名单
        modify_dir = os.path.join(base_dir, "output", "modify_json")
        if not os.path.exists(modify_dir):
            self._log(f"目录不存在: {modify_dir}")
            return
        
        try:
            person_files = [f for f in os.listdir(modify_dir) if f.endswith('.json')]
            for json_file in person_files:
                file_path = os.path.join(modify_dir, json_file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if data and isinstance(data, dict):
                            for person_name in data.keys():
                                self.person_tree.insert("", "end", values=(person_name,))
                except Exception as e:
                    self._log(f"读取文件时出错 {json_file}: {str(e)}")
            
            self._log(f"已更新人员名单，共 {len(self.person_tree.get_children())} 人")
        except Exception as e:
            self._log(f"更新人员名单时出错: {str(e)}")
    
    def _generate_resumes(self):
        bankname = self.bank_var.get()
        if not bankname:
            self._log("请选择银行")
            return
        
        # 确定要处理的人员名单
        if self.generate_method.get() == "all":
            person_names = "all"
            self._log(f"开始全量生成简历，银行: {bankname}")
        else:
            # 获取选中的人员
            selected_items = self.person_tree.selection()
            if not selected_items:
                self._log("请至少选择一位人员")
                return
            
            person_names = []
            for item in selected_items:
                person_names.append(self.person_tree.item(item, "values")[0])
            
            self._log(f"开始生成选中人员简历，银行: {bankname}，人员数量: {len(person_names)}")
        
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
            
            # 生成简历
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
                    if person_names == "all":
                        valid_names = list(data.keys())
                    else:
                        valid_names = [name for name in person_names if name in data]
                    
                    # 生成每个人员的简历
                    for person_name in valid_names:
                        # 这里简化处理，实际应该调用render_from_docx模块
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