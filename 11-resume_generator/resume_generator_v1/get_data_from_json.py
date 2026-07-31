# !/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import glob
import time
import threading
import queue
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkthemes import ThemedTk
import pandas as pd
from datetime import datetime
from openpyxl.styles import PatternFill, Font, Alignment  # 新增：用于处理 Excel 单元格样式

class JsonExtractorApp(ThemedTk):
    def __init__(self):
        super().__init__(theme="arc")
        
        self.title("简历 JSON 数据提取工具")
        self.geometry("950x750") 
        self.configure(padx=20, pady=20)
        
        if getattr(sys, 'frozen', False):
            self.base_dir = os.path.dirname(sys.executable)
        else:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))
            
        self.current_folder = os.path.join(self.base_dir, "output", "modify_json")
        self.json_files = []
        
        # 界面显示的中文筛选结构（按此顺序导出Excel）
        self.structure = {
            "基本信息": ["工作年限", "毕业时间", "毕业院校", "专业", "最高学历", "部门", "职位", "个人简介"],
            "工作能力": ["业务能力", "资质认证", "培训经历", "技能标签"],
            "附加信息": ["一级部门", "二级部门", "岗位", "职务类别", "专业职级", "公司邮箱", "在职状态", 
                         "入职时间", "首次入职时间", "司龄", "发薪公司", "常驻地", "性别", "出生日期", 
                         "年龄", "政治面貌", "身份证号", "联系电话", "合同法人","籍贯"],
            "特殊信息": ["学位", "参加工作时间", "教育_学历类型", "教育_毕业时间", "教育_毕业院校", "教育_专业", "教育_最高学历"],
            "工作经历": ["工作_开始时间", "工作_结束时间", "工作_公司", "工作_职位", "工作_描述", "工作_时长"],
            "项目经历": ["项目_开始时间", "项目_结束时间", "项目_名称", "项目_角色", "项目_描述", "项目_时长"]
        }

        # 汇总/自定义合并列字段名（固定输出列名，不属于常规勾选字段）
        self.WORK_SUMMARY_FIELD = "工作_汇总"
        self.PROJ_SUMMARY_FIELD = "项目_汇总"

        # 工作经历/项目经历 合并列的自定义可选字段（中文标签 -> JSON 字段名），
        # 开始时间/结束时间/公司名称(项目名称) 为固定默认列，不在此列出
        self.merge_custom_options = {
            "工作经历": [("职位", "Position"), ("描述", "JobDescription")],
            "项目经历": [("角色", "ProjectRole"), ("描述", "JobDescription")],
        }
        self.merge_mode_vars = {}      # category -> tk.StringVar("off"/"full"/"custom")
        self.merge_custom_vars = {}    # category -> {json_key: tk.BooleanVar}
        self.merge_custom_widgets = {} # category -> [Checkbutton, ...]，用于根据模式启用/禁用
        
        # 分类对应的表头颜色映射 (HEX 颜色码)
        self.category_colors = {
            "主键": "607D8B",      # 蓝灰色 (人员名称、员工号)
            "基本信息": "4CAF50",  # 绿色
            "工作能力": "2196F3",  # 蓝色
            "附加信息": "9C27B0",  # 紫色
            "特殊信息": "FF9800",  # 橙色
            "工作经历": "009688",  # 蓝绿色 (Teal)
            "项目经历": "E91E63"   # 玫红色 (Pink)
        }
        
        # 标量字段映射
        self.key_mapping = {
            "工作年限": "WorkYears", "毕业时间": "GraduationTime", "毕业院校": "GraduationSchool", 
            "专业": "Major", "最高学历": "HighestEducation", "部门": "Department", 
            "职位": "Title", "个人简介": "PersonalProfile",
            "业务能力": "BusinessAbility", "资质认证": "Certification", 
            "培训经历": "Training", "技能标签": "SkillTag",
            "一级部门": "DepartmentLevel1", "二级部门": "DepartmentLevel2", "岗位": "Position", 
            "职务类别": "JobCategory", "专业职级": "ProfessionalLevel", "公司邮箱": "CompanyEmail", 
            "在职状态": "EmploymentStatus", "入职时间": "EntryDate", "首次入职时间": "FirstEntryDate", 
            "司龄": "CompanyYears", "发薪公司": "PaymentCompany", "常驻地": "BaseLocation", 
            "性别": "Gender", "出生日期": "BirthDate", "年龄": "Age", 
            "政治面貌": "PoliticalStatus", "身份证号": "IDNumber", "联系电话": "PhoneNumber", 
            "合同法人": "ContractLegalPerson", "学位": "Degree", "参加工作时间": "StartWorkDate",
            "籍贯":"NativePlace"
        }
        
        # 列表型字段映射
        self.list_fields_map = {
            "教育_学历类型": ("EducationList", "DegreeType"),
            "教育_毕业时间": ("EducationList", "GraduationTime"),
            "教育_毕业院校": ("EducationList", "GraduationSchool"),
            "教育_专业": ("EducationList", "Major"),
            "教育_最高学历": ("EducationList", "HighestEducation"),
            
            "工作_开始时间": ("WorkExperience", "StartTime"),
            "工作_结束时间": ("WorkExperience", "EndTime"),
            "工作_公司": ("WorkExperience", "CompanyName"),
            "工作_职位": ("WorkExperience", "Position"),
            "工作_描述": ("WorkExperience", "JobDescription"),
            "工作_时长": ("WorkExperience", "Duration"),
            
            "项目_开始时间": ("ProjectExperience", "StartTime"),
            "项目_结束时间": ("ProjectExperience", "EndTime"),
            "项目_名称": ("ProjectExperience", "ProjectName"),
            "项目_角色": ("ProjectExperience", "ProjectRole"),
            "项目_描述": ("ProjectExperience", "JobDescription"),
            "项目_时长": ("ProjectExperience", "Duration")
        }
        
        self.check_vars = {}

        # 人员名单筛选相关状态
        self.emp_list_file_path = None      # 已上传的名单文件路径
        self.pending_emp_ids = set()        # 从已上传文件中解析出的员工号（与 confirmed_emp_ids 一致，上传后立即生效）
        self.confirmed_emp_ids = set()      # 当前生效的员工号集合
        self.emp_list_confirmed = False     # 是否已上传并生效名单

        # 彩蛋：连续点击 "JSON数量" 标签相关状态
        self.egg_click_count = 0
        self.egg_last_click_time = 0.0

        # 导出格式（xlsx / csv）
        self.export_format = tk.StringVar(value="xlsx")

        # 记住上次选择：配置文件路径
        self.settings_path = os.path.join(self.base_dir, "app_settings.json")

        # 后台导出线程 / 进度条相关状态
        self.export_queue = queue.Queue()
        self.export_thread = None
        self.last_export_dir = None

        self.init_ui()
        self._load_settings()
        self.update_folder(self.current_folder)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def init_ui(self):
        folder_frame = ttk.LabelFrame(self, text=" 目录选择与状态 ")
        folder_frame.pack(fill="x", pady=(0, 15), ipady=10)
        
        self.path_label = ttk.Label(folder_frame, text="当前路径: 未选择", wraplength=600)
        self.path_label.pack(side="left", padx=15, pady=5)
        
        self.count_label = ttk.Label(folder_frame, text="JSON数量: 0", font=("", 10, "bold"), foreground="#0052cc", cursor="hand2")
        self.count_label.pack(side="left", padx=20, pady=5)
        self.count_label.bind("<Button-1>", self.on_count_label_click)
        
        btn_select = ttk.Button(folder_frame, text="选择文件夹", command=self.select_folder)
        btn_select.pack(side="right", padx=15, pady=5)

        # ---- 人员名单筛选区块 ----
        emp_frame = ttk.LabelFrame(self, text=" 人员名单筛选（可选，支持 xlsx/txt，上传后自动识别员工号；不上传则默认导出全部人员） ")
        emp_frame.pack(fill="x", pady=(0, 15), ipady=8)

        emp_row1 = ttk.Frame(emp_frame)
        emp_row1.pack(fill="x", padx=15, pady=(5, 3))

        btn_upload_emp = ttk.Button(emp_row1, text="上传人员名单", command=self.upload_emp_list)
        btn_upload_emp.pack(side="left")

        btn_cancel_emp = ttk.Button(emp_row1, text="取消选择", command=self.cancel_emp_list)
        btn_cancel_emp.pack(side="left", padx=(10, 0))

        self.emp_file_label = ttk.Label(emp_row1, text="未上传文件", foreground="#666666")
        self.emp_file_label.pack(side="left", padx=(15, 0))

        self.emp_status_label = ttk.Label(emp_frame, text="", font=("", 10, "bold"), foreground="#0052cc")
        self.emp_status_label.pack(anchor="w", padx=15, pady=(0, 5))

        filter_frame = ttk.LabelFrame(self, text=" 数据字段筛选 (按分类顺序导出，人员名称和员工号默认居首) ")
        filter_frame.pack(fill="both", expand=True, pady=(0, 15), ipady=5)
        
        style = ttk.Style()
        style.configure("Bold.TCheckbutton", font=("", 10, "bold"))
        
        canvas = tk.Canvas(filter_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(filter_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True, padx=5, pady=5)
        scrollbar.pack(side="right", fill="y")

        for category, items in self.structure.items():
            cat_frame = ttk.Frame(scrollable_frame)
            cat_frame.pack(fill="x", padx=15, pady=8)
            
            cat_var = tk.BooleanVar(value=False)
            self.check_vars[category] = {"var": cat_var, "children": {}}
            
            cb_cat = ttk.Checkbutton(
                cat_frame, text=category, variable=cat_var, 
                command=lambda c=category: self.on_category_toggle(c),
                style="Bold.TCheckbutton"
            )
            cb_cat.pack(anchor="w")
            
            sub_frame = ttk.Frame(cat_frame)
            sub_frame.pack(fill="x", padx=35, pady=5)
            
            col, row = 0, 0
            for item in items:
                item_var = tk.BooleanVar(value=False)
                self.check_vars[category]["children"][item] = item_var
                cb_item = ttk.Checkbutton(
                    sub_frame, text=item, variable=item_var,
                    command=lambda c=category: self.on_item_toggle(c)
                )
                cb_item.grid(row=row, column=col, sticky="w", padx=(0, 15), pady=3)
                col += 1
                if col > 4: 
                    col = 0
                    row += 1

            if category in ("工作经历", "项目经历"):
                self._build_merge_section(cat_frame, category)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", pady=10)

        style.configure("Accent.TButton", font=("", 11, "bold"))

        # 导出格式选择
        format_row = ttk.Frame(btn_frame)
        format_row.pack(side="top", pady=(0, 8))
        ttk.Label(format_row, text="导出格式：").pack(side="left")
        ttk.Radiobutton(format_row, text="Excel (.xlsx)", value="xlsx", variable=self.export_format).pack(side="left", padx=(5, 15))
        ttk.Radiobutton(format_row, text="CSV (.csv)", value="csv", variable=self.export_format).pack(side="left")

        # 保存 + 打开文件夹 按钮
        action_row = ttk.Frame(btn_frame)
        action_row.pack(side="top")
        self.btn_save = ttk.Button(action_row, text="保存提取数据", style="Accent.TButton", command=self.save_data)
        self.btn_save.pack(side="left", ipadx=30, ipady=5, padx=(0, 10))
        self.btn_open_folder = ttk.Button(action_row, text="打开文件夹", command=self.open_export_folder, state="disabled")
        self.btn_open_folder.pack(side="left", ipady=5)

        # 进度条 + 状态提示
        progress_row = ttk.Frame(btn_frame)
        progress_row.pack(side="top", fill="x", padx=40, pady=(8, 0))
        self.progress_bar = ttk.Progressbar(progress_row, orient="horizontal", mode="determinate")
        self.progress_bar.pack(fill="x")
        self.progress_label = ttk.Label(btn_frame, text="", foreground="#666666")
        self.progress_label.pack(side="top", pady=(3, 0))

    def open_export_folder(self):
        if not self.last_export_dir or not os.path.exists(self.last_export_dir):
            messagebox.showinfo("提示", "还没有可打开的导出文件夹，请先完成一次导出。")
            return
        try:
            if sys.platform == "win32":
                os.startfile(self.last_export_dir)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", self.last_export_dir])
            else:
                subprocess.Popen(["xdg-open", self.last_export_dir])
        except Exception as e:
            messagebox.showerror("打开失败", f"无法打开文件夹:\n{e}")

    def on_category_toggle(self, category):
        cat_state = self.check_vars[category]["var"].get()
        for item_var in self.check_vars[category]["children"].values():
            item_var.set(cat_state)

    def on_item_toggle(self, category):
        child_vars = [var.get() for var in self.check_vars[category]["children"].values()]
        if all(child_vars):
            self.check_vars[category]["var"].set(True)
        else:
            self.check_vars[category]["var"].set(False)

    def _build_merge_section(self, parent_frame, category):
        """在指定分类（工作经历/项目经历）下方构建"汇总 / 自定义合并"二选一的设置区块"""
        is_work = category == "工作经历"
        default_desc = (
            "默认包含：开始时间、结束时间、公司名称（自动包含，无需勾选）" if is_work
            else "默认包含：开始时间、结束时间、项目名称（自动包含，无需勾选）"
        )
        options = self.merge_custom_options[category]

        mode_var = tk.BooleanVar(value=False)
        self.merge_mode_vars[category] = mode_var
        self.merge_custom_vars[category] = {}
        self.merge_custom_widgets[category] = []

        merge_frame = ttk.LabelFrame(parent_frame, text=f" {category}合并列（导出为单独一列） ")
        merge_frame.pack(fill="x", padx=35, pady=(0, 8))

        radio_row = ttk.Frame(merge_frame)
        radio_row.pack(fill="x", padx=10, pady=(5, 3))
        ttk.Checkbutton(
            radio_row, text="启用自定义合并列", variable=mode_var,
            command=lambda c=category: self.on_merge_mode_change(c)
        ).pack(side="left")

        ttk.Label(merge_frame, text=default_desc, foreground="#666666").pack(anchor="w", padx=10)

        custom_row = ttk.Frame(merge_frame)
        custom_row.pack(fill="x", padx=10, pady=(3, 5))
        ttk.Label(custom_row, text="补充列：").pack(side="left")
        for label, json_key in options:
            var = tk.BooleanVar(value=False)
            self.merge_custom_vars[category][json_key] = var
            cb = ttk.Checkbutton(custom_row, text=label, variable=var)
            cb.pack(side="left", padx=(5, 15))
            self.merge_custom_widgets[category].append(cb)

        self.on_merge_mode_change(category)  # 初始化补充列的启用/禁用状态

    def on_merge_mode_change(self, category):
        """启用自定义合并列时，激活补充列勾选框；未启用时禁用"""
        enabled = self.merge_mode_vars[category].get()
        state = "normal" if enabled else "disabled"
        for cb in self.merge_custom_widgets.get(category, []):
            cb.config(state=state)

    def _normalize_emp_id(self, raw):
        """将任意形式的员工号统一规范为 5 位数字字符串（不足前面补0），非数字内容返回 None"""
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        # 处理 Excel 数值列被 pandas 读成浮点数的情况，例如 "1234.0"
        if s.endswith(".0"):
            s = s[:-2]
        if not s.isdigit():
            return None
        return s.zfill(5)

    def _parse_emp_list_file(self, file_path):
        """解析上传的人员名单文件（xlsx 或 txt），返回规范化后的员工号集合"""
        ext = os.path.splitext(file_path)[1].lower()
        emp_ids = set()

        if ext == ".xlsx":
            df = pd.read_excel(file_path, header=None, dtype=str)
            for col in df.columns:
                for raw_val in df[col].dropna().tolist():
                    emp_id = self._normalize_emp_id(raw_val)
                    if emp_id:
                        emp_ids.add(emp_id)
        elif ext == ".txt":
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            # 支持每行一个 或 逗号分隔（中英文逗号）两种格式
            content = content.replace('，', ',').replace('\r\n', '\n').replace('\r', '\n')
            raw_parts = content.replace('\n', ',').split(',')
            for raw_val in raw_parts:
                emp_id = self._normalize_emp_id(raw_val)
                if emp_id:
                    emp_ids.add(emp_id)
        else:
            raise ValueError("仅支持 .xlsx 或 .txt 格式的人员名单文件")

        return emp_ids

    def upload_emp_list(self):
        file_path = filedialog.askopenfilename(
            title="选择人员名单文件",
            filetypes=[("支持的名单文件", "*.xlsx *.txt"), ("Excel 文件", "*.xlsx"), ("文本文件", "*.txt")]
        )
        if not file_path:
            return

        try:
            emp_ids = self._parse_emp_list_file(file_path)
        except Exception as e:
            messagebox.showerror("解析失败", f"读取人员名单文件时发生错误:\n{e}")
            return

        if not emp_ids:
            messagebox.showwarning("未识别到员工号", "未能从该文件中解析出任何有效的员工号，请检查文件内容。")
            return

        self.emp_list_file_path = file_path
        self.pending_emp_ids = emp_ids
        # 上传成功后自动识别并直接生效，无需再点击"确定"按钮
        self.confirmed_emp_ids = emp_ids
        self.emp_list_confirmed = True

        self.emp_file_label.config(text=os.path.basename(file_path), foreground="#FF6600")
        self.emp_status_label.config(text=f"已选择 {len(self.confirmed_emp_ids)} 位员工", foreground="#0052cc")

    def cancel_emp_list(self):
        self.emp_list_file_path = None
        self.pending_emp_ids = set()
        self.confirmed_emp_ids = set()
        self.emp_list_confirmed = False

        self.emp_file_label.config(text="未上传文件", foreground="#666666")
        self.emp_status_label.config(text="已清除所选名单", foreground="#0052cc")

    def select_folder(self):
        folder = filedialog.askdirectory(initialdir=self.current_folder, title="选择包含 JSON 的文件夹")
        if folder:
            self.update_folder(folder)

    def update_folder(self, folder_path):
        self.current_folder = folder_path
        self.path_label.config(text=f"当前路径: {self.current_folder}")
        
        if os.path.exists(self.current_folder):
            search_pattern = os.path.join(self.current_folder, "*.json")
            self.json_files = glob.glob(search_pattern)
        else:
            self.json_files = []
            
        self.count_label.config(text=f"JSON数量: {len(self.json_files)}")

    def _load_settings(self):
        """启动时恢复上次的文件夹路径、字段勾选、合并列设置、导出格式"""
        if not os.path.exists(self.settings_path):
            return
        try:
            with open(self.settings_path, 'r', encoding='utf-8') as f:
                settings = json.load(f)
        except Exception:
            return  # 配置文件损坏或无法读取，忽略，使用默认值

        folder = settings.get("folder")
        if folder and os.path.exists(folder):
            self.current_folder = folder

        fmt = settings.get("export_format")
        if fmt in ("xlsx", "csv"):
            self.export_format.set(fmt)

        fields = settings.get("fields", {})
        for category, items in fields.items():
            if category not in self.check_vars:
                continue
            for item, checked in items.items():
                item_var = self.check_vars[category]["children"].get(item)
                if item_var is not None:
                    item_var.set(bool(checked))
            self.on_item_toggle(category)  # 同步分类总开关的勾选状态

        merge_mode = settings.get("merge_mode", {})
        for category, checked in merge_mode.items():
            if category in self.merge_mode_vars:
                self.merge_mode_vars[category].set(bool(checked))
                self.on_merge_mode_change(category)

        merge_custom = settings.get("merge_custom", {})
        for category, keys in merge_custom.items():
            if category in self.merge_custom_vars:
                for json_key, checked in keys.items():
                    if json_key in self.merge_custom_vars[category]:
                        self.merge_custom_vars[category][json_key].set(bool(checked))

    def _save_settings(self):
        """将当前的文件夹路径、字段勾选、合并列设置、导出格式写入配置文件，供下次启动恢复"""
        fields = {
            category: {item: var.get() for item, var in data["children"].items()}
            for category, data in self.check_vars.items()
        }
        merge_mode = {category: var.get() for category, var in self.merge_mode_vars.items()}
        merge_custom = {
            category: {key: var.get() for key, var in vars_dict.items()}
            for category, vars_dict in self.merge_custom_vars.items()
        }
        settings = {
            "folder": self.current_folder,
            "export_format": self.export_format.get(),
            "fields": fields,
            "merge_mode": merge_mode,
            "merge_custom": merge_custom,
        }
        try:
            with open(self.settings_path, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
        except Exception:
            pass  # 保存失败静默忽略，不影响关闭流程

    def _on_close(self):
        self._save_settings()
        self.destroy()

    def on_count_label_click(self, event):
        """彩蛋：连续点击「JSON数量」标签 5 次，弹出信息展示页面。
        若两次点击间隔超过 1.5 秒，视为重新计数（避免误触发）。"""
        now = time.time()
        if now - self.egg_last_click_time > 1.5:
            self.egg_click_count = 0
        self.egg_click_count += 1
        self.egg_last_click_time = now

        if self.egg_click_count >= 5:
            self.egg_click_count = 0
            self.show_easter_egg()

    def show_easter_egg(self):
        """展示信息页面：作者/版权信息、使用方法等，内容可自行修改 EASTER_EGG_TEXT。"""
        EASTER_EGG_TEXT = """简历 JSON 数据提取工具

——————————————————
作者信息
——————————————————
作者：[请在此处填写作者姓名]
联系方式：[请在此处填写联系方式]
版本：v1.0

——————————————————
版权声明
——————————————————
本工具仅供内部使用，禁止未经授权对外分发、传播或用于商业用途。
如有问题或建议，请联系作者。

——————————————————
使用方法
——————————————————
1. 点击「选择文件夹」，选取包含简历 JSON 文件的目录。
2. （可选）在「人员名单筛选」区域上传 xlsx / txt 名单文件，
   上传成功后会自动识别并生效，仅导出名单内员工；不上传则默认导出全部人员。
3. 在「数据字段筛选」区域勾选需要导出的字段
   （工作经历/项目经历下方可勾选"启用自定义合并列"，额外导出拼接后的经历文本列）。
4. 选择导出格式（xlsx / csv），点击「保存提取数据」，
   生成的文件会保存在程序所在目录下的「数据下载」文件夹中，
   导出完成后可点击「打开文件夹」直接查看。

——————————————————
彩蛋
——————————————————
连续点击 5 次左上角的「JSON数量」文字，即可再次打开本页面 :)
"""
        win = tk.Toplevel(self)
        win.title("关于本工具")
        win.geometry("560x480")
        win.transient(self)

        text_frame = ttk.Frame(win)
        text_frame.pack(fill="both", expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(text_frame, orient="vertical")
        scrollbar.pack(side="right", fill="y")

        text_widget = tk.Text(
            text_frame, wrap="word", padx=10, pady=10,
            yscrollcommand=scrollbar.set, font=("", 10)
        )
        text_widget.pack(side="left", fill="both", expand=True)
        scrollbar.config(command=text_widget.yview)

        text_widget.insert("1.0", EASTER_EGG_TEXT)
        text_widget.config(state="disabled")

        btn_close = ttk.Button(win, text="关闭", command=win.destroy)
        btn_close.pack(pady=(0, 10))

    def get_selected_fields(self):
        selected = []
        for category, data in self.check_vars.items():
            cat_selected = data["var"].get()
            if cat_selected:
                selected.extend(data["children"].keys())
            else:
                for item, item_var in data["children"].items():
                    if item_var.get():
                        selected.append(item)
        return list(set(selected))

    def save_data(self):
        if not self.json_files:
            messagebox.showwarning("无数据", "当前目录下没有 JSON 文件可供处理。")
            return
            
        fields_to_extract = self.get_selected_fields()
        need_work_summary = self.merge_mode_vars["工作经历"].get()
        need_proj_summary = self.merge_mode_vars["项目经历"].get()

        if not fields_to_extract and not need_work_summary and not need_proj_summary:
            messagebox.showwarning("未选择数据", "请至少选择一个需要提取的数据字段。")
            return

        ordered_fields = []
        # 构建字段到分类的反向映射字典，用于后续渲染表头颜色
        field_to_category = {"人员名称": "主键", "员工号": "主键"}
        
        for category, items in self.structure.items():
            for item in items:
                field_to_category[item] = category
                if item in fields_to_extract:
                    ordered_fields.append(item)

        WORK_SUMMARY_FIELD = self.WORK_SUMMARY_FIELD
        PROJ_SUMMARY_FIELD = self.PROJ_SUMMARY_FIELD

        # 逐条明细字段：勾选了工作经历/项目经历下的具体字段才需要按条目数展开多行
        need_work_detail = any(field.startswith("工作_") for field in ordered_fields)
        need_proj_detail = any(field.startswith("项目_") for field in ordered_fields)
        need_edu = any(field.startswith("教育_") for field in ordered_fields)

        # 汇总/自定义合并列由专门的切换按钮控制，与逐条明细字段是否勾选无关
        need_work = need_work_detail or need_work_summary
        need_proj = need_proj_detail or need_proj_summary

        if need_work_summary:
            ordered_fields.append(WORK_SUMMARY_FIELD)
            field_to_category[WORK_SUMMARY_FIELD] = "工作经历"
        if need_proj_summary:
            ordered_fields.append(PROJ_SUMMARY_FIELD)
            field_to_category[PROJ_SUMMARY_FIELD] = "项目经历"

        # Tkinter 变量只能在主线程读取，提前把自定义合并的补充字段取出，传给后台线程使用
        work_extra_keys = []
        if need_work_summary:
            work_extra_keys = [
                json_key for _, json_key in self.merge_custom_options["工作经历"]
                if self.merge_custom_vars["工作经历"][json_key].get()
            ]
        proj_extra_keys = []
        if need_proj_summary:
            proj_extra_keys = [
                json_key for _, json_key in self.merge_custom_options["项目经历"]
                if self.merge_custom_vars["项目经历"][json_key].get()
            ]

        # 根据人员名单筛选待处理的 json 文件：未上传名单时，默认处理全部文件
        files_to_process = self.json_files
        unmatched_emp_ids = []  # 已确认名单中，未能在当前文件夹找到对应 json 的员工号
        if self.emp_list_confirmed and self.confirmed_emp_ids:
            filtered_files = []
            matched_emp_ids = set()
            for fp in self.json_files:
                fname = os.path.basename(fp)
                name_parts = fname.split('_')
                emp_no_in_filename = name_parts[0] if name_parts else ""
                norm_emp_id = self._normalize_emp_id(emp_no_in_filename)
                if norm_emp_id in self.confirmed_emp_ids:
                    filtered_files.append(fp)
                    matched_emp_ids.add(norm_emp_id)
            files_to_process = filtered_files
            # 名单中没有对应上 json 文件的员工号，记录下来，不中断流程，继续处理已匹配的部分
            unmatched_emp_ids = sorted(self.confirmed_emp_ids - matched_emp_ids)

        export_format = self.export_format.get()  # "xlsx" 或 "csv"

        # 进入后台线程处理前，先锁定界面相关按钮、重置进度条
        self.btn_save.config(state="disabled")
        self.btn_open_folder.config(state="disabled")
        total = len(files_to_process)
        self.progress_bar["value"] = 0
        self.progress_bar["maximum"] = max(total, 1)
        self.progress_label.config(text=f"正在处理... 0/{total}")

        self.export_thread = threading.Thread(
            target=self._export_worker,
            kwargs=dict(
                files_to_process=files_to_process,
                ordered_fields=ordered_fields,
                field_to_category=field_to_category,
                need_work=need_work, need_proj=need_proj, need_edu=need_edu,
                need_work_detail=need_work_detail, need_proj_detail=need_proj_detail,
                need_work_summary=need_work_summary, need_proj_summary=need_proj_summary,
                work_extra_keys=work_extra_keys, proj_extra_keys=proj_extra_keys,
                unmatched_emp_ids=unmatched_emp_ids,
                export_format=export_format,
                WORK_SUMMARY_FIELD=WORK_SUMMARY_FIELD, PROJ_SUMMARY_FIELD=PROJ_SUMMARY_FIELD,
            ),
            daemon=True
        )
        self.export_thread.start()
        self.after(100, self._poll_export_queue)

    def _export_worker(self, files_to_process, ordered_fields, field_to_category,
                        need_work, need_proj, need_edu, need_work_detail, need_proj_detail,
                        need_work_summary, need_proj_summary, work_extra_keys, proj_extra_keys,
                        unmatched_emp_ids, export_format, WORK_SUMMARY_FIELD, PROJ_SUMMARY_FIELD):
        """后台线程：读取/解析每个 json 文件、拼装数据、写出 Excel/CSV。仅通过 self.export_queue 与主线程通信。"""
        parsed_data = []
        failed_files = []  # [(文件名, 错误信息), ...]，单个文件解析失败不影响其他文件继续处理
        total = len(files_to_process)

        for idx, file_path in enumerate(files_to_process, 1):
            filename = os.path.basename(file_path)
            try:
                name_from_filename = ""
                parts = filename.split('_')
                if len(parts) >= 2:
                    name_from_filename = parts[1]

                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_content = json.load(f)

                for root_key, person_data in raw_content.items():
                    if not isinstance(person_data, dict):
                        continue

                    basic_info = person_data.get("BasicInfo", {})
                    add_info = person_data.get("AdditionInfo", {})

                    raw_name = name_from_filename if name_from_filename else basic_info.get("Name", add_info.get("Name", root_key))
                    raw_emp_no = basic_info.get("EmpNo", add_info.get("EmpNo", ""))

                    works = person_data.get("WorkExperience", []) if need_work else []
                    projs = person_data.get("ProjectExperience", []) if need_proj else []
                    edus = person_data.get("SpecialInfo", {}).get("EducationList", []) if need_edu else []

                    # 只有勾选了逐条明细字段才需要按条目数展开多行；只启用汇总/自定义合并时仅导出一行
                    max_rows = max(1, len(works) if (need_work_detail and isinstance(works, list)) else 0,
                                     len(projs) if (need_proj_detail and isinstance(projs, list)) else 0,
                                     len(edus) if isinstance(edus, list) else 0)

                    scalars = {}
                    for field in ordered_fields:
                        if field in self.list_fields_map:
                            continue
                        if field in (WORK_SUMMARY_FIELD, PROJ_SUMMARY_FIELD):
                            continue  # 汇总字段单独处理，见下方
                        en_key = self.key_mapping.get(field, field)
                        val = self._find_value_in_dict(person_data, en_key)
                        scalars[field] = val if val is not None else ""

                    if need_work_summary:
                        scalars[WORK_SUMMARY_FIELD] = (
                            self._build_summary_text(works, "StartTime", "EndTime", "CompanyName", work_extra_keys)
                            if isinstance(works, list) and works else ""
                        )
                    if need_proj_summary:
                        scalars[PROJ_SUMMARY_FIELD] = (
                            self._build_summary_text(projs, "StartTime", "EndTime", "ProjectName", proj_extra_keys)
                            if isinstance(projs, list) and projs else ""
                        )

                    for i in range(max_rows):
                        row_data = {}
                        row_data["人员名称"] = raw_name if i == 0 else ""
                        row_data["员工号"] = raw_emp_no if i == 0 else ""

                        for field in ordered_fields:
                            if field in self.list_fields_map:
                                list_type, en_key = self.list_fields_map[field]
                                if list_type == "WorkExperience" and i < len(works):
                                    row_data[field] = works[i].get(en_key, "")
                                elif list_type == "ProjectExperience" and i < len(projs):
                                    row_data[field] = projs[i].get(en_key, "")
                                elif list_type == "EducationList" and i < len(edus):
                                    row_data[field] = edus[i].get(en_key, "")
                                else:
                                    row_data[field] = ""
                            else:
                                row_data[field] = scalars[field] if i == 0 else ""

                        parsed_data.append(row_data)
            except Exception as e:
                # 单个文件解析失败：记录下来，不中断整体流程，继续处理下一个文件
                failed_files.append((filename, str(e)))

            self.export_queue.put(("progress", idx, total))

        if not parsed_data:
            self.export_queue.put(("empty", unmatched_emp_ids, failed_files))
            return

        try:
            df = pd.DataFrame(parsed_data)

            save_dir = os.path.join(self.base_dir, "数据下载")
            os.makedirs(save_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

            if export_format == "csv":
                filename_output = f"已下载_{timestamp}.csv"
                save_path = os.path.join(save_dir, filename_output)
                # utf-8-sig 带 BOM，避免用 Excel 直接打开 csv 时中文乱码
                df.to_csv(save_path, index=False, encoding="utf-8-sig")
            else:
                filename_output = f"已下载_{timestamp}.xlsx"
                save_path = os.path.join(save_dir, filename_output)
                with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
                    worksheet = writer.sheets['Sheet1']

                    white_bold_font = Font(color="FFFFFF", bold=True)
                    default_alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
                    summary_alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

                    for col_idx, col_name in enumerate(df.columns, 1):
                        category = field_to_category.get(col_name, "主键")
                        hex_color = self.category_colors.get(category, "000000")
                        fill = PatternFill(start_color=hex_color, end_color=hex_color, fill_type="solid")

                        header_cell = worksheet.cell(row=1, column=col_idx)
                        header_cell.fill = fill
                        header_cell.font = white_bold_font
                        header_cell.alignment = default_alignment

                        is_summary_col = col_name in (WORK_SUMMARY_FIELD, PROJ_SUMMARY_FIELD)
                        if is_summary_col:
                            col_letter = header_cell.column_letter
                            worksheet.column_dimensions[col_letter].width = 60

                        cell_alignment = summary_alignment if is_summary_col else default_alignment
                        for r in range(2, worksheet.max_row + 1):
                            worksheet.cell(row=r, column=col_idx).alignment = cell_alignment

            self.export_queue.put(("done", save_path, unmatched_emp_ids, failed_files))
        except Exception as e:
            self.export_queue.put(("error", str(e)))

    def _poll_export_queue(self):
        """在主线程中周期性轮询后台线程的进度/结果消息，安全地更新界面"""
        try:
            while True:
                msg = self.export_queue.get_nowait()
                kind = msg[0]

                if kind == "progress":
                    _, done, total = msg
                    self.progress_bar["value"] = done
                    self.progress_label.config(text=f"正在处理... {done}/{total}")

                elif kind == "empty":
                    _, unmatched_emp_ids, failed_files = msg
                    self._finish_export_ui()
                    messagebox.showwarning("提示", "未能成功提取到任何有效数据。")
                    self._show_unmatched_emp_warning(unmatched_emp_ids)
                    self._show_failed_files_warning(failed_files)
                    return

                elif kind == "done":
                    _, save_path, unmatched_emp_ids, failed_files = msg
                    self._finish_export_ui()
                    self.last_export_dir = os.path.dirname(save_path)
                    self.btn_open_folder.config(state="normal")
                    self.progress_label.config(text=f"导出完成：{os.path.basename(save_path)}")
                    messagebox.showinfo("保存成功", f"数据已成功保存至:\n{save_path}")
                    self._show_unmatched_emp_warning(unmatched_emp_ids)
                    self._show_failed_files_warning(failed_files)
                    return

                elif kind == "error":
                    _, err_msg = msg
                    self._finish_export_ui()
                    messagebox.showerror("保存失败", f"导出时发生错误:\n{err_msg}")
                    return
        except queue.Empty:
            pass

        # 队列暂时没有新消息，继续轮询
        self.after(100, self._poll_export_queue)

    def _finish_export_ui(self):
        self.btn_save.config(state="normal")

    def _show_failed_files_warning(self, failed_files):
        """若有 json 文件解析失败（不影响其他文件的处理），弹窗提示清单"""
        if not failed_files:
            return
        n = len(failed_files)
        detail_lines = [f"{name}：{err}" for name, err in failed_files[:20]]
        detail = "\n".join(detail_lines)
        if n > 20:
            detail += f"\n...（其余 {n - 20} 个省略）"
        messagebox.showwarning(
            "存在解析失败的文件",
            f"本次共有{n}个 JSON 文件解析失败，已跳过并继续处理其余文件，以下为清单:\n{detail}\n请检查文件格式是否正确。"
        )

    def _show_unmatched_emp_warning(self, unmatched_emp_ids):
        """若已确认的人员名单中存在未能匹配到任何 json 文件的员工号，弹窗提示清单"""
        if not unmatched_emp_ids:
            return
        emp_list_str = ",".join(unmatched_emp_ids)
        n = len(unmatched_emp_ids)
        messagebox.showwarning(
            "存在未匹配员工",
            f"本次共有{n}名员工没有匹配到，以下为清单:\n{emp_list_str}\n请检查员工编号或重新生成简历json重新尝试"
        )

    def _build_summary_text(self, items, start_key, end_key, name_key, extra_keys=None):
        """
        将工作经历/项目经历列表拼接为一列完整文本，格式如下：
        1. 开始时间-结束时间 公司/项目名 [附加字段1] [附加字段2] ...
        2. 开始时间-结束时间 公司/项目名 [附加字段1] [附加字段2] ...
        ...
        每条记录一行，条目之间用换行符分隔。
        extra_keys: 按顺序排列的附加字段 JSON key 列表（如 ["Position","JobDescription"]），
                    传全部字段即可拼出完整汇总，只传部分字段则为自定义合并输出。
        """
        extra_keys = extra_keys or []
        lines = []
        for idx, item in enumerate(items, 1):
            if not isinstance(item, dict):
                continue
            start = item.get(start_key, "") or ""
            end = item.get(end_key, "") or ""
            name = item.get(name_key, "") or ""
            period = f"{start}-{end}" if (start or end) else ""
            parts = [p for p in [period, name] if p]
            for key in extra_keys:
                val = item.get(key, "") or ""
                if val:
                    parts.append(val)
            line = f"{idx}. " + " ".join(parts)
            lines.append(line)
        return "\n".join(lines)

    def _find_value_in_dict(self, data_dict, target_key):
        if target_key in data_dict:
            return data_dict[target_key]
        for key, value in data_dict.items():
            if isinstance(value, dict):
                result = self._find_value_in_dict(value, target_key)
                if result is not None:
                    return result
        return None

if __name__ == "__main__":
    app = JsonExtractorApp()
    app.mainloop()