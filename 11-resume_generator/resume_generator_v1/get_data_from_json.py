import os
import sys
import json
import glob
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkthemes import ThemedTk
import pandas as pd
from datetime import datetime

class JsonExtractorApp(ThemedTk):
    def __init__(self):
        super().__init__(theme="arc")
        
        self.title("简历 JSON 数据提取工具")
        self.geometry("900x650") 
        self.configure(padx=20, pady=20)
        
        if getattr(sys, 'frozen', False):
            self.base_dir = os.path.dirname(sys.executable)
        else:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))
            
        self.current_folder = os.path.join(self.base_dir, "output", "modify_json")
        self.json_files = []
        
        # 界面显示的中文筛选结构
        self.structure = {
            "基本信息": ["工作年限", "毕业时间", "毕业院校", "专业", "最高学历", "部门", "职位", "个人简介"],
            "工作能力": ["业务能力", "资质认证", "培训经历", "技能标签"],
            "附加信息": ["一级部门", "二级部门", "岗位", "职务类别", "专业职级", "公司邮箱", "在职状态", 
                         "入职时间", "首次入职时间", "司龄", "发薪公司", "常驻地", "性别", "出生日期", 
                         "年龄", "政治面貌", "身份证号", "联系电话", "合同法人"],
            "特殊信息": ["学位", "参加工作时间", "教育经历"]
        }
        
        # 中文选项到 JSON 英文键的映射字典
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
            "教育经历": "EducationList"
        }
        
        # EducationList 内部字段的中文映射
        self.edu_inner_mapping = {
            "DegreeType": "学历类型", "GraduationTime": "毕业时间", 
            "GraduationSchool": "毕业院校", "Major": "专业", "HighestEducation": "最高学历"
        }
        
        self.check_vars = {}
        
        self.init_ui()
        self.update_folder(self.current_folder)

    def init_ui(self):
        # --- 目录选择与状态显示区域 ---
        folder_frame = ttk.LabelFrame(self, text=" 目录选择与状态 ")
        folder_frame.pack(fill="x", pady=(0, 15), ipady=10)
        
        self.path_label = ttk.Label(folder_frame, text="当前路径: 未选择", wraplength=550)
        self.path_label.pack(side="left", padx=15, pady=5)
        
        self.count_label = ttk.Label(folder_frame, text="JSON数量: 0", font=("", 10, "bold"), foreground="#0052cc")
        self.count_label.pack(side="left", padx=20, pady=5)
        
        btn_select = ttk.Button(folder_frame, text="选择文件夹", command=self.select_folder)
        btn_select.pack(side="right", padx=15, pady=5)

        # --- 数据筛选区域 ---
        filter_frame = ttk.LabelFrame(self, text=" 数据字段筛选 (人员名称和员工号默认导出) ")
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

        # 构建二级筛选框
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

        # --- 操作按钮区域 ---
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", pady=10)
        
        style.configure("Accent.TButton", font=("", 11, "bold"))
        btn_save = ttk.Button(btn_frame, text="保存提取数据", style="Accent.TButton", command=self.save_data)
        btn_save.pack(side="bottom", ipadx=30, ipady=5)

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
        if not fields_to_extract:
            messagebox.showwarning("未选择数据", "请至少选择一个需要提取的数据字段。")
            return

        parsed_data = []
        
        for file_path in self.json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_content = json.load(f)
            except Exception as e:
                print(f"读取文件失败 {file_path}: {e}")
                continue

            for root_key, person_data in raw_content.items():
                if not isinstance(person_data, dict):
                    continue

                basic_info = person_data.get("BasicInfo", {})
                add_info = person_data.get("AdditionInfo", {})
                
                raw_name = basic_info.get("Name", add_info.get("Name", root_key))
                raw_emp_no = basic_info.get("EmpNo", add_info.get("EmpNo", ""))
                
                # 默认固定字段
                row_data = {
                    "人员名称": raw_name,
                    "员工号": raw_emp_no
                }
                
                for chinese_field in fields_to_extract:
                    en_key = self.key_mapping.get(chinese_field, chinese_field)
                    
                    if en_key == "EducationList":
                        edu_list = person_data.get("SpecialInfo", {}).get("EducationList", [])
                        if isinstance(edu_list, list):
                            for idx, edu_item in enumerate(edu_list, 1):
                                if isinstance(edu_item, dict):
                                    for k, val in edu_item.items():
                                        cn_k = self.edu_inner_mapping.get(k, k)
                                        row_data[f"教育经历{idx}_{cn_k}"] = val
                    else:
                        val = self._find_value_in_dict(person_data, en_key)
                        row_data[chinese_field] = val if val is not None else ""
                        
                parsed_data.append(row_data)

        if not parsed_data:
            messagebox.showwarning("提示", "未能成功提取到任何有效数据。")
            return

        # 转换为 DataFrame，这时的列名本身就是用户选择的二级菜单键
        df = pd.DataFrame(parsed_data)
        
        save_dir = os.path.join(self.base_dir, "数据下载")
        os.makedirs(save_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"已下载_{timestamp}.xlsx"
        save_path = os.path.join(save_dir, filename)

        try:
            # 去除多级表头的逻辑，恢复最基础的 to_excel 导出
            # index=False 表示不写入最左侧的序号，直接写入表头(第1行)和数据(第2行起)
            with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            messagebox.showinfo("保存成功", f"数据已成功保存至:\n{save_path}")
        except Exception as e:
            messagebox.showerror("保存失败", f"导出 Excel 时发生错误:\n{e}")
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