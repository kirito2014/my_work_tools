def process_single_department(source_file_path, target_file_path, output_directory, department_name):
    """处理单个部门的数据"""
    try:
        # 创建输出目录
        output_path = os.path.join(os.path.dirname(source_file_path), output_directory)
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # 生成新文件名
        new_file_name = get_new_file_name(target_file_path, department_name)
        print(f"处理部门: {department_name}, 文件名: {new_file_name}")

        # 删除旧文件并创建新文件
        check_and_remove_file(new_file_name, output_path)
        create_new_file(target_file_path, new_file_name, output_path)

        # 循环处理 cols_mapping 中的 sheet_name
        for sheet_name in cols_mapping.keys():
            filter_col = get_usecols(sheet_name)
            data = filter_department_data(source_file_path, sheet_name=sheet_name, department_name=department_name, usecols=filter_col)
            
            # 写入到目标文件
            target_file = os.path.join(output_path, new_file_name)
            with pd.ExcelWriter(target_file, mode='a', if_sheet_exists='overlay') as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
        
        # 在标题及目录 sheet 中填写部门信息
        write_to_specific_cells(target_file, sheet_name="标题及目录", dp_name=department_name)

    except Exception as e:
        print(f"处理部门 {department_name} 时发生错误: {e}")


class App():
    def __init__(self, root):
        # 现有代码...
        
        self.department_vars = []
        self.department_checkbuttons = []

        # 增加一个获取部门列表的按钮
        ttk.Button(frame, text="获取部门列表", command=self.load_departments, width=20).grid(row=2, column=0, padx=10, pady=5)

        # 增加执行选中部门的按钮
        ttk.Button(frame, text="执行选中部门文件生成", command=self.run_selected_departments, width=30).grid(row=3, column=0, padx=10, pady=5)

    def load_departments(self):
        """从源文件中获取部门列表并生成复选框"""
        source_file_path = self.source_file_var.get()
        if not source_file_path or not Path(source_file_path).is_file():
            self.info_label.config(text="请先选择有效的源文件", foreground="#DB231D")
            return

        departments = get_unique_departments(source_file_path, sheet_name='售前情况统计表', usecols=get_usecols('售前情况统计表'))

        # 清除旧的复选框
        for cb in self.department_checkbuttons:
            cb.grid_forget()
        self.department_vars = []
        self.department_checkbuttons = []

        # 生成复选框
        for i, department in enumerate(departments):
            var = tk.BooleanVar()
            self.department_vars.append(var)
            cb = ttk.Checkbutton(self.root, text=department, variable=var)
            cb.pack(anchor='w')  # 放置到窗口上
            self.department_checkbuttons.append(cb)

    def run_selected_departments(self):
        """执行选中的部门的文件生成"""
        source_file_path = self.source_file_var.get()
        target_file_path = self.template_file_var.get()

        if not source_file_path or not Path(source_file_path).is_file():
            self.info_label.config(text="请选择有效的源文件", foreground="#DB231D")
            return

        if not target_file_path or not Path(target_file_path).is_file():
            self.info_label.config(text="请选择有效的模板文件", foreground="#DB231D")
            return

        selected_departments = [dep for var, dep in zip(self.department_vars, get_unique_departments(source_file_path, sheet_name='售前情况统计表', usecols=get_usecols('售前情况统计表'))) if var.get()]

        if not selected_departments:
            self.info_label.config(text="请选择至少一个部门", foreground="#DB231D")
            return

        # 使用多线程处理选中的部门
        threading.Thread(target=self.process_selected_departments, args=(source_file_path, target_file_path, "售前情况输出", selected_departments)).start()

    def process_selected_departments(self, source_file_path, target_file_path, output_directory, selected_departments):
        """处理选中的部门"""
        try:
            for department in selected_departments:
                process_single_department(source_file_path, target_file_path, output_directory, department)
            self.info_label.config(text="文件生成完成", foreground="#298073")
        except Exception as e:
            self.info_label.config(text=f"处理失败: {e}", foreground="#DB231D")
