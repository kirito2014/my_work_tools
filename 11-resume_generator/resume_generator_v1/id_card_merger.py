import os
import re
import queue
import logging
import threading
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from ttkthemes import ThemedTk
from PIL import Image, ImageOps
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class IDCardMergerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("身份证图片智能排版合并工具 (紧凑版)")
        self.root.geometry("850x650")
        self.root.minsize(800, 600)

        # 核心数据状态
        self.folder_path = ""
        self.list_file_path = ""
        self.all_persons = {}  # 结构: {id: {'name': name, 'front': path, 'back': path}}
        self.target_ids = []   # 需要合并的人员ID列表
        
        # 线程安全日志队列
        self.log_queue = queue.Queue()
        
        self.setup_logging()
        self.build_ui()
        self.start_log_monitor()

    def setup_logging(self):
        """配置本地文件日志记录"""
        os.makedirs("logs", exist_ok=True)
        log_filename = datetime.now().strftime("logs/merge_log_%Y%m%d_%H%M%S.txt")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )

    def build_ui(self):
        """构建图形界面"""
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # ================= 1. 文件夹选择区域 =================
        folder_frame = ttk.LabelFrame(main_frame, text="第一步：选择图片所在文件夹", padding=15)
        folder_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_folder = ttk.Button(folder_frame, text="浏览文件夹...", command=self.select_folder)
        self.btn_select_folder.pack(side=tk.LEFT, padx=(0, 15))

        self.lbl_folder_path = ttk.Label(folder_frame, text="尚未选择文件夹", foreground="gray")
        self.lbl_folder_path.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.lbl_person_count = ttk.Label(folder_frame, text="去重人员数量: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_person_count.pack(side=tk.RIGHT, padx=10)

        # ================= 2. 名单导入区域 =================
        list_frame = ttk.LabelFrame(main_frame, text="第二步：导入人员名单 (可选)", padding=15)
        list_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_list = ttk.Button(list_frame, text="导入名单(Excel/Txt)", command=self.select_list_file)
        self.btn_select_list.pack(side=tk.LEFT, padx=(0, 15))

        self.lbl_list_path = ttk.Label(list_frame, text="未导入名单（默认合并文件夹下所有人员）", foreground="gray")
        self.lbl_list_path.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.lbl_target_count = ttk.Label(list_frame, text="待合并数量: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_target_count.pack(side=tk.RIGHT, padx=10)

        # ================= 3. 日志展示区域 =================
        log_frame = ttk.LabelFrame(main_frame, text="执行日志", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9))
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text.tag_config("info", foreground="#2ca02c")    # 绿色
        self.log_text.tag_config("error", foreground="#d62728")   # 红色
        self.log_text.tag_config("warning", foreground="#ff7f0e") # 橙色
        self.log_text.tag_config("normal", foreground="#333333")  # 深灰

        # ================= 4. 操作按钮区域 =================
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)

        self.btn_merge = ttk.Button(btn_frame, text="开始合并", command=self.start_merge_thread, style="Accent.TButton")
        self.btn_merge.pack(side=tk.RIGHT, ipadx=20, ipady=5)

        self.btn_exit = ttk.Button(btn_frame, text="退 出", command=self.root.quit)
        self.btn_exit.pack(side=tk.RIGHT, padx=15)

        self.progress_bar = ttk.Progressbar(btn_frame, mode='determinate')
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 20))

    # ------------------- 业务逻辑功能 -------------------

    def select_folder(self):
        folder = filedialog.askdirectory(title="请选择包含身份证图片的文件夹")
        if folder:
            self.folder_path = folder
            self.lbl_folder_path.config(text=folder, foreground="black")
            self.parse_folder()

    def select_list_file(self):
        file_path = filedialog.askopenfilename(
            title="选择人员名单",
            filetypes=[("Excel或Txt文件", "*.xlsx *.xls *.txt")]
        )
        if file_path:
            self.list_file_path = file_path
            self.lbl_list_path.config(text=file_path, foreground="black")
            self.parse_list_file()

    def parse_folder(self):
        self.all_persons.clear()
        if not os.path.exists(self.folder_path):
            return

        # 匹配逻辑：5位编号+姓名+身份证(人像面/国徽面).jpg
        pattern = re.compile(r'^(\d{5})\s*(.*?)\s*身份证\s*[（\(](人像面|国徽面)[）\)]\.(jpg|jpeg|png)$', re.IGNORECASE)

        for filename in os.listdir(self.folder_path):
            match = pattern.match(filename)
            if match:
                person_id = match.group(1)
                name = match.group(2).strip()
                side = match.group(3)
                
                if person_id not in self.all_persons:
                    self.all_persons[person_id] = {'name': name}
                
                full_path = os.path.join(self.folder_path, filename)
                if side == "人像面":
                    self.all_persons[person_id]['front'] = full_path
                else:
                    self.all_persons[person_id]['back'] = full_path

        count = len(self.all_persons)
        self.lbl_person_count.config(text=f"去重人员数量: {count}")
        self.write_log(f"扫描完成，识别到 {count} 位人员", "info")
        
        if not self.target_ids:
            self.lbl_target_count.config(text=f"待合并数量: {count} (全部)")

    def parse_list_file(self):
        try:
            ids = []
            if self.list_file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(self.list_file_path, usecols=[0], dtype=str)
                ids = df.iloc[:, 0].dropna().str.strip().str.zfill(5).tolist()
            elif self.list_file_path.endswith('.txt'):
                with open(self.list_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    raw_ids = re.split(r'[,;\n]+', content)
                    ids = [x.strip().zfill(5) for x in raw_ids if x.strip()]

            self.target_ids = list(dict.fromkeys(ids))
            count = len(self.target_ids)
            self.lbl_target_count.config(text=f"待合并数量: {count}")
            self.write_log(f"名单导入成功，共 {count} 人", "info")

        except Exception as e:
            self.write_log(f"名单解析失败: {str(e)}", "error")

    # ------------------- 核心图像处理 -------------------

    def get_safe_id_image(self, image_path, target_width=1011, target_height=638):
        """等比例缩放并居中留白，确保不拉伸不裁剪"""
        try:
            with open(image_path, 'rb') as f:
                img = Image.open(f)
                img.load()
                img = img.convert('RGB')

            # 缩放至目标框内
            img_resized = ImageOps.contain(img, (target_width, target_height), Image.Resampling.LANCZOS)
            background = Image.new('RGB', (target_width, target_height), (255, 255, 255))
            
            offset_x = (target_width - img_resized.width) // 2
            offset_y = (target_height - img_resized.height) // 2
            background.paste(img_resized, (offset_x, offset_y))
            return background
        except Exception as e:
            raise Exception(f"处理异常: {str(e)}")

    def process_single_person(self, person_id, output_dir):
        if person_id not in self.all_persons:
            self.write_log(f"[{person_id}] 缺失文件", "error")
            return False

        person_info = self.all_persons[person_id]
        name = person_info['name']
        front_p, back_p = person_info.get('front'), person_info.get('back')

        if not front_p or not back_p:
            self.write_log(f"[{person_id} {name}] 缺少单面图片", "error")
            return False

        try:
            # 标准尺寸与间距设置
            target_w, target_h = 1011, 638
            vertical_gap = 150  # 【核心修改：缩小上下间隔至150像素】
            
            img_front = self.get_safe_id_image(front_p, target_w, target_h)
            img_back = self.get_safe_id_image(back_p, target_w, target_h)

            # A4画布 (300DPI: 2480x3508)
            a4_canvas = Image.new('RGB', (2480, 3508), color='white')

            # 计算排版坐标 (整体垂直居中)
            total_content_h = (target_h * 2) + vertical_gap
            start_y = (3508 - total_content_h) // 2
            x_offset = (2480 - target_w) // 2
            
            # 粘贴人像面
            a4_canvas.paste(img_front, (x_offset, start_y))
            # 粘贴国徽面
            a4_canvas.paste(img_back, (x_offset, start_y + target_h + vertical_gap))

            # 保存
            save_name = f"{person_id}{name}身份证（合并页）.jpg"
            a4_canvas.save(os.path.join(output_dir, save_name), "JPEG", quality=95)
            self.write_log(f"[{person_id} {name}] 合并成功", "info")
            return True
        except Exception as e:
            self.write_log(f"[{person_id} {name}] 错误: {str(e)}", "error")
            return False

    # ------------------- 进程控制 -------------------

    def start_merge_thread(self):
        if not self.folder_path:
            messagebox.showwarning("提示", "请选择文件夹")
            return

        p_ids = self.target_ids if self.target_ids else list(self.all_persons.keys())
        output_dir = os.path.join(self.folder_path, "已合并输出")
        os.makedirs(output_dir, exist_ok=True)

        self.btn_merge.config(state=tk.DISABLED)
        self.progress_bar['maximum'] = len(p_ids)
        self.progress_bar['value'] = 0
        
        self.write_log("开始任务...", "normal")
        threading.Thread(target=self._run_task, args=(p_ids, output_dir), daemon=True).start()

    def _run_task(self, ids, out_dir):
        success = 0
        with ThreadPoolExecutor(max_workers=4) as exe:
            futures = {exe.submit(self.process_single_person, i, out_dir): i for i in ids}
            for f in as_completed(futures):
                if f.result(): success += 1
                self.log_queue.put(('progress', 1))
        self.log_queue.put(('finish', success))

    def write_log(self, message, level="normal"):
        self.log_queue.put(('log', (message, level)))

    def start_log_monitor(self):
        try:
            while True:
                t, d = self.log_queue.get_nowait()
                if t == 'log':
                    msg, lv = d
                    self.log_text.config(state=tk.NORMAL)
                    self.log_text.insert(tk.END, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n", lv)
                    self.log_text.see(tk.END)
                    self.log_text.config(state=tk.DISABLED)
                elif t == 'progress':
                    self.progress_bar['value'] += d
                elif t == 'finish':
                    self.btn_merge.config(state=tk.NORMAL)
                    messagebox.showinfo("完成", f"成功合并 {d} 份")
        except queue.Empty: pass
        finally: self.root.after(100, self.start_log_monitor)

if __name__ == "__main__":
    root = ThemedTk(theme="arc")
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except: pass
    IDCardMergerApp(root)
    root.mainloop()