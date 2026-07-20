import os
import re
import cv2
import fitz  # PyMuPDF
import queue
import logging
import threading
import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from ttkthemes import ThemedTk
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# 导入极其稳定的 EasyOCR 引擎
import easyocr

try:
    import docx
except ImportError:
    docx = None

class IDCardSemanticProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("身份证智能处理中枢 (EasyOCR 语义制导版)")
        self.root.geometry("950x700")
        self.root.minsize(900, 650)

        self.folder_path = ""
        self.list_file_path = ""
        self.all_persons = {}  
        self.target_ids = []   
        
        self.log_queue = queue.Queue()
        
        self.setup_logging()
        self.build_ui()
        
        # 异步加载 EasyOCR 模型，防止 UI 卡顿
        threading.Thread(target=self.init_ocr_engine, daemon=True).start()
        self.start_log_monitor()

    def init_ocr_engine(self):
        self.write_log("正在加载 EasyOCR 语义引擎 (初次运行可能需要下载模型，请稍候)...", "normal")
        # gpu=False 保证在任何普通办公电脑上都能稳定运行
        self.ocr_reader = easyocr.Reader(['ch_sim', 'en'], gpu=False)
        self.write_log("✅ 语义引擎加载完毕，系统准备就绪！", "info")

    def setup_logging(self):
        os.makedirs("logs", exist_ok=True)
        log_filename = datetime.now().strftime("logs/semantic_process_%Y%m%d_%H%M%S.txt")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )

    def build_ui(self):
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        folder_frame = ttk.LabelFrame(main_frame, text="第一步：选择包含身份证(图片/PDF/Word)的文件夹", padding=15)
        folder_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_folder = ttk.Button(folder_frame, text="浏览文件夹...", command=self.select_folder)
        self.btn_select_folder.pack(side=tk.LEFT, padx=(0, 15))
        self.lbl_folder_path = ttk.Label(folder_frame, text="尚未选择文件夹", foreground="gray")
        self.lbl_folder_path.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.lbl_person_count = ttk.Label(folder_frame, text="识别数量: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_person_count.pack(side=tk.RIGHT, padx=10)

        list_frame = ttk.LabelFrame(main_frame, text="第二步：导入人员名单 (可选)", padding=15)
        list_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_list = ttk.Button(list_frame, text="导入名单(Excel/Txt)", command=self.select_list_file)
        self.btn_select_list.pack(side=tk.LEFT, padx=(0, 15))
        self.lbl_list_path = ttk.Label(list_frame, text="未导入名单（默认处理文件夹下所有）", foreground="gray")
        self.lbl_list_path.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.lbl_target_count = ttk.Label(list_frame, text="待处理: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_target_count.pack(side=tk.RIGHT, padx=10)

        log_frame = ttk.LabelFrame(main_frame, text="执行日志", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 15))

        self.log_text = tk.Text(log_frame, wrap=tk.WORD, state=tk.DISABLED, font=("Consolas", 9))
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text.tag_config("info", foreground="#2ca02c")
        self.log_text.tag_config("error", foreground="#d62728")
        self.log_text.tag_config("warning", foreground="#ff7f0e")
        self.log_text.tag_config("normal", foreground="#333333")

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)

        self.btn_exit = ttk.Button(btn_frame, text="退 出", command=self.root.quit)
        self.btn_exit.pack(side=tk.RIGHT, padx=15)

        self.btn_merge = ttk.Button(btn_frame, text="标准合并 (排版A4)", command=self.start_merge_thread, style="Accent.TButton")
        self.btn_merge.pack(side=tk.RIGHT, ipadx=10, ipady=5)

        self.btn_split = ttk.Button(btn_frame, text="混合件语义拆分", command=self.confirm_split)
        self.btn_split.pack(side=tk.RIGHT, padx=15, ipadx=10, ipady=5)

        self.progress_bar = ttk.Progressbar(btn_frame, mode='determinate')
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 20))

    def select_folder(self):
        folder = filedialog.askdirectory(title="请选择文件所在目录")
        if folder:
            self.folder_path = folder
            self.lbl_folder_path.config(text=folder, foreground="black")
            self.parse_folder()

    def select_list_file(self):
        file_path = filedialog.askopenfilename(title="选择人员名单", filetypes=[("表格/文本", "*.xlsx *.xls *.txt")])
        if file_path:
            self.list_file_path = file_path
            self.lbl_list_path.config(text=file_path, foreground="black")
            self.parse_list_file()

    def parse_folder(self):
        self.all_persons.clear()
        if not os.path.exists(self.folder_path): return
        pattern = re.compile(r'^(\d{5})[\s\+]*(.*?)[\s\+]*(?:身份证|合并).*?\.([a-zA-Z0-9]+)$', re.IGNORECASE)

        for filename in os.listdir(self.folder_path):
            match = pattern.match(filename)
            if match:
                person_id = match.group(1)
                # 严格清理姓名，确保没有空格和加号
                name = match.group(2).strip().replace(" ", "").replace("+", "")
                full_path = os.path.join(self.folder_path, filename)

                if person_id not in self.all_persons: self.all_persons[person_id] = {'name': name}

                if "人像面" in filename: self.all_persons[person_id]['front'] = full_path
                elif "国徽面" in filename: self.all_persons[person_id]['back'] = full_path
                else: self.all_persons[person_id]['mixed'] = full_path

        count = len(self.all_persons)
        self.lbl_person_count.config(text=f"去重识别: {count} 人")
        self.write_log(f"扫描完成，识别到 {count} 位人员", "info")
        if not self.target_ids: self.lbl_target_count.config(text=f"待处理: {count} (全部)")

    def parse_list_file(self):
        try:
            ids = []
            if self.list_file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(self.list_file_path, usecols=[0], dtype=str)
                ids = df.iloc[:, 0].dropna().str.strip().str.zfill(5).tolist()
            elif self.list_file_path.endswith('.txt'):
                with open(self.list_file_path, 'r', encoding='utf-8') as f:
                    ids = [x.strip().zfill(5) for x in re.split(r'[,;\n]+', f.read()) if x.strip()]

            self.target_ids = list(dict.fromkeys(ids))
            count = len(self.target_ids)
            self.lbl_target_count.config(text=f"待处理: {count}")
            self.write_log(f"名单导入成功，共 {count} 人", "info")
        except Exception as e:
            self.write_log(f"名单解析失败: {str(e)}", "error")

    # ================= ★ 全新语义制导核心架构 ★ =================

    def semantic_crop_and_orient(self, img_array):
        """
        【核心算法】：完全无视背景，通过文字群反向推导物理边框，并精准回正。
        """
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None: return None, "unknown"
        
        # 1. 执行全图 OCR 获取文本框
        results = self.ocr_reader.readtext(img)
        if not results:
            return img, "unknown" # 没字，原样返回

        # 2. 找到所有文字的全局边界框
        x_coords = []
        y_coords = []
        full_text = ""
        for bbox, text, prob in results:
            full_text += text
            for p in bbox:
                x_coords.append(p[0])
                y_coords.append(p[1])

        min_x, max_x = min(x_coords), max(x_coords)
        min_y, max_y = min(y_coords), max(y_coords)

        # 3. 语义分类定性：是人像面还是国徽面？
        card_type = "unknown"
        if any(kw in full_text for kw in ['姓名', '性别', '民族', '出生', '住址', '身份号码']):
            card_type = "front"
        elif any(kw in full_text for kw in ['中华', '人民', '共和国', '居民', '签发', '期限']):
            card_type = "back"

        # 4. 反向推导扩展裁剪 (向外扩 15% 包含物理边缘)
        w = max_x - min_x
        h = max_y - min_y
        pad_x = int(w * 0.15)
        pad_y = int(h * 0.15)

        img_h, img_w = img.shape[:2]
        crop_x1 = max(0, int(min_x - pad_x))
        crop_y1 = max(0, int(min_y - pad_y))
        crop_x2 = min(img_w, int(max_x + pad_x))
        crop_y2 = min(img_h, int(max_y + pad_y))

        cropped_img = img[crop_y1:crop_y2, crop_x1:crop_x2]

        # 5. 绝对锚点定向：彻底解决颠倒问题
        c_h, c_w = cropped_img.shape[:2]
        needs_180_rotation = False
        
        # 通过二次局部精准识别找锚点词的中心Y坐标
        crop_results = self.ocr_reader.readtext(cropped_img)
        for bbox, text, prob in crop_results:
            center_y = sum([p[1] for p in bbox]) / 4
            
            # 如果是正面，"姓名"等词本应在顶部。若跑到了下半部，必是倒立。
            if card_type == 'front' and any(kw in text for kw in ['名', '族', '男', '女']):
                if center_y > c_h * 0.55: needs_180_rotation = True
                break
            # 如果是反面，"中华"等词本应在顶部。
            if card_type == 'back' and any(kw in text for kw in ['中', '华', '人', '民']):
                if center_y > c_h * 0.55: needs_180_rotation = True
                break

        if needs_180_rotation:
            cropped_img = cv2.rotate(cropped_img, cv2.ROTATE_180)

        # 6. 强制放平并标准化为 1011 x 638
        ch, cw = cropped_img.shape[:2]
        if ch > cw: # 如果是竖向的，旋转 90 度放平
            cropped_img = cv2.rotate(cropped_img, cv2.ROTATE_90_CLOCKWISE)
            
        std_img = cv2.resize(cropped_img, (1011, 638), interpolation=cv2.INTER_CUBIC)
        return std_img, card_type


    def extract_mixed_document(self, img_array):
        """处理一图中包含正反两面的情况 (例如扫描全能王生成的A4纸)"""
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None: return None, None

        # 无论版式多乱，最安全的办法就是把它先居中劈开，然后送入语义引擎各自独立推导
        h, w = img.shape[:2]
        if h > w: 
            part1 = img[0:h//2, 0:w]
            part2 = img[h//2:h, 0:w]
        else:       
            part1 = img[0:h, 0:w//2]
            part2 = img[0:h, w//2:w]

        front_std, back_std = None, None

        for part in [part1, part2]:
            _, part_buffer = cv2.imencode('.jpg', part)
            std_img, c_type = self.semantic_crop_and_orient(np.array(part_buffer))
            
            if c_type == 'front': front_std = std_img
            elif c_type == 'back': back_std = std_img

        return front_std, back_std

    # ================= 格式解析路由 =================

    def process_pdf(self, pdf_path):
        doc = fitz.open(pdf_path)
        if len(doc) == 0: return None, None
        
        images = []
        for page_num in range(min(2, len(doc))):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(dpi=200) 
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            if pix.n == 4: img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2RGB)
            elif pix.n == 3: img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            _, buffer = cv2.imencode('.jpg', img_array)
            images.append(np.array(buffer))
        doc.close()

        if len(images) == 1:
            return self.extract_mixed_document(images[0])
        elif len(images) >= 2:
            front_std, back_std = None, None
            w1, t1 = self.semantic_crop_and_orient(images[0])
            w2, t2 = self.semantic_crop_and_orient(images[1])
            
            if t1 == 'front': front_std = w1
            if t1 == 'back': back_std = w1
            if t2 == 'front': front_std = w2
            if t2 == 'back': back_std = w2
            return front_std, back_std

    def process_docx(self, docx_path):
        if docx is None: raise ImportError("系统未安装 python-docx")
        try: doc = docx.Document(docx_path)
        except: return None, None

        images = []
        for rel in doc.part.rels.values():
            if "image" in rel.target_ref:
                img_data = np.frombuffer(rel.target_part.blob, dtype=np.uint8)
                tmp = cv2.imdecode(img_data, cv2.IMREAD_COLOR)
                if tmp is not None and tmp.shape[0] > 100:
                    images.append(img_data)

        if len(images) == 0: return None, None
        elif len(images) == 1: return self.extract_mixed_document(images[0])
        else:
            w1, t1 = self.semantic_crop_and_orient(images[0])
            w2, t2 = self.semantic_crop_and_orient(images[1])
            front_std, back_std = None, None
            if t1 == 'front': front_std = w1
            if t1 == 'back': back_std = w1
            if t2 == 'front': front_std = w2
            if t2 == 'back': back_std = w2
            return front_std, back_std

    # ================= 业务执行逻辑 =================

    def process_merge_single_person(self, person_id, output_dir):
        if not hasattr(self, 'ocr_reader'): return False
        
        person_info = self.all_persons[person_id]
        name = person_info['name']

        if 'front' not in person_info or 'back' not in person_info:
            self.write_log(f"[{person_id}+{name}] 略过合并：缺失单面源文件", "warning")
            return False

        try:
            # 读取单面原图，过一遍语义引擎（自动去背景、扶正、裁剪）
            with open(person_info['front'], 'rb') as f:
                w_front, _ = self.semantic_crop_and_orient(np.frombuffer(f.read(), dtype=np.uint8))
            with open(person_info['back'], 'rb') as f:
                w_back, _ = self.semantic_crop_and_orient(np.frombuffer(f.read(), dtype=np.uint8))
            
            if w_front is None or w_back is None:
                raise ValueError("语义引擎提取图像实体失败")

            front_pil = Image.fromarray(cv2.cvtColor(w_front, cv2.COLOR_BGR2RGB))
            back_pil = Image.fromarray(cv2.cvtColor(w_back, cv2.COLOR_BGR2RGB))

            a4_canvas = Image.new('RGB', (2480, 3508), color='white')
            target_w, target_h, gap = 1011, 638, 150
            start_y = (3508 - (target_h * 2 + gap)) // 2
            x_offset = (2480 - target_w) // 2
            
            a4_canvas.paste(front_pil, (x_offset, start_y))
            a4_canvas.paste(back_pil, (x_offset, start_y + target_h + gap))

            # 严苛的命名规则：全加号，无空格
            save_name = f"{person_id}+{name}+身份证（合并页）.jpg"
            a4_canvas.save(os.path.join(output_dir, save_name), "JPEG", quality=95)
            self.write_log(f"[{person_id}+{name}] 合并排版成功", "info")
            return True
        except Exception as e:
            self.write_log(f"[{person_id}+{name}] 合并失败: {str(e)}", "error")
            return False

    def confirm_split(self):
        if not self.folder_path:
            messagebox.showwarning("提示", "请先选择文件夹")
            return
        if messagebox.askyesno("拆分确认", "确认启动语义引擎提取并拆分文件吗？"):
            self.start_split_thread()

    def process_split_single_person(self, person_id, output_dir):
        if not hasattr(self, 'ocr_reader'): return False
        
        person_info = self.all_persons[person_id]
        name = person_info['name']
        if 'mixed' not in person_info: return False 

        try:
            mixed_path = person_info['mixed']
            f_cv, b_cv = None, None

            if mixed_path.lower().endswith('.pdf'):
                f_cv, b_cv = self.process_pdf(mixed_path)
            elif mixed_path.lower().endswith('.docx'):
                f_cv, b_cv = self.process_docx(mixed_path)
            else:
                with open(mixed_path, 'rb') as f:
                    f_cv, b_cv = self.extract_mixed_document(np.frombuffer(f.read(), dtype=np.uint8))

            if f_cv is None or b_cv is None:
                raise ValueError("文件模糊或格式不支持，未能完整提取出两个版面")

            front_pil = Image.fromarray(cv2.cvtColor(f_cv, cv2.COLOR_BGR2RGB))
            back_pil = Image.fromarray(cv2.cvtColor(b_cv, cv2.COLOR_BGR2RGB))

            # 严苛的命名规则：全加号，无空格
            front_name = f"{person_id}+{name}+身份证（人像面）.jpg"
            back_name = f"{person_id}+{name}+身份证（国徽面）.jpg"

            front_pil.save(os.path.join(output_dir, front_name), "JPEG", quality=95)
            back_pil.save(os.path.join(output_dir, back_name), "JPEG", quality=95)

            self.write_log(f"[{person_id}+{name}] 语义制导拆分成功", "info")
            return True
        except Exception as e:
            self.write_log(f"[{person_id}+{name}] 拆分失败: {str(e)}", "error")
            return False

    # ================= UI 锁与多线程调度 =================

    def lock_ui(self):
        self.btn_merge.config(state=tk.DISABLED)
        self.btn_split.config(state=tk.DISABLED)
        self.btn_select_folder.config(state=tk.DISABLED)
        self.btn_select_list.config(state=tk.DISABLED)

    def unlock_ui(self):
        self.btn_merge.config(state=tk.NORMAL)
        self.btn_split.config(state=tk.NORMAL)
        self.btn_select_folder.config(state=tk.NORMAL)
        self.btn_select_list.config(state=tk.NORMAL)

    def start_merge_thread(self):
        if not self.folder_path: return
        p_ids = self.target_ids if self.target_ids else list(self.all_persons.keys())
        if not p_ids: return

        out_dir = os.path.join(self.folder_path, "已合并输出")
        os.makedirs(out_dir, exist_ok=True)
        self.lock_ui()
        self.progress_bar.config(maximum=len(p_ids), value=0)
        self.write_log("========== 开始标准合并任务 ==========", "normal")
        threading.Thread(target=self._run_task, args=(p_ids, out_dir, "合并"), daemon=True).start()

    def start_split_thread(self):
        p_ids = self.target_ids if self.target_ids else list(self.all_persons.keys())
        if not p_ids: return

        out_dir = os.path.join(self.folder_path, "已拆分输出")
        os.makedirs(out_dir, exist_ok=True)
        self.lock_ui()
        self.progress_bar.config(maximum=len(p_ids), value=0)
        self.write_log("========== 开始语义提取拆分任务 ==========", "normal")
        threading.Thread(target=self._run_task, args=(p_ids, out_dir, "拆分"), daemon=True).start()

    def _run_task(self, ids, out_dir, task_type):
        success = 0
        # EasyOCR 使用单线程排队即可跑满 CPU，且绝不闪退崩溃
        with ThreadPoolExecutor(max_workers=1) as exe:
            target_func = self.process_merge_single_person if task_type == "合并" else self.process_split_single_person
            futures = {exe.submit(target_func, i, out_dir): i for i in ids}
            for f in as_completed(futures):
                try:
                    if f.result(): success += 1
                except Exception as e:
                    self.write_log(f"任务异常被拦截: {str(e)}", "error")
                finally:
                    self.log_queue.put(('progress', 1))
        
        self.log_queue.put(('finish', (success, task_type)))

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
                    success_count, task_type = d
                    self.unlock_ui()
                    msg = f"任务结束！成功{task_type} {success_count} 份"
                    self.write_log(f"========== {msg} ==========\n", "normal")
                    messagebox.showinfo(f"{task_type}完成", msg)
        except queue.Empty: pass
        finally: self.root.after(100, self.start_log_monitor)

if __name__ == "__main__":
    root = ThemedTk(theme="arc")
    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except: pass
    IDCardSemanticProcessorApp(root)
    root.mainloop()