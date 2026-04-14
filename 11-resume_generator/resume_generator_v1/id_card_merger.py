import os
import re
import cv2
import fitz  # PyMuPDF，用于处理PDF
import queue
import logging
import threading
import numpy as np
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
        self.root.title("身份证图片智能排版合并工具 (AI视觉校正版)")
        self.root.geometry("900x700")
        self.root.minsize(850, 650)

        # 核心数据状态
        self.folder_path = ""
        self.list_file_path = ""
        # 结构: {id: {'name': name, 'front': path, 'back': path, 'mixed': path}}
        self.all_persons = {}  
        self.target_ids = []   
        
        self.log_queue = queue.Queue()
        
        # 初始化人脸分类器（用于自动区分正反面）
        self.face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

        self.setup_logging()
        self.build_ui()
        self.start_log_monitor()

    def setup_logging(self):
        os.makedirs("logs", exist_ok=True)
        log_filename = datetime.now().strftime("logs/merge_log_%Y%m%d_%H%M%S.txt")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )

    def build_ui(self):
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 1. 文件夹选择
        folder_frame = ttk.LabelFrame(main_frame, text="第一步：选择包含身份证(图片/PDF)的文件夹", padding=15)
        folder_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_folder = ttk.Button(folder_frame, text="浏览文件夹...", command=self.select_folder)
        self.btn_select_folder.pack(side=tk.LEFT, padx=(0, 15))

        self.lbl_folder_path = ttk.Label(folder_frame, text="尚未选择文件夹", foreground="gray")
        self.lbl_folder_path.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.lbl_person_count = ttk.Label(folder_frame, text="识别人员数量: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_person_count.pack(side=tk.RIGHT, padx=10)

        # 2. 名单导入
        list_frame = ttk.LabelFrame(main_frame, text="第二步：导入人员名单 (可选)", padding=15)
        list_frame.pack(fill=tk.X, pady=(0, 15))

        self.btn_select_list = ttk.Button(list_frame, text="导入名单(Excel/Txt)", command=self.select_list_file)
        self.btn_select_list.pack(side=tk.LEFT, padx=(0, 15))

        self.lbl_list_path = ttk.Label(list_frame, text="未导入名单（默认合并文件夹下所有人员）", foreground="gray")
        self.lbl_list_path.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.lbl_target_count = ttk.Label(list_frame, text="待合并数量: 0", font=("Microsoft YaHei", 10, "bold"))
        self.lbl_target_count.pack(side=tk.RIGHT, padx=10)

        # 3. 日志
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

        # 4. 按钮
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X)

        self.btn_merge = ttk.Button(btn_frame, text="智能校正并合并", command=self.start_merge_thread, style="Accent.TButton")
        self.btn_merge.pack(side=tk.RIGHT, ipadx=20, ipady=5)

        self.btn_exit = ttk.Button(btn_frame, text="退 出", command=self.root.quit)
        self.btn_exit.pack(side=tk.RIGHT, padx=15)

        self.progress_bar = ttk.Progressbar(btn_frame, mode='determinate')
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 20))

    # ------------------- 文件解析逻辑 -------------------

    def select_folder(self):
        folder = filedialog.askdirectory(title="请选择文件所在目录")
        if folder:
            self.folder_path = folder
            self.lbl_folder_path.config(text=folder, foreground="black")
            self.parse_folder()

    def select_list_file(self):
        file_path = filedialog.askopenfilename(title="选择人员名单", filetypes=[("表格或文本", "*.xlsx *.xls *.txt")])
        if file_path:
            self.list_file_path = file_path
            self.lbl_list_path.config(text=file_path, foreground="black")
            self.parse_list_file()

    def parse_folder(self):
        self.all_persons.clear()
        if not os.path.exists(self.folder_path): return

        # 宽泛正则：支持 5位编号 + 姓名 + 任意后缀(jpg,png,pdf)
        pattern = re.compile(r'^(\d{5})\s*(.*?)\s*(?:身份证|合并).*?\.([a-zA-Z0-9]+)$', re.IGNORECASE)

        for filename in os.listdir(self.folder_path):
            match = pattern.match(filename)
            if match:
                person_id = match.group(1)
                name = match.group(2).strip()
                ext = match.group(3).lower()
                full_path = os.path.join(self.folder_path, filename)

                if person_id not in self.all_persons:
                    self.all_persons[person_id] = {'name': name}

                # 判断类型
                if "人像面" in filename:
                    self.all_persons[person_id]['front'] = full_path
                elif "国徽面" in filename:
                    self.all_persons[person_id]['back'] = full_path
                else:
                    # 只要没有明确区分正反，一律按混合文件(mixed)处理，包括 PDF 和拼接图
                    self.all_persons[person_id]['mixed'] = full_path

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

    # ------------------- 核心视觉处理算法 -------------------

    def order_points(self, pts):
        """数学辅助：将四个坐标点按左上、右上、右下、左下的顺序排列"""
        rect = np.zeros((4, 2), dtype="float32")
        s = pts.sum(axis=1)
        rect[0] = pts[np.argmin(s)]
        rect[2] = pts[np.argmax(s)]
        diff = np.diff(pts, axis=1)
        rect[1] = pts[np.argmin(diff)]
        rect[3] = pts[np.argmax(diff)]
        return rect

    def four_point_transform(self, image, pts):
        """核心视觉算法：透视变换（拍平倾斜的卡片）"""
        rect = self.order_points(pts)
        (tl, tr, br, bl) = rect
        widthA = np.sqrt(((br[0] - bl[0]) ** 2) + ((br[1] - bl[1]) ** 2))
        widthB = np.sqrt(((tr[0] - tl[0]) ** 2) + ((tr[1] - tl[1]) ** 2))
        maxWidth = max(int(widthA), int(widthB))
        heightA = np.sqrt(((tr[0] - br[0]) ** 2) + ((tr[1] - br[1]) ** 2))
        heightB = np.sqrt(((tl[0] - bl[0]) ** 2) + ((tl[1] - bl[1]) ** 2))
        maxHeight = max(int(heightA), int(heightB))

        dst = np.array([[0, 0], [maxWidth - 1, 0], [maxWidth - 1, maxHeight - 1], [0, maxHeight - 1]], dtype="float32")
        M = cv2.getPerspectiveTransform(rect, dst)
        warped = cv2.warpPerspective(image, M, (maxWidth, maxHeight))
        return warped

    def classify_front_back(self, img1, img2):
        """通过人脸识别区分人像面和国徽面"""
        def has_face(img):
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            # 提高识别宽容度
            faces = self.face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=3, minSize=(30, 30))
            return len(faces) > 0

        # 如果img1有人脸，它就是front
        if has_face(img1): return img1, img2
        if has_face(img2): return img2, img1
        # 如果都没识别出，默认原顺序
        return img1, img2

    def extract_cards_from_image(self, img_array):
        """从一张复杂的图像中提取两张身份证，支持倾斜校正或智能切割"""
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None: return None, None

        # 1. 尝试通过轮廓寻找两张卡片（对付不规则拍照/倾斜）
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (5, 5), 0)
        edged = cv2.Canny(blur, 50, 150)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
        closed = cv2.morphologyEx(edged, cv2.MORPH_CLOSE, kernel)

        cnts, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        cnts = sorted(cnts, key=cv2.contourArea, reverse=True)

        cards = []
        img_area = img.shape[0] * img.shape[1]

        for c in cnts:
            # 过滤掉太小的噪点
            if cv2.contourArea(c) < img_area * 0.05: continue
            peri = cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, 0.02 * peri, True)
            if len(approx) == 4: # 找到四边形
                cards.append(self.four_point_transform(img, approx.reshape(4, 2)))
            if len(cards) == 2: break

        # 2. 如果没能找到两个明显的边框（比如完美无缝拼接到一起的图）
        if len(cards) < 2:
            h, w = img.shape[:2]
            # 启发式分割：比较宽高比决定横切还是竖切
            if h > w: # 竖向长图，上下切
                cards = [img[0:h//2, 0:w], img[h//2:h, 0:w]]
            else:     # 横向长图，左右切
                cards = [img[0:h, 0:w//2], img[0:h, w//2:w]]

        if len(cards) < 2: return None, None # 极端失败情况

        # 3. 人脸识别分类正反面
        front_cv, back_cv = self.classify_front_back(cards[0], cards[1])

        # 转换回 PIL Image 供后续排版
        front_pil = Image.fromarray(cv2.cvtColor(front_cv, cv2.COLOR_BGR2RGB))
        back_pil = Image.fromarray(cv2.cvtColor(back_cv, cv2.COLOR_BGR2RGB))
        return front_pil, back_pil

    def process_pdf_to_images(self, pdf_path):
        """处理PDF文件，返回正反面 PIL Image"""
        doc = fitz.open(pdf_path)
        if len(doc) == 0: return None, None
        
        images = []
        # 提取前两页（或者第一页里的图）
        for page_num in range(min(2, len(doc))):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(dpi=300)
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            # RGBA 转 RGB
            if pix.n == 4:
                img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2RGB)
            elif pix.n == 3:
                img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
                
            # 编码成标准 numpy buffer 以复用流程
            _, buffer = cv2.imencode('.png', img_array)
            images.append(np.array(buffer))

        doc.close()

        # 如果 PDF 只有一页，假设这一页里拼了两张图
        if len(images) == 1:
            return self.extract_cards_from_image(images[0])
        # 如果 PDF 有两页，分别作为两张图处理
        elif len(images) >= 2:
            img1 = cv2.imdecode(images[0], cv2.IMREAD_COLOR)
            img2 = cv2.imdecode(images[1], cv2.IMREAD_COLOR)
            front_cv, back_cv = self.classify_front_back(img1, img2)
            return Image.fromarray(cv2.cvtColor(front_cv, cv2.COLOR_BGR2RGB)), Image.fromarray(cv2.cvtColor(back_cv, cv2.COLOR_BGR2RGB))

    # ------------------- 格式化排版引擎 -------------------

    def format_to_a4(self, img_front, img_back, person_id, name, output_dir):
        """接收两张PIL Image对象，排版为A4并保存"""
        target_w, target_h = 1011, 638
        vertical_gap = 150

        # 安全缩放
        img_front = ImageOps.contain(img_front, (target_w, target_h), Image.Resampling.LANCZOS)
        img_back = ImageOps.contain(img_back, (target_w, target_h), Image.Resampling.LANCZOS)

        # 补白边对齐
        bg_front = Image.new('RGB', (target_w, target_h), (255, 255, 255))
        bg_back = Image.new('RGB', (target_w, target_h), (255, 255, 255))
        
        bg_front.paste(img_front, ((target_w - img_front.width)//2, (target_h - img_front.height)//2))
        bg_back.paste(img_back, ((target_w - img_back.width)//2, (target_h - img_back.height)//2))

        # A4 画布
        a4_canvas = Image.new('RGB', (2480, 3508), color='white')
        total_content_h = (target_h * 2) + vertical_gap
        start_y = (3508 - total_content_h) // 2
        x_offset = (2480 - target_w) // 2
        
        a4_canvas.paste(bg_front, (x_offset, start_y))
        a4_canvas.paste(bg_back, (x_offset, start_y + target_h + vertical_gap))

        save_name = f"{person_id} {name} 身份证（合并页）.jpg"
        a4_canvas.save(os.path.join(output_dir, save_name), "JPEG", quality=95)

    def process_single_person(self, person_id, output_dir):
        if person_id not in self.all_persons:
            self.write_log(f"[{person_id}] 缺失记录", "error")
            return False

        person_info = self.all_persons[person_id]
        name = person_info['name']

        try:
            front_pil, back_pil = None, None

            # 情形1：这是一个混合文件（PDF、或者单张合并图）
            if 'mixed' in person_info:
                mixed_path = person_info['mixed']
                if mixed_path.lower().endswith('.pdf'):
                    front_pil, back_pil = self.process_pdf_to_images(mixed_path)
                else:
                    # 单张混合图像读取
                    with open(mixed_path, 'rb') as f:
                        img_array = np.frombuffer(f.read(), dtype=np.uint8)
                    front_pil, back_pil = self.extract_cards_from_image(img_array)
            
            # 情形2：规范的两个单面文件
            elif 'front' in person_info and 'back' in person_info:
                front_pil = Image.open(person_info['front']).convert('RGB')
                back_pil = Image.open(person_info['back']).convert('RGB')
            else:
                self.write_log(f"[{person_id} {name}] 文件不全，无法处理", "error")
                return False

            if not front_pil or not back_pil:
                raise ValueError("无法从文件中正确解析出正反面")

            # 统一送入A4排版器
            self.format_to_a4(front_pil, back_pil, person_id, name, output_dir)
            self.write_log(f"[{person_id} {name}] 处理成功", "info")
            return True

        except Exception as e:
            self.write_log(f"[{person_id} {name}] 处理失败: {str(e)}", "error")
            return False

    # ------------------- 进程与UI控制 -------------------

    def start_merge_thread(self):
        if not self.folder_path:
            messagebox.showwarning("提示", "请选择文件夹")
            return

        p_ids = self.target_ids if self.target_ids else list(self.all_persons.keys())
        if not p_ids: return

        output_dir = os.path.join(self.folder_path, "已合并输出")
        os.makedirs(output_dir, exist_ok=True)

        self.btn_merge.config(state=tk.DISABLED)
        self.progress_bar['maximum'] = len(p_ids)
        self.progress_bar['value'] = 0
        
        self.write_log("启动 AI 视觉分析与合并任务...", "normal")
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
                    messagebox.showinfo("完成", f"任务结束！成功处理 {d} 份")
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