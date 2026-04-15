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
from PIL import Image, ImageOps
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class IDCardProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("身份证图片智能处理中枢 (精准命名版)")
        self.root.geometry("950x700")
        self.root.minsize(900, 650)

        self.folder_path = ""
        self.list_file_path = ""
        self.all_persons = {}  
        self.target_ids = []   
        
        self.log_queue = queue.Queue()
        
        self.face_cascade = cv2.CascadeClassifier()
        cascade_path = os.path.join(cv2.data.haarcascades, 'haarcascade_frontalface_default.xml')
        if os.path.exists(cascade_path):
            self.face_cascade.load(cascade_path)

        self.setup_logging()
        self.build_ui()
        self.start_log_monitor()

    def setup_logging(self):
        os.makedirs("logs", exist_ok=True)
        log_filename = datetime.now().strftime("logs/process_log_%Y%m%d_%H%M%S.txt")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            encoding='utf-8'
        )

    def build_ui(self):
        main_frame = ttk.Frame(self.root, padding=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        folder_frame = ttk.LabelFrame(main_frame, text="第一步：选择包含身份证(图片/PDF)的文件夹", padding=15)
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

        self.btn_merge = ttk.Button(btn_frame, text="标准合并(排版A4)", command=self.start_merge_thread, style="Accent.TButton")
        self.btn_merge.pack(side=tk.RIGHT, ipadx=10, ipady=5)

        self.btn_split = ttk.Button(btn_frame, text="混合件严格提取拆分", command=self.confirm_split)
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

        # 兼容读取带空格或加号的输入文件
        pattern = re.compile(r'^(\d{5})[\s\+]*(.*?)[\s\+]*(?:身份证|合并).*?\.([a-zA-Z0-9]+)$', re.IGNORECASE)

        for filename in os.listdir(self.folder_path):
            match = pattern.match(filename)
            if match:
                person_id = match.group(1)
                # 【严格过滤】：提取名称时，去除左右两边和中间的所有空格、加号，确保名字本身纯净
                name = match.group(2).strip().replace(" ", "").replace("+", "")
                full_path = os.path.join(self.folder_path, filename)

                if person_id not in self.all_persons:
                    self.all_persons[person_id] = {'name': name}

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

    # ------------------- 二次深度矫正视觉中枢 -------------------

    def order_points(self, pts):
        rect = np.zeros((4, 2), dtype="float32")
        s = pts.sum(axis=1)
        rect[0] = pts[np.argmin(s)]
        rect[2] = pts[np.argmax(s)]
        diff = np.diff(pts, axis=1)
        rect[1] = pts[np.argmin(diff)]
        rect[3] = pts[np.argmax(diff)]
        return rect

    def strict_four_point_transform(self, image, pts):
        """严格抠图拉伸算法"""
        rect = self.order_points(pts)
        (tl, tr, br, bl) = rect
        
        widthA = np.sqrt(((br[0] - bl[0]) ** 2) + ((br[1] - bl[1]) ** 2))
        widthB = np.sqrt(((tr[0] - tl[0]) ** 2) + ((tr[1] - tl[1]) ** 2))
        maxWidth = max(int(widthA), int(widthB))
        
        heightA = np.sqrt(((tr[0] - br[0]) ** 2) + ((tr[1] - br[1]) ** 2))
        heightB = np.sqrt(((tl[0] - bl[0]) ** 2) + ((tl[1] - bl[1]) ** 2))
        maxHeight = max(int(heightA), int(heightB))
        
        if maxWidth <= 0 or maxHeight <= 0: return None 

        dst = np.array([[0, 0], [maxWidth - 1, 0], [maxWidth - 1, maxHeight - 1], [0, maxHeight - 1]], dtype="float32")
        M = cv2.getPerspectiveTransform(rect, dst)
        warped = cv2.warpPerspective(image, M, (maxWidth, maxHeight))
        
        if maxHeight > maxWidth:
            warped = cv2.rotate(warped, cv2.ROTATE_90_CLOCKWISE)
            
        warped = cv2.resize(warped, (1011, 638), interpolation=cv2.INTER_CUBIC)
        return warped

    def perspective_correction(self, img):
        """二次深度矫正引擎"""
        h, w = img.shape[:2]
        if h < 50 or w < 50: return img

        try:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            blur = cv2.bilateralFilter(gray, 9, 75, 75)
            edged = cv2.Canny(blur, 30, 150)
            
            thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 21, 10)
            combined = cv2.bitwise_or(thresh, edged)

            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
            closed = cv2.morphologyEx(combined, cv2.MORPH_CLOSE, kernel)

            cnts, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            if not cnts: return img

            cnts = sorted(cnts, key=cv2.contourArea, reverse=True)
            img_area = h * w

            for c in cnts:
                area = cv2.contourArea(c)
                if area > img_area * 0.92: continue
                if area < img_area * 0.10: continue

                rect = cv2.minAreaRect(c)
                box = cv2.boxPoints(rect)
                box = np.int32(box)

                rw, rh = rect[1]
                if min(rw, rh) == 0: continue
                ratio = max(rw, rh) / min(rw, rh)

                if 1.3 <= ratio <= 2.2:
                    warped = self.strict_four_point_transform(img, box)
                    if warped is not None:
                        return warped 
        except Exception:
            pass
            
        return img 

    def _force_standard_image(self, img_cv):
        h, w = img_cv.shape[:2]
        if h > w: img_cv = cv2.rotate(img_cv, cv2.ROTATE_90_CLOCKWISE)
        img_cv = cv2.resize(img_cv, (1011, 638), interpolation=cv2.INTER_CUBIC)
        return Image.fromarray(cv2.cvtColor(img_cv, cv2.COLOR_BGR2RGB))

    def classify_front_back(self, img1, img2):
        if self.face_cascade.empty(): return img1, img2
        try:
            gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
            faces1 = self.face_cascade.detectMultiScale(gray1, 1.1, 3, minSize=(30, 30))
            if len(faces1) > 0: return img1, img2
            
            gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
            faces2 = self.face_cascade.detectMultiScale(gray2, 1.1, 3, minSize=(30, 30))
            if len(faces2) > 0: return img2, img1
        except Exception: pass 
        return img1, img2

    def extract_cards_from_image(self, img_array):
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None: return None, None
        
        h, w = img.shape[:2]
        if h < 50 or w < 50: return None, None

        try:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            thresh = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 21, 10)
            blur = cv2.bilateralFilter(gray, 9, 75, 75)
            edged = cv2.Canny(blur, 30, 150)
            combined = cv2.bitwise_or(thresh, edged)
            
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
            closed = cv2.morphologyEx(combined, cv2.MORPH_CLOSE, kernel)
            
            cnts, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            cnts = sorted(cnts, key=cv2.contourArea, reverse=True)
            
            cards_cv = []
            img_area = h * w
            
            for c in cnts:
                area = cv2.contourArea(c)
                if area < img_area * 0.06 or area > img_area * 0.90: continue
                    
                rect = cv2.minAreaRect(c)
                box = cv2.boxPoints(rect)
                box = np.int32(box)
                
                rw, rh = rect[1]
                if min(rw, rh) == 0: continue
                ratio = max(rw, rh) / min(rw, rh)
                
                if 1.3 <= ratio <= 2.2:
                    warped = self.strict_four_point_transform(img, box)
                    if warped is not None:
                        cards_cv.append(warped)
                        
                if len(cards_cv) == 2: break
                
            if len(cards_cv) < 2:
                _, binary = cv2.threshold(gray, 245, 255, cv2.THRESH_BINARY_INV)
                coords = cv2.findNonZero(binary)
                if coords is not None:
                    x, y, cw, ch = cv2.boundingRect(coords)
                    roi = img[y:y+ch, x:x+cw]
                else:
                    roi = img
                
                rh, rw = roi.shape[:2]
                if rh > rw: 
                    c1 = roi[0:rh//2, 0:rw].copy()
                    c2 = roi[rh//2:rh, 0:rw].copy()
                else:       
                    c1 = roi[0:rh, 0:rw//2].copy()
                    c2 = roi[0:rh, rw//2:rw].copy()
                
                cards_cv = []
                for c in [c1, c2]:
                    corrected_c = self.perspective_correction(c)
                    cards_cv.append(corrected_c)

            if len(cards_cv) < 2: return None, None
            
            front_cv, back_cv = self.classify_front_back(cards_cv[0], cards_cv[1])
            return (self._force_standard_image(front_cv), self._force_standard_image(back_cv))

        except Exception as e:
            raise ValueError(f"视觉处理崩溃拦截: {str(e)}")

    def process_pdf_to_images(self, pdf_path):
        doc = fitz.open(pdf_path)
        if len(doc) == 0: return None, None
        
        images = []
        for page_num in range(min(2, len(doc))):
            page = doc.load_page(page_num)
            pix = page.get_pixmap(dpi=200) 
            img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            if pix.n == 4: img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2RGB)
            elif pix.n == 3: img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
            _, buffer = cv2.imencode('.png', img_array)
            images.append(np.array(buffer))
        doc.close()

        if len(images) == 1:
            return self.extract_cards_from_image(images[0])
        elif len(images) >= 2:
            img1 = cv2.imdecode(images[0], cv2.IMREAD_COLOR)
            img2 = cv2.imdecode(images[1], cv2.IMREAD_COLOR)
            if img1 is None or img2 is None: return None, None
            
            img1_corrected = self.perspective_correction(img1)
            img2_corrected = self.perspective_correction(img2)
            
            front_cv, back_cv = self.classify_front_back(img1_corrected, img2_corrected)
            return (self._force_standard_image(front_cv), self._force_standard_image(back_cv))

    # ================= 业务线 A：执行合并操作 =================

    def load_and_correct_single_file(self, file_path):
        with open(file_path, 'rb') as f:
            img_array = np.frombuffer(f.read(), dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None: return None
        
        corrected_img = self.perspective_correction(img)
        return self._force_standard_image(corrected_img)

    def process_merge_single_person(self, person_id, output_dir):
        if person_id not in self.all_persons: return False
        person_info = self.all_persons[person_id]
        name = person_info['name']

        if 'front' not in person_info or 'back' not in person_info:
            self.write_log(f"[{person_id}+{name}] 略过合并：缺失标准的单面图片", "warning")
            return False

        try:
            front_pil = self.load_and_correct_single_file(person_info['front'])
            back_pil = self.load_and_correct_single_file(person_info['back'])
            
            if not front_pil or not back_pil:
                raise ValueError("图片数据读取或提取损坏")

            a4_canvas = Image.new('RGB', (2480, 3508), color='white')
            target_w, target_h, vertical_gap = 1011, 638, 150
            start_y = (3508 - (target_h * 2 + vertical_gap)) // 2
            x_offset = (2480 - target_w) // 2
            
            a4_canvas.paste(front_pil, (x_offset, start_y))
            a4_canvas.paste(back_pil, (x_offset, start_y + target_h + vertical_gap))

            # 【强制命名规范：全加号，无空格】
            save_name = f"{person_id}+{name}+身份证（合并页）.jpg"
            a4_canvas.save(os.path.join(output_dir, save_name), "JPEG", quality=95)
            self.write_log(f"[{person_id}+{name}] 合并成功", "info")
            return True
        except Exception as e:
            self.write_log(f"[{person_id}+{name}] 合并失败: {str(e)}", "error")
            return False

    # ================= 业务线 B：执行拆分操作 =================

    def confirm_split(self):
        if not self.folder_path:
            messagebox.showwarning("提示", "请先选择文件夹")
            return
        if messagebox.askyesno("拆分确认", "确认将混合文件严格拆分提取为独立单面？"):
            self.start_split_thread()

    def process_split_single_person(self, person_id, output_dir):
        if person_id not in self.all_persons: return False
        person_info = self.all_persons[person_id]
        name = person_info['name']

        if 'mixed' not in person_info:
            return False 

        try:
            mixed_path = person_info['mixed']
            front_pil, back_pil = None, None

            if mixed_path.lower().endswith('.pdf'):
                front_pil, back_pil = self.process_pdf_to_images(mixed_path)
            else:
                with open(mixed_path, 'rb') as f:
                    img_array = np.frombuffer(f.read(), dtype=np.uint8)
                front_pil, back_pil = self.extract_cards_from_image(img_array)

            if not front_pil or not back_pil:
                raise ValueError("图像解析失败，未能提取出主体")

            # 【强制命名规范：全加号，无空格】
            front_name = f"{person_id}+{name}+身份证（人像面）.jpg"
            back_name = f"{person_id}+{name}+身份证（国徽面）.jpg"

            front_pil.save(os.path.join(output_dir, front_name), "JPEG", quality=95)
            back_pil.save(os.path.join(output_dir, back_name), "JPEG", quality=95)

            self.write_log(f"[{person_id}+{name}] 严格提取拆分成功", "info")
            return True
        except Exception as e:
            self.write_log(f"[{person_id}+{name}] 拆分失败: {str(e)}", "error")
            return False

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
        self.write_log("========== 开始严格提取拆分任务 ==========", "normal")
        threading.Thread(target=self._run_task, args=(p_ids, out_dir, "拆分"), daemon=True).start()

    def _run_task(self, ids, out_dir, task_type):
        success = 0
        with ThreadPoolExecutor(max_workers=1) as exe:
            target_func = self.process_merge_single_person if task_type == "合并" else self.process_split_single_person
            futures = {exe.submit(target_func, i, out_dir): i for i in ids}
            for f in as_completed(futures):
                try:
                    if f.result(): success += 1
                except Exception as e:
                    self.write_log(f"底层错误拦截: {str(e)}", "error")
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
    IDCardProcessorApp(root)
    root.mainloop()