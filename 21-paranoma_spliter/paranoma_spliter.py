import os
import shutil
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from pathlib import Path

from PIL import Image, ImageTk, ImageOps, ImageDraw

try:
    from ttkthemes import ThemedTk
except ImportError:
    ThemedTk = None


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent, width=340):
        super().__init__(parent)

        self.canvas = tk.Canvas(
            self,
            borderwidth=0,
            highlightthickness=0,
            width=width,
            bg="#f5f6f7",
        )
        self.scrollbar = ttk.Scrollbar(
            self,
            orient="vertical",
            command=self.canvas.yview,
        )

        self.inner = ttk.Frame(self.canvas)
        self.inner_id = self.canvas.create_window(
            (0, 0),
            window=self.inner,
            anchor="nw",
        )

        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel_windows)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux)

    def _on_inner_configure(self, event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfigure(self.inner_id, width=event.width)

    def _on_mousewheel_windows(self, event):
        if self.winfo_containing(event.x_root, event.y_root) is None:
            return
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mousewheel_linux(self, event):
        if self.winfo_containing(event.x_root, event.y_root) is None:
            return
        if event.num == 4:
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            self.canvas.yview_scroll(1, "units")


class PanoramaSplitterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("全景图片无损切分工具")

        self.font_family = "Microsoft YaHei"
        self.round_radius = 18

        self.image_path = None
        self.original_image = None
        self.preview_tk = None

        self.canvas_image_id = None
        self.preview_scale = 1.0
        self.preview_offset_x = 0
        self.preview_offset_y = 0
        self.preview_display_w = 0
        self.preview_display_h = 0

        self.ratio_presets = {
            "9:16": (9, 16),
            "1:2": (1, 2),
            "3:4": (3, 4),
            "2:3": (2, 3),
            "1:1": (1, 1),
            "4:5": (4, 5),
            "16:9": (16, 9),
            "自定义": None,
        }

        self.ratio_var = tk.StringVar(value="9:16")
        self.custom_w_var = tk.IntVar(value=9)
        self.custom_h_var = tk.IntVar(value=16)

        self.split_count_var = tk.IntVar(value=3)
        self.mode_var = tk.StringVar(value="fixed_height")
        self.output_format_var = tk.StringVar(value="PNG")
        self.output_quality_var = tk.IntVar(value=95)

        self.status_var = tk.StringVar(value="请选择一张全景图片")
        self.info_var = tk.StringVar(value="")
        self.loading_var = tk.StringVar(value="")

        self._setup_window_size()
        self._setup_style()
        self._build_ui()
        self._bind_events()

    def _setup_window_size(self):
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()

        win_w = int(screen_w * 0.82)
        win_h = int(screen_h * 0.82)

        win_w = max(1040, min(win_w, 1480))
        win_h = max(680, min(win_h, 920))

        x = max(0, (screen_w - win_w) // 2)
        y = max(0, (screen_h - win_h) // 2)

        self.root.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self.root.minsize(960, 620)

    def _setup_style(self):
        self.root.option_add("*Font", f"{{{self.font_family}}} 10")
        self.root.option_add("*TCombobox*Listbox.font", f"{{{self.font_family}}} 10")

        style = ttk.Style()

        try:
            if "arc" in style.theme_names():
                style.theme_use("arc")
        except Exception:
            pass

        style.configure(".", font=(self.font_family, 10))
        style.configure("TLabel", font=(self.font_family, 10))
        style.configure("TButton", font=(self.font_family, 10), padding=(8, 5))
        style.configure("TCheckbutton", font=(self.font_family, 10))
        style.configure("TRadiobutton", font=(self.font_family, 10))
        style.configure("TLabelframe.Label", font=(self.font_family, 10, "bold"))
        style.configure("Header.TLabel", font=(self.font_family, 12, "bold"))
        style.configure("Hint.TLabel", font=(self.font_family, 9), foreground="#666666")
        style.configure("Tip.TLabel", font=(self.font_family, 9), foreground="#4d6f91")
        style.configure("Warning.TLabel", font=(self.font_family, 9), foreground="#a05a2c")
        style.configure("Loading.TLabel", font=(self.font_family, 11, "bold"), foreground="#3f6ea5")
        style.configure("Primary.TButton", font=(self.font_family, 11, "bold"), padding=(10, 8))

    def _build_ui(self):
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        left = ttk.Frame(main)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        right_scroll = ScrollableFrame(main, width=350)
        right_scroll.pack(side=tk.RIGHT, fill=tk.Y, padx=(14, 0))

        right = right_scroll.inner

        preview_frame = ttk.LabelFrame(left, text="图片预览", padding=10)
        preview_frame.pack(fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(
            preview_frame,
            bg="#eef1f4",
            highlightthickness=0,
            bd=0,
            relief=tk.FLAT,
        )
        self.canvas.pack(fill=tk.BOTH, expand=True)

        bottom_bar = ttk.Frame(left)
        bottom_bar.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(bottom_bar, textvariable=self.status_var).pack(side=tk.LEFT, anchor=tk.W)
        ttk.Label(bottom_bar, textvariable=self.info_var).pack(side=tk.RIGHT, anchor=tk.E)

        title = ttk.Label(right, text="全景图片切分", style="Header.TLabel")
        title.pack(anchor=tk.W, pady=(0, 10), padx=(2, 10))

        ttk.Button(
            right,
            text="选择全景图片",
            command=self.open_image,
            style="Primary.TButton",
        ).pack(fill=tk.X, pady=(0, 10), padx=(2, 10))

        self.loading_label = ttk.Label(
            right,
            textvariable=self.loading_var,
            style="Loading.TLabel",
        )
        self.loading_label.pack(anchor=tk.W, pady=(0, 8), padx=(2, 10))

        file_box = ttk.LabelFrame(right, text="图片信息", padding=10)
        file_box.pack(fill=tk.X, pady=(0, 12), padx=(2, 10))

        self.file_label = ttk.Label(file_box, text="未选择图片", wraplength=300)
        self.file_label.pack(anchor=tk.W)

        self.size_label = ttk.Label(file_box, text="")
        self.size_label.pack(anchor=tk.W, pady=(6, 0))

        ratio_box = ttk.LabelFrame(right, text="切分比例", padding=10)
        ratio_box.pack(fill=tk.X, pady=(0, 12), padx=(2, 10))

        self.ratio_combo = ttk.Combobox(
            ratio_box,
            textvariable=self.ratio_var,
            values=list(self.ratio_presets.keys()),
            state="readonly",
            font=(self.font_family, 10),
        )
        self.ratio_combo.pack(fill=tk.X)

        tip_text = (
            "平台建议：小红书推荐 9:16；抖音推荐 1:2。\n"
            "轮播图常用 3:4、4:5；横图展示可用 16:9。"
        )
        ttk.Label(
            ratio_box,
            text=tip_text,
            style="Tip.TLabel",
            wraplength=300,
        ).pack(anchor=tk.W, pady=(8, 0))

        custom_frame = ttk.Frame(ratio_box)
        custom_frame.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(custom_frame, text="自定义：").pack(side=tk.LEFT)

        self.custom_w_spin = ttk.Spinbox(
            custom_frame,
            from_=1,
            to=100,
            textvariable=self.custom_w_var,
            width=5,
            command=self.on_setting_changed,
            font=(self.font_family, 10),
        )
        self.custom_w_spin.pack(side=tk.LEFT)

        ttk.Label(custom_frame, text=" : ").pack(side=tk.LEFT)

        self.custom_h_spin = ttk.Spinbox(
            custom_frame,
            from_=1,
            to=100,
            textvariable=self.custom_h_var,
            width=5,
            command=self.on_setting_changed,
            font=(self.font_family, 10),
        )
        self.custom_h_spin.pack(side=tk.LEFT)

        count_box = ttk.LabelFrame(right, text="切分数量", padding=10)
        count_box.pack(fill=tk.X, pady=(0, 12), padx=(2, 10))

        count_row = ttk.Frame(count_box)
        count_row.pack(fill=tk.X)

        ttk.Label(count_row, text="数量：").pack(side=tk.LEFT)

        self.count_spin = ttk.Spinbox(
            count_row,
            from_=1,
            to=99,
            textvariable=self.split_count_var,
            width=8,
            command=self.on_count_changed,
            font=(self.font_family, 10),
        )
        self.count_spin.pack(side=tk.LEFT)

        ttk.Button(count_row, text="自动推荐", command=self.auto_recommend_count).pack(
            side=tk.RIGHT
        )

        self.count_hint_label = ttk.Label(count_box, text="", style="Hint.TLabel")
        self.count_hint_label.pack(anchor=tk.W, pady=(8, 0))

        mode_box = ttk.LabelFrame(right, text="裁切模式", padding=10)
        mode_box.pack(fill=tk.X, pady=(0, 12), padx=(2, 10))

        ttk.Radiobutton(
            mode_box,
            text="固定比例，以图片高度为基准切分",
            variable=self.mode_var,
            value="fixed_height",
            command=self.on_setting_changed,
        ).pack(anchor=tk.W)

        ttk.Radiobutton(
            mode_box,
            text="智能铺满宽度，减少横向损失",
            variable=self.mode_var,
            value="smart_width",
            command=self.on_setting_changed,
        ).pack(anchor=tk.W, pady=(6, 0))

        ttk.Label(
            mode_box,
            text="固定高度适合完整保留上下内容；智能铺满适合尽量保留横向全景。",
            style="Hint.TLabel",
            wraplength=300,
        ).pack(anchor=tk.W, pady=(8, 0))

        output_box = ttk.LabelFrame(right, text="输出设置", padding=10)
        output_box.pack(fill=tk.X, pady=(0, 12), padx=(2, 10))

        ttk.Label(output_box, text="输出格式：").pack(anchor=tk.W)

        self.format_combo = ttk.Combobox(
            output_box,
            textvariable=self.output_format_var,
            values=["PNG", "JPEG", "WEBP", "原格式"],
            state="readonly",
            font=(self.font_family, 10),
        )
        self.format_combo.pack(fill=tk.X, pady=(4, 8))

        quality_row = ttk.Frame(output_box)
        quality_row.pack(fill=tk.X)

        ttk.Label(quality_row, text="JPEG / WEBP 质量：").pack(side=tk.LEFT)

        self.quality_spin = ttk.Spinbox(
            quality_row,
            from_=1,
            to=100,
            textvariable=self.output_quality_var,
            width=6,
            font=(self.font_family, 10),
        )
        self.quality_spin.pack(side=tk.RIGHT)

        note = (
            "说明：脚本不会缩放图片，只按原始像素裁切。\n"
            "PNG 输出不会产生 JPEG 二次压缩损失。\n"
            "JPEG 原图若继续保存为 JPEG，会重新编码。"
        )
        ttk.Label(output_box, text=note, style="Hint.TLabel", wraplength=300).pack(
            anchor=tk.W, pady=(8, 0)
        )

        action_box = ttk.LabelFrame(right, text="操作", padding=10)
        action_box.pack(fill=tk.X, pady=(0, 14), padx=(2, 10))

        ttk.Button(
            action_box,
            text="开始切分并保存",
            command=self.split_and_save,
            style="Primary.TButton",
        ).pack(fill=tk.X)

        ttk.Button(
            action_box,
            text="打开输出文件夹",
            command=self.open_output_folder,
        ).pack(fill=tk.X, pady=(8, 0))

        ttk.Label(
            action_box,
            text="窗口较小时，右侧面板可滚动查看全部选项。",
            style="Warning.TLabel",
            wraplength=300,
        ).pack(anchor=tk.W, pady=(8, 0))

    def _bind_events(self):
        self.canvas.bind("<Configure>", lambda event: self.refresh_preview())
        self.ratio_combo.bind("<<ComboboxSelected>>", lambda event: self.on_setting_changed())
        self.custom_w_spin.bind("<KeyRelease>", lambda event: self.on_setting_changed())
        self.custom_h_spin.bind("<KeyRelease>", lambda event: self.on_setting_changed())
        self.count_spin.bind("<KeyRelease>", lambda event: self.on_count_changed())

    def set_loading(self, is_loading, text=""):
        if is_loading:
            self.loading_var.set(text or "图片加载中，请稍候...")
            self.status_var.set(text or "图片加载中，请稍候...")
            self.root.configure(cursor="watch")
            self.root.update_idletasks()
        else:
            self.loading_var.set("")
            self.root.configure(cursor="")
            self.root.update_idletasks()

    def open_image(self):
        path = filedialog.askopenfilename(
            title="选择全景图片",
            filetypes=[
                ("图片文件", "*.jpg *.jpeg *.png *.webp *.bmp *.tif *.tiff"),
                ("所有文件", "*.*"),
            ],
        )

        if not path:
            return

        self.set_loading(True, "图片加载中，请稍候...")
        self.root.after(120, lambda: self.load_image_from_path(path))

    def load_image_from_path(self, path):
        try:
            img = Image.open(path)
            img = ImageOps.exif_transpose(img)
            img.load()

            if img.mode not in ("RGB", "RGBA"):
                img = img.convert("RGB")

            self.image_path = Path(path)
            self.original_image = img

            self.file_label.config(text=str(self.image_path.name))
            self.size_label.config(text=f"尺寸：{img.width} × {img.height} px")

            self.status_var.set("图片已加载")
            self.update_count_limit()
            self.auto_recommend_count()
            self.refresh_preview()

        except Exception as exc:
            messagebox.showerror("打开失败", f"无法打开图片：\n{exc}")
            self.status_var.set("图片加载失败")
        finally:
            self.set_loading(False)

    def get_ratio(self):
        key = self.ratio_var.get()

        if key == "自定义":
            try:
                w = max(1, int(self.custom_w_var.get()))
                h = max(1, int(self.custom_h_var.get()))
                return w / h, f"{w}:{h}"
            except Exception:
                return 9 / 16, "9:16"

        preset = self.ratio_presets.get(key, (9, 16))
        return preset[0] / preset[1], key

    def on_setting_changed(self):
        if self.original_image is None:
            return

        self.update_count_limit()
        self.refresh_preview()

    def on_count_changed(self):
        if self.original_image is None:
            return

        self.clamp_split_count()
        self.refresh_preview()

    def update_count_limit(self):
        if self.original_image is None:
            return

        max_count = self.get_max_count()
        current = self.safe_get_count()

        if current > max_count:
            self.split_count_var.set(max_count)

        self.count_spin.config(from_=1, to=max_count)

        ratio_value, ratio_name = self.get_ratio()
        mode = self.mode_var.get()

        if mode == "fixed_height":
            piece_w = int(self.original_image.height * ratio_value)
            self.count_hint_label.config(
                text=f"当前比例 {ratio_name}，单张最大宽度约 {piece_w}px，最多 {max_count} 张"
            )
        else:
            self.count_hint_label.config(
                text=f"当前比例 {ratio_name}，智能铺满宽度模式最多 {max_count} 张"
            )

    def get_max_count(self):
        if self.original_image is None:
            return 1

        img_w = self.original_image.width
        img_h = self.original_image.height
        ratio_value, _ = self.get_ratio()

        if self.mode_var.get() == "fixed_height":
            piece_w = max(1, int(img_h * ratio_value))
            return max(1, img_w // piece_w)

        min_piece_w = max(1, int(img_h * ratio_value))
        max_reasonable = max(1, img_w // min_piece_w)

        return max(1, min(30, max(img_w // 200, max_reasonable)))

    def safe_get_count(self):
        try:
            return max(1, int(self.split_count_var.get()))
        except Exception:
            return 1

    def clamp_split_count(self):
        max_count = self.get_max_count()
        count = self.safe_get_count()

        if count > max_count:
            count = max_count

        if count < 1:
            count = 1

        self.split_count_var.set(count)
        return count

    def auto_recommend_count(self):
        if self.original_image is None:
            return

        img_w = self.original_image.width
        img_h = self.original_image.height
        ratio_value, _ = self.get_ratio()

        if self.mode_var.get() == "fixed_height":
            piece_w = max(1, int(img_h * ratio_value))
            count = max(1, img_w // piece_w)
            count = min(count, self.get_max_count())
            self.split_count_var.set(count)
            self.update_count_limit()
            self.refresh_preview()
            return

        best_count = 1
        best_score = float("inf")
        max_count = self.get_max_count()

        for count in range(1, max_count + 1):
            piece_w = img_w / count
            piece_h = piece_w / ratio_value

            if piece_h > img_h:
                continue

            vertical_loss = img_h - piece_h
            piece_width_score = abs(piece_w - 1080)

            score = vertical_loss * 10 + piece_width_score

            if score < best_score:
                best_score = score
                best_count = count

        self.split_count_var.set(best_count)
        self.update_count_limit()
        self.refresh_preview()

    def get_crop_boxes(self):
        if self.original_image is None:
            return []

        img_w = self.original_image.width
        img_h = self.original_image.height
        count = self.clamp_split_count()
        ratio_value, _ = self.get_ratio()
        mode = self.mode_var.get()

        boxes = []

        if mode == "fixed_height":
            piece_h = img_h
            piece_w = int(round(piece_h * ratio_value))
            piece_w = max(1, piece_w)

            total_w = piece_w * count

            if total_w > img_w:
                count = max(1, img_w // piece_w)
                self.split_count_var.set(count)
                total_w = piece_w * count

            start_x = max(0, (img_w - total_w) // 2)

            for i in range(count):
                left = start_x + i * piece_w
                right = left + piece_w
                boxes.append((left, 0, right, img_h))

        else:
            for i in range(count):
                left = int(round(i * img_w / count))
                right = int(round((i + 1) * img_w / count))
                piece_w = right - left
                piece_h = int(round(piece_w / ratio_value))

                if piece_h > img_h:
                    piece_h = img_h
                    adjusted_w = int(round(piece_h * ratio_value))
                    cx = (left + right) // 2
                    left = max(0, cx - adjusted_w // 2)
                    right = min(img_w, left + adjusted_w)

                top = max(0, (img_h - piece_h) // 2)
                bottom = min(img_h, top + piece_h)

                boxes.append((left, top, right, bottom))

        return boxes

    def make_rounded_preview(self, image, radius):
        image = image.convert("RGBA")

        mask = Image.new("L", image.size, 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle(
            (0, 0, image.width - 1, image.height - 1),
            radius=radius,
            fill=255,
        )

        rounded = Image.new("RGBA", image.size, (0, 0, 0, 0))
        rounded.paste(image, (0, 0), mask)
        return rounded

    def refresh_preview(self):
        self.canvas.delete("all")

        if self.original_image is None:
            w = self.canvas.winfo_width()
            h = self.canvas.winfo_height()

            self.canvas.create_text(
                w // 2,
                h // 2 - 38,
                text="请选择一张全景图片",
                fill="#6f7c86",
                font=(self.font_family, 18, "bold"),
            )
            self.canvas.create_text(
                w // 2,
                h // 2,
                text="支持 JPG / PNG / WEBP / BMP / TIFF",
                fill="#8a969f",
                font=(self.font_family, 10),
            )
            self.canvas.create_text(
                w // 2,
                h // 2 + 30,
                text="小红书推荐 9:16 ｜ 抖音推荐 1:2",
                fill="#5f7f52",
                font=(self.font_family, 10, "bold"),
            )
            return

        canvas_w = max(1, self.canvas.winfo_width())
        canvas_h = max(1, self.canvas.winfo_height())

        img_w = self.original_image.width
        img_h = self.original_image.height

        scale = min(canvas_w / img_w, canvas_h / img_h) * 0.92
        preview_w = max(1, int(img_w * scale))
        preview_h = max(1, int(img_h * scale))

        self.preview_scale = scale
        self.preview_display_w = preview_w
        self.preview_display_h = preview_h
        self.preview_offset_x = (canvas_w - preview_w) // 2
        self.preview_offset_y = (canvas_h - preview_h) // 2

        preview = self.original_image.resize(
            (preview_w, preview_h),
            Image.Resampling.LANCZOS,
        )

        rounded_preview = self.make_rounded_preview(preview, self.round_radius)
        self.preview_tk = ImageTk.PhotoImage(rounded_preview)

        self.draw_preview_shadow_and_border()

        self.canvas_image_id = self.canvas.create_image(
            self.preview_offset_x,
            self.preview_offset_y,
            image=self.preview_tk,
            anchor=tk.NW,
        )

        self.draw_crop_preview()

        boxes = self.get_crop_boxes()
        if boxes:
            first = boxes[0]
            crop_w = first[2] - first[0]
            crop_h = first[3] - first[1]
            self.info_var.set(f"单张约 {crop_w} × {crop_h}px，共 {len(boxes)} 张")

    def draw_preview_shadow_and_border(self):
        x1 = self.preview_offset_x
        y1 = self.preview_offset_y
        x2 = x1 + self.preview_display_w
        y2 = y1 + self.preview_display_h

        self.create_rounded_rectangle(
            x1 + 5,
            y1 + 6,
            x2 + 5,
            y2 + 6,
            radius=self.round_radius,
            fill="#cfd5db",
            outline="",
        )

        self.create_rounded_rectangle(
            x1 - 1,
            y1 - 1,
            x2 + 1,
            y2 + 1,
            radius=self.round_radius + 1,
            fill="",
            outline="#ffffff",
            width=2,
        )

    def create_rounded_rectangle(
        self,
        x1,
        y1,
        x2,
        y2,
        radius=16,
        fill="",
        outline="",
        width=1,
    ):
        points = [
            x1 + radius,
            y1,
            x2 - radius,
            y1,
            x2,
            y1,
            x2,
            y1 + radius,
            x2,
            y2 - radius,
            x2,
            y2,
            x2 - radius,
            y2,
            x1 + radius,
            y2,
            x1,
            y2,
            x1,
            y2 - radius,
            x1,
            y1 + radius,
            x1,
            y1,
        ]

        return self.canvas.create_polygon(
            points,
            smooth=True,
            fill=fill,
            outline=outline,
            width=width,
        )

    def draw_crop_preview(self):
        boxes = self.get_crop_boxes()

        for index, box in enumerate(boxes, start=1):
            left, top, right, bottom = box

            x1 = self.preview_offset_x + left * self.preview_scale
            y1 = self.preview_offset_y + top * self.preview_scale
            x2 = self.preview_offset_x + right * self.preview_scale
            y2 = self.preview_offset_y + bottom * self.preview_scale

            self.canvas.create_rectangle(
                x1,
                y1,
                x2,
                y2,
                outline="#ffffff",
                width=2,
                dash=(8, 5),
            )

            self.canvas.create_rectangle(
                x1 + 2,
                y1 + 2,
                x2 - 2,
                y2 - 2,
                outline="#222222",
                width=1,
                dash=(8, 5),
            )

            label_x = x1 + 18
            label_y = y1 + 18

            self.canvas.create_oval(
                label_x - 12,
                label_y - 12,
                label_x + 12,
                label_y + 12,
                fill="#ffffff",
                outline="#2f3b45",
                width=1,
            )

            self.canvas.create_text(
                label_x,
                label_y,
                text=str(index),
                fill="#1f2a33",
                font=(self.font_family, 9, "bold"),
            )

    def get_output_format(self):
        selected = self.output_format_var.get()

        if selected == "原格式":
            suffix = self.image_path.suffix.lower()

            if suffix in [".jpg", ".jpeg"]:
                return "JPEG", ".jpg"
            if suffix == ".png":
                return "PNG", ".png"
            if suffix == ".webp":
                return "WEBP", ".webp"
            if suffix in [".tif", ".tiff"]:
                return "TIFF", ".tif"
            if suffix == ".bmp":
                return "BMP", ".bmp"

            return "PNG", ".png"

        if selected == "JPEG":
            return "JPEG", ".jpg"

        if selected == "WEBP":
            return "WEBP", ".webp"

        return "PNG", ".png"

    def get_output_dir(self):
        if not self.image_path:
            return None

        return self.image_path.parent / "切分输出"

    def split_and_save(self):
        if self.original_image is None or self.image_path is None:
            messagebox.showwarning("未选择图片", "请先选择一张全景图片。")
            return

        boxes = self.get_crop_boxes()

        if not boxes:
            messagebox.showwarning("无法切分", "当前参数无法生成有效切分区域。")
            return

        output_dir = self.get_output_dir()

        try:
            self.set_loading(True, "正在切分并保存图片...")

            output_dir.mkdir(parents=True, exist_ok=True)

            fmt, ext = self.get_output_format()
            base_name = self.image_path.stem

            saved_files = []

            for index, box in enumerate(boxes, start=1):
                crop = self.original_image.crop(box)

                if fmt == "JPEG":
                    if crop.mode == "RGBA":
                        background = Image.new("RGB", crop.size, (255, 255, 255))
                        background.paste(crop, mask=crop.split()[-1])
                        crop = background
                    else:
                        crop = crop.convert("RGB")

                output_path = output_dir / f"{base_name}_切分_{index:02d}{ext}"

                save_kwargs = {}

                if fmt in ("JPEG", "WEBP"):
                    save_kwargs["quality"] = int(self.output_quality_var.get())
                    save_kwargs["optimize"] = True

                if fmt == "PNG":
                    save_kwargs["compress_level"] = 0

                crop.save(output_path, format=fmt, **save_kwargs)
                saved_files.append(output_path)

            messagebox.showinfo(
                "切分完成",
                f"已保存 {len(saved_files)} 张图片到：\n{output_dir}",
            )

            self.status_var.set(f"切分完成：{len(saved_files)} 张")

        except Exception as exc:
            messagebox.showerror("保存失败", f"切分保存时发生错误：\n{exc}")
            self.status_var.set("切分保存失败")
        finally:
            self.set_loading(False)

    def open_output_folder(self):
        output_dir = self.get_output_dir()

        if output_dir is None:
            messagebox.showwarning("未选择图片", "请先选择一张图片。")
            return

        if not output_dir.exists():
            messagebox.showwarning("文件夹不存在", "还没有生成切分输出文件夹。")
            return

        try:
            if os.name == "nt":
                os.startfile(str(output_dir))
            elif os.name == "posix":
                if shutil.which("open"):
                    os.system(f'open "{output_dir}"')
                elif shutil.which("xdg-open"):
                    os.system(f'xdg-open "{output_dir}"')
                else:
                    messagebox.showinfo("输出路径", str(output_dir))
            else:
                messagebox.showinfo("输出路径", str(output_dir))
        except Exception:
            messagebox.showinfo("输出路径", str(output_dir))


def main():
    if ThemedTk is not None:
        root = ThemedTk(theme="arc")
    else:
        root = tk.Tk()
        messagebox.showwarning(
            "缺少 ttkthemes",
            "当前环境未安装 ttkthemes，将使用默认主题。\n\n请执行：pip install ttkthemes",
        )

    app = PanoramaSplitterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()