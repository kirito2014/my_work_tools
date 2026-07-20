#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
作者: zjj/gsp
日期: 20240630
功能: 为 gen_ae_sql.py / gen_ae_migration_sql.py 提供图形界面
      - 顶部下拉切换脚本，参数区动态联动
      - 参数1（归属层级）使用下拉框
      - 参数2（模板文件路径）使用文件选择器
      - gen_ae_migration_sql 额外显示 开始日期/结束日期 输入框
      - 执行按钮调用对应脚本（子进程，实时日志，支持强制停止）

依赖安装：
    pip install ttkthemes

使用方法：
    python gui_gen_ae_sql.py
    （需要保证 ETL_HOME 环境变量已设置）
"""

import os
import sys
import queue
import threading
import subprocess
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import date

try:
    from ttkthemes import ThemedTk
except ImportError:
    print("未检测到 ttkthemes，请先执行: pip install ttkthemes")
    sys.exit(1)


# ========== 配置区域 ==========
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = {
    "AE模板SQL生成": {
        "path": os.path.join(_BASE_DIR, "gen_ae_sql.py"),
        "desc": "根据Excel模板生成AE层SQL文件，输出至 ${ELT_HOME}/autocode/dml/agl|exl/",
        "extra_params": [],
    },
    "AE迁移SQL生成": {
        "path": os.path.join(_BASE_DIR, "gen_ae_migration_sql.py"),
        "desc": "根据Excel模板及日期范围生成AE迁移SQL文件",
        "extra_params": ["date_range"],  # 标记需要显示日期区间参数
    },
}

LEVEL_OPTIONS = ["agl", "exl"]
# ===============================


class GenAeSqlGUI:
    def __init__(self, root: ThemedTk):
        self.root = root
        self.root.set_theme("arc")
        self.root.title("AE SQL 工具集")
        self.root.geometry("860x660")
        self.root.minsize(720, 560)

        self.process = None
        self.log_queue = queue.Queue()
        self.is_running = False
        self.user_stopped = False

        self._build_style()
        self._build_widgets()
        self._poll_log_queue()

    # ---------- 样式 ----------
    def _build_style(self):
        s = ttk.Style()
        s.configure("Title.TLabel",    font=("微软雅黑", 15, "bold"))
        s.configure("Desc.TLabel",     font=("微软雅黑", 9), foreground="#555555")
        s.configure("TLabel",          font=("微软雅黑", 10))
        s.configure("TButton",         font=("微软雅黑", 10))
        s.configure("Run.TButton",     font=("微软雅黑", 11, "bold"))
        s.configure("Section.TLabel",  font=("微软雅黑", 10, "bold"), foreground="#1565c0")

    # ---------- 界面构建 ----------
    def _build_widgets(self):
        self._theme_bg = ttk.Style().lookup("TFrame", "background") or "#f5f6f7"

        outer = ttk.Frame(self.root, padding=20)
        outer.pack(fill=tk.BOTH, expand=True)

        # ── 标题 ──
        ttk.Label(outer, text="AE SQL 工具集", style="Title.TLabel").pack(anchor="w")

        # ── 脚本选择区 ──
        script_frame = ttk.LabelFrame(outer, text="脚本选择", padding=12)
        script_frame.pack(fill=tk.X, pady=(10, 0))
        script_frame.columnconfigure(1, weight=1)

        ttk.Label(script_frame, text="执行脚本：").grid(row=0, column=0, sticky="w", pady=4)
        self.script_var = tk.StringVar(value=list(SCRIPTS.keys())[0])
        self.script_combo = ttk.Combobox(
            script_frame,
            textvariable=self.script_var,
            values=list(SCRIPTS.keys()),
            state="readonly",
            width=28,
        )
        self.script_combo.grid(row=0, column=1, sticky="w", padx=(10, 0), pady=4)
        self.script_combo.bind("<<ComboboxSelected>>", self._on_script_changed)

        self.desc_label = ttk.Label(script_frame, text="", style="Desc.TLabel", wraplength=660)
        self.desc_label.grid(row=1, column=0, columnspan=2, sticky="w", pady=(2, 0))

        # ── 参数区 ──
        self.param_frame = ttk.LabelFrame(outer, text="参数设置", padding=15)
        self.param_frame.pack(fill=tk.X, pady=(12, 0))
        self.param_frame.columnconfigure(1, weight=1)

        # 公共参数：归属层级
        ttk.Label(self.param_frame, text="归属层级：").grid(
            row=0, column=0, sticky="w", pady=8, padx=(0, 6)
        )
        self.level_var = tk.StringVar(value=LEVEL_OPTIONS[0])
        self.level_combo = ttk.Combobox(
            self.param_frame,
            textvariable=self.level_var,
            values=LEVEL_OPTIONS,
            state="readonly",
            width=20,
        )
        self.level_combo.grid(row=0, column=1, sticky="w", padx=(4, 0), pady=8)

        # 公共参数：模板文件
        ttk.Label(self.param_frame, text="模板文件路径：").grid(
            row=1, column=0, sticky="w", pady=8, padx=(0, 6)
        )
        path_row = ttk.Frame(self.param_frame)
        path_row.grid(row=1, column=1, sticky="ew", padx=(4, 0), pady=8)
        path_row.columnconfigure(0, weight=1)

        self.file_path_var = tk.StringVar()
        ttk.Entry(path_row, textvariable=self.file_path_var).grid(row=0, column=0, sticky="ew")
        ttk.Button(path_row, text="浏览...", command=self._choose_file).grid(
            row=0, column=1, padx=(8, 0)
        )

        # 扩展参数：日期区间（gen_ae_migration_sql 使用）
        self.date_row_widgets = []

        lbl_start = ttk.Label(self.param_frame, text="开始日期：")
        lbl_start.grid(row=2, column=0, sticky="w", pady=8, padx=(0, 6))

        date_container = ttk.Frame(self.param_frame)
        date_container.grid(row=2, column=1, sticky="w", padx=(4, 0), pady=8)

        today = date.today()
        self.start_date_var = tk.StringVar(value=today.strftime("%Y-%m-%d"))
        self.end_date_var   = tk.StringVar(value=today.strftime("%Y-%m-%d"))

        entry_start = ttk.Entry(date_container, textvariable=self.start_date_var, width=14)
        entry_start.pack(side=tk.LEFT)

        lbl_to = ttk.Label(date_container, text=" 至 ")
        lbl_to.pack(side=tk.LEFT)

        lbl_end = ttk.Label(self.param_frame, text="结束日期：")  # 隐藏备用（已合并到同行）
        entry_end = ttk.Entry(date_container, textvariable=self.end_date_var, width=14)
        entry_end.pack(side=tk.LEFT)

        lbl_hint = ttk.Label(date_container, text="  格式：YYYY-MM-DD", style="Desc.TLabel")
        lbl_hint.pack(side=tk.LEFT)

        # 记录需要显隐的控件（行 grid_remove / grid）
        self.date_row_labels   = [lbl_start]
        self.date_row_frames   = [date_container]

        # ── 按钮区 ──
        btn_frame = ttk.Frame(outer)
        btn_frame.pack(fill=tk.X, pady=(14, 0))

        self.run_btn = ttk.Button(
            btn_frame, text="▶  执行", style="Run.TButton", command=self._on_run_clicked
        )
        self.run_btn.pack(side=tk.LEFT)

        self.stop_btn = ttk.Button(
            btn_frame, text="■  停止执行", command=self._on_stop_clicked, state="disabled"
        )
        self.stop_btn.pack(side=tk.LEFT, padx=(10, 0))

        ttk.Button(btn_frame, text="清空日志", command=self._clear_log).pack(
            side=tk.LEFT, padx=(10, 0)
        )

        self.status_var = tk.StringVar(value="就绪")
        self.status_label = tk.Label(
            btn_frame,
            textvariable=self.status_var,
            font=("微软雅黑", 10, "bold"),
            fg="#1565c0",
            bg=self._theme_bg,
        )
        self.status_label.pack(side=tk.LEFT, padx=(16, 0))

        self.progress = ttk.Progressbar(btn_frame, mode="indeterminate", length=140)
        self.progress.pack(side=tk.RIGHT)

        # ── 日志区 ──
        log_frame = ttk.LabelFrame(outer, text="执行日志", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=(14, 0))

        log_inner = ttk.Frame(log_frame)
        log_inner.pack(fill=tk.BOTH, expand=True)

        sb = ttk.Scrollbar(log_inner)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        self.log_text = tk.Text(
            log_inner,
            wrap="word",
            bg="#1e1e1e",
            fg="#d4d4d4",
            insertbackground="#d4d4d4",
            font=("Consolas", 10),
            state="disabled",
            yscrollcommand=sb.set,
        )
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.config(command=self.log_text.yview)

        self.log_text.tag_config("info",    foreground="#d4d4d4")
        self.log_text.tag_config("error",   foreground="#f14c4c")
        self.log_text.tag_config("success", foreground="#4ec9b0")

        # 初始化脚本描述 & 参数显隐
        self._refresh_script_ui()

    # ---------- 脚本切换 ----------
    def _on_script_changed(self, _event=None):
        self._refresh_script_ui()

    def _refresh_script_ui(self):
        key = self.script_var.get()
        cfg = SCRIPTS[key]
        self.desc_label.config(text=cfg["desc"])

        show_dates = "date_range" in cfg["extra_params"]
        for w in self.date_row_labels + self.date_row_frames:
            if show_dates:
                w.grid()
            else:
                w.grid_remove()

    # ---------- 交互逻辑 ----------
    def _choose_file(self):
        path = filedialog.askopenfilename(
            title="选择模板文件",
            filetypes=[("Excel 文件", "*.xlsx *.xls *.xlsm"), ("所有文件", "*.*")],
        )
        if path:
            self.file_path_var.set(path)

    def _set_status(self, text: str, color: str = "#1565c0"):
        self.status_var.set(text)
        self.status_label.config(fg=color)

    def _clear_log(self):
        self.log_text.config(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.config(state="disabled")

    def _append_log(self, text: str, tag: str = "info"):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, text, tag)
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    # ---------- 执行 ----------
    def _on_run_clicked(self):
        if self.is_running:
            messagebox.showwarning("提示", "当前已有任务在执行，请等待完成后再操作。")
            return

        key       = self.script_var.get()
        cfg       = SCRIPTS[key]
        script    = cfg["path"]
        level     = self.level_var.get().strip()
        file_path = self.file_path_var.get().strip()

        # ── 校验 ──
        if not level:
            messagebox.showerror("错误", "请选择归属层级！"); return
        if not file_path:
            messagebox.showerror("错误", "请选择模板文件路径！"); return
        if not os.path.exists(file_path):
            messagebox.showerror("错误", "模板文件不存在，请重新选择！"); return
        if not os.path.exists(script):
            messagebox.showerror("错误", "未找到脚本文件，请检查路径配置：\n%s" % script); return
        if not os.environ.get("ETL_HOME"):
            messagebox.showerror("错误", "环境变量 ETL_HOME 未设置，脚本依赖该变量运行。"); return

        # 构建命令参数
        cmd = [sys.executable, "-u", script, level, file_path]

        if "date_range" in cfg["extra_params"]:
            start = self.start_date_var.get().strip()
            end   = self.end_date_var.get().strip()
            if not start or not end:
                messagebox.showerror("错误", "请填写开始日期和结束日期！"); return
            import re
            date_pattern = r"^\d{4}-\d{2}-\d{2}$"
            if not re.match(date_pattern, start) or not re.match(date_pattern, end):
                messagebox.showerror("错误", "日期格式错误，请使用 YYYY-MM-DD 格式！"); return
            if start > end:
                messagebox.showerror("错误", "开始日期不能晚于结束日期！"); return
            cmd += [start, end]

        self._clear_log()
        self._append_log("执行命令: %s\n\n" % " ".join(cmd), "info")

        self.is_running   = True
        self.user_stopped = False
        self.run_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.script_combo.config(state="disabled")
        self._set_status("执行中...", "#2e7d32")
        self.progress.start(12)

        threading.Thread(
            target=self._run_script_thread, args=(cmd, script), daemon=True
        ).start()

    def _run_script_thread(self, cmd: list, script: str):
        try:
            run_env = os.environ.copy()
            run_env["PYTHONIOENCODING"] = "utf-8"
            run_env["PYTHONUTF8"]       = "1"

            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=os.path.dirname(script),
                env=run_env,
                start_new_session=(os.name != "nt"),
            )
            for line in iter(self.process.stdout.readline, ""):
                if line:
                    self.log_queue.put(("info", line))
            self.process.stdout.close()
            rc = self.process.wait()

            if not self.user_stopped:
                if rc == 0:
                    self.log_queue.put(("success", "\n✔ 执行完成，进程退出码: 0\n"))
                else:
                    self.log_queue.put(("error", "\n✘ 执行失败，进程退出码: %s\n" % rc))
        except Exception as e:
            self.log_queue.put(("error", "\n执行出现异常: %s\n" % str(e)))
        finally:
            self.log_queue.put(("__DONE__", ""))

    # ---------- 停止 ----------
    def _on_stop_clicked(self):
        if not self.is_running or not self.process or self.process.poll() is not None:
            messagebox.showinfo("提示", "当前没有正在执行的任务。")
            return
        if not messagebox.askyesno("确认", "确定要强制停止当前正在执行的任务吗？"):
            return
        self._append_log("\n用户手动终止任务...\n", "error")
        self.user_stopped = True
        self._kill_process_tree()

    def _kill_process_tree(self):
        if not self.process:
            return
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(self.process.pid)],
                    capture_output=True,
                )
            else:
                import signal
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                except Exception:
                    self.process.kill()
        except Exception as e:
            self._append_log("终止进程时出现异常: %s\n" % str(e), "error")

    # ---------- 日志轮询 ----------
    def _poll_log_queue(self):
        try:
            while True:
                tag, text = self.log_queue.get_nowait()
                if tag == "__DONE__":
                    self._on_run_finished()
                else:
                    self._append_log(text, tag)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._poll_log_queue)

    def _on_run_finished(self):
        self.is_running = False
        self.run_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.script_combo.config(state="readonly")
        self.progress.stop()
        if self.user_stopped:
            self._set_status("已停止", "#c62828")
        else:
            self._set_status("就绪", "#1565c0")

    # ---------- 关闭窗口 ----------
    def on_close(self):
        if self.is_running and self.process and self.process.poll() is None:
            if not messagebox.askyesno("确认", "任务正在执行中，确定要强制关闭吗？"):
                return
            self._kill_process_tree()
        self.root.destroy()


def main():
    root = ThemedTk(theme="arc")
    app  = GenAeSqlGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    root.mainloop()


if __name__ == "__main__":
    main()