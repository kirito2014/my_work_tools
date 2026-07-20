#!/usr/bin/env python3
# -*- coding:utf-8 -*-

"""
作者: zjj/gsp
日期: 20240701
功能: ETL 脚本统一图形调用工具（配置驱动，动态参数表单）

━━━ 如何新增脚本 ━━━
在下方 SCRIPTS 字典中追加一项，每个参数用字典描述：
  key       唯一标识（用于取值，不显示）
  label     界面显示的标签文字
  type      参数类型：combo / file / folder / text / date
  required  True=必填，False=可选（可选项不填则不传入命令行）
  flag      None=按顺序作为位置参数；"--xxx"=作为具名参数传入
  options   type=combo 时的下拉选项列表
  filetypes type=file 时的文件过滤器，格式同 filedialog
  default   默认值
  hint      输入框内的灰色提示文字（placeholder）
  validate  "date"=校验 YYYY-MM-DD 格式；None=不校验

依赖安装：pip install ttkthemes
"""

import os, sys, re, queue, threading, subprocess, tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import date as _date

try:
    from ttkthemes import ThemedTk
except ImportError:
    print("未检测到 ttkthemes，请先执行: pip install ttkthemes")
    sys.exit(1)


# ╔══════════════════════════════════════════════════════╗
# ║                  脚本配置（按需修改）                 ║
# ╚══════════════════════════════════════════════════════╝
_BASE = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = {
    "AE模板SQL生成": {
        "path": os.path.join(_BASE, "gen_ae_sql.py"),
        "desc": "根据 Excel 模板生成 AE 层 SQL 文件，输出至 ${ETL_HOME}/autocode/dml/agl|exl/",
        "params": [
            {
                "key": "level",
                "label": "归属层级",
                "type": "combo",
                "options": ["agl", "exl"],
                "required": True,
                "flag": None,
                "default": "agl",
            },
            {
                "key": "template",
                "label": "模板文件",
                "type": "file",
                "filetypes": [("Excel 文件", "*.xlsx *.xls *.xlsm"), ("所有文件", "*.*")],
                "required": True,
                "flag": None,
                "default": "",
                "hint": "请选择 Excel 模板文件",
            },
        ],
    },

    "AE迁移SQL生成": {
        "path": os.path.join(_BASE, "gen_ae_migration_sql.py"),
        "desc": "根据 Excel 模板及日期范围生成 AE 迁移 SQL 文件",
        "params": [
            {
                "key": "level",
                "label": "归属层级",
                "type": "combo",
                "options": ["agl", "exl"],
                "required": True,
                "flag": None,
                "default": "agl",
            },
            {
                "key": "template",
                "label": "模板文件",
                "type": "file",
                "filetypes": [("Excel 文件", "*.xlsx *.xls *.xlsm"), ("所有文件", "*.*")],
                "required": True,
                "flag": None,
                "default": "",
                "hint": "请选择 Excel 模板文件",
            },
            {
                "key": "start_date",
                "label": "开始日期",
                "type": "date",
                "required": True,
                "flag": None,
                "default": _date.today().strftime("%Y-%m-%d"),
                "hint": "YYYY-MM-DD",
            },
            {
                "key": "end_date",
                "label": "结束日期",
                "type": "date",
                "required": True,
                "flag": None,
                "default": _date.today().strftime("%Y-%m-%d"),
                "hint": "YYYY-MM-DD",
            },
        ],
    },

    "获取依赖表(AGL)": {
        # path 支持绝对路径，例如其他项目目录下的脚本
        "path": r"D:\other_project\scripts\get_rely_table_agl.py",
        "desc": "扫描指定文件夹下的 HQL/SQL 脚本，分析并输出表依赖关系",
        "params": [
            {
                "key": "script_dir",
                "label": "脚本文件夹",
                "type": "folder",
                "required": True,
                "flag": None,
                "default": "",
                "hint": "包含待分析 HQL/SQL 文件的目录",
            },
            {
                "key": "output",
                "label": "输出文件（可选）",
                "type": "file_save",          # 另存为对话框
                "filetypes": [("文本文件", "*.txt"), ("CSV", "*.csv"), ("所有文件", "*.*")],
                "required": False,
                "flag": "--output",           # 具名参数，不填则不传
                "default": "",
                "hint": "不填则直接打印到日志",
            },
            {
                "key": "depth",
                "label": "解析深度（可选）",
                "type": "text",
                "required": False,
                "flag": "--depth",
                "default": "",
                "hint": "整数，默认无限深度",
                "validate": "number",
            },
        ],
    },
}
# ╔══════════════════════════════════════════════════════╗
# ║                     界面实现                          ║
# ╚══════════════════════════════════════════════════════╝

# 参数类型对应的图标前缀（仅用于标签可读性）
_TYPE_ICON = {
    "combo":     "▾ ",
    "file":      "📄 ",
    "file_save": "💾 ",
    "folder":    "📁 ",
    "text":      "✏ ",
    "date":      "📅 ",
}


class ParamWidget:
    """单个参数的控件封装：标签 + 输入控件 + 可选的浏览按钮"""

    def __init__(self, parent_frame: ttk.Frame, cfg: dict, row: int):
        self.cfg = cfg
        self.var = tk.StringVar(value=cfg.get("default", ""))
        ptype = cfg["type"]
        required = cfg.get("required", True)
        icon  = _TYPE_ICON.get(ptype, "")

        req_mark = "" if required else "  (可选)"
        label_text = icon + cfg["label"] + req_mark + "："

        # 标签
        self.lbl = ttk.Label(parent_frame, text=label_text)
        self.lbl.grid(row=row, column=0, sticky="w", pady=6, padx=(0, 6))

        # 控件容器
        self.container = ttk.Frame(parent_frame)
        self.container.grid(row=row, column=1, sticky="ew", padx=(4, 0), pady=6)
        self.container.columnconfigure(0, weight=1)

        # 根据类型构建输入控件
        if ptype == "combo":
            self.widget = ttk.Combobox(
                self.container,
                textvariable=self.var,
                values=cfg.get("options", []),
                state="readonly",
                width=24,
            )
            self.widget.grid(row=0, column=0, sticky="w")

        elif ptype in ("file", "file_save", "folder"):
            self.widget = ttk.Entry(self.container, textvariable=self.var)
            self.widget.grid(row=0, column=0, sticky="ew")
            btn_text = "另存为..." if ptype == "file_save" else ("选择文件夹..." if ptype == "folder" else "浏览...")
            ttk.Button(self.container, text=btn_text,
                       command=self._browse).grid(row=0, column=1, padx=(8, 0))
            self._set_hint()

        elif ptype in ("text", "date"):
            self.widget = ttk.Entry(self.container, textvariable=self.var)
            self.widget.grid(row=0, column=0, sticky="ew")
            self._set_hint()

    # ── placeholder 模拟 ──
    def _set_hint(self):
        hint = self.cfg.get("hint", "")
        if not hint:
            return
        if not self.var.get():
            self.widget.insert(0, hint)
            self.widget.config(foreground="#aaaaaa")
        self.widget.bind("<FocusIn>",  self._on_focus_in)
        self.widget.bind("<FocusOut>", self._on_focus_out)

    def _on_focus_in(self, _e):
        hint = self.cfg.get("hint", "")
        if self.widget.get() == hint:
            self.widget.delete(0, tk.END)
            self.widget.config(foreground="")
            self.var.set("")

    def _on_focus_out(self, _e):
        hint = self.cfg.get("hint", "")
        if not self.widget.get():
            self.widget.insert(0, hint)
            self.widget.config(foreground="#aaaaaa")

    def _browse(self):
        ptype = self.cfg["type"]
        if ptype == "folder":
            result = filedialog.askdirectory(title="选择文件夹")
        elif ptype == "file_save":
            result = filedialog.asksaveasfilename(
                title="保存到",
                filetypes=self.cfg.get("filetypes", [("所有文件", "*.*")]),
            )
        else:
            result = filedialog.askopenfilename(
                title="选择文件",
                filetypes=self.cfg.get("filetypes", [("所有文件", "*.*")]),
            )
        if result:
            self.widget.config(foreground="")
            self.var.set(result)

    def get_value(self) -> str:
        """获取真实值（剔除占位提示文字）"""
        val = self.var.get()
        if val == self.cfg.get("hint", ""):
            return ""
        return val.strip()

    def validate(self) -> str | None:
        """返回 None 表示通过，返回字符串则为错误信息"""
        val = self.get_value()
        required = self.cfg.get("required", True)
        label    = self.cfg["label"]
        ptype    = self.cfg["type"]

        if not val:
            if required:
                return "「%s」为必填项，请填写后再执行！" % label
            return None  # 可选且为空，跳过

        if ptype in ("file",) and not os.path.isfile(val):
            return "「%s」指定的文件不存在：\n%s" % (label, val)
        if ptype == "folder" and not os.path.isdir(val):
            return "「%s」指定的文件夹不存在：\n%s" % (label, val)

        rule = self.cfg.get("validate")
        if rule == "date":
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                return "「%s」日期格式错误，请使用 YYYY-MM-DD" % label
        if rule == "number":
            if not re.match(r"^\d+$", val):
                return "「%s」请输入整数" % label

        return None

    def to_cmd_args(self) -> list[str]:
        """将本参数转换为命令行参数片段"""
        val  = self.get_value()
        flag = self.cfg.get("flag")
        if not val:
            return []
        if flag:
            return [flag, val]
        return [val]

    def set_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        combo_state = "readonly" if enabled else "disabled"
        if self.cfg["type"] == "combo":
            self.widget.config(state=combo_state)
        else:
            self.widget.config(state=state)
        for child in self.container.winfo_children():
            if isinstance(child, ttk.Button):
                child.config(state=state)

    def destroy(self):
        self.lbl.destroy()
        self.container.destroy()


class GenAeSqlGUI:
    def __init__(self, root: ThemedTk):
        self.root = root
        self.root.set_theme("arc")
        self.root.title("ETL 脚本工具集")
        self.root.geometry("900x680")
        self.root.minsize(740, 560)

        self.process       = None
        self.log_queue     = queue.Queue()
        self.is_running    = False
        self.user_stopped  = False
        self._param_widgets: list[ParamWidget] = []

        self._build_style()
        self._build_static_widgets()
        self._rebuild_param_form()
        self._poll_log_queue()

    # ── 样式 ──
    def _build_style(self):
        s = ttk.Style()
        s.configure("Title.TLabel",   font=("微软雅黑", 15, "bold"))
        s.configure("Desc.TLabel",    font=("微软雅黑", 9),  foreground="#555555")
        s.configure("TLabel",         font=("微软雅黑", 10))
        s.configure("TButton",        font=("微软雅黑", 10))
        s.configure("Run.TButton",    font=("微软雅黑", 11, "bold"))
        self._theme_bg = s.lookup("TFrame", "background") or "#f5f6f7"

    # ── 静态控件（不随脚本切换变化的部分）──
    def _build_static_widgets(self):
        outer = ttk.Frame(self.root, padding=20)
        outer.pack(fill=tk.BOTH, expand=True)
        self._outer = outer

        # 标题
        ttk.Label(outer, text="ETL 脚本工具集", style="Title.TLabel").pack(anchor="w")

        # 脚本选择
        sel_frame = ttk.LabelFrame(outer, text="脚本选择", padding=12)
        sel_frame.pack(fill=tk.X, pady=(10, 0))
        sel_frame.columnconfigure(1, weight=1)

        ttk.Label(sel_frame, text="执行脚本：").grid(row=0, column=0, sticky="w", pady=4)
        self.script_var = tk.StringVar(value=list(SCRIPTS.keys())[0])
        self.script_combo = ttk.Combobox(
            sel_frame,
            textvariable=self.script_var,
            values=list(SCRIPTS.keys()),
            state="readonly",
            width=30,
        )
        self.script_combo.grid(row=0, column=1, sticky="w", padx=(10, 0), pady=4)
        self.script_combo.bind("<<ComboboxSelected>>", lambda _e: self._rebuild_param_form())

        self.desc_var = tk.StringVar()
        ttk.Label(sel_frame, textvariable=self.desc_var,
                  style="Desc.TLabel", wraplength=700).grid(
            row=1, column=0, columnspan=2, sticky="w", pady=(2, 0)
        )

        # 参数区（LabelFrame 固定，内部 grid 动态重建）
        self.param_lf = ttk.LabelFrame(outer, text="参数设置", padding=15)
        self.param_lf.pack(fill=tk.X, pady=(12, 0))
        self.param_lf.columnconfigure(1, weight=1)

        # 按钮区
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

        # 日志区
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
        self.log_text.tag_config("cmd",     foreground="#9cdcfe")

    # ── 动态重建参数表单 ──
    def _rebuild_param_form(self):
        # 销毁旧控件
        for pw in self._param_widgets:
            pw.destroy()
        self._param_widgets.clear()

        key = self.script_var.get()
        cfg = SCRIPTS[key]
        self.desc_var.set(cfg["desc"])

        for row, param_cfg in enumerate(cfg["params"]):
            pw = ParamWidget(self.param_lf, param_cfg, row)
            self._param_widgets.append(pw)

    # ── 执行 ──
    def _on_run_clicked(self):
        if self.is_running:
            messagebox.showwarning("提示", "当前已有任务在执行，请等待完成后再操作。")
            return

        key    = self.script_var.get()
        cfg    = SCRIPTS[key]
        script = cfg["path"]

        if not os.path.exists(script):
            messagebox.showerror(
                "错误",
                "未找到脚本文件，请检查 SCRIPTS 中的 path 配置：\n%s" % script,
            )
            return

        # 逐参数校验
        for pw in self._param_widgets:
            err = pw.validate()
            if err:
                messagebox.showerror("参数错误", err)
                return

        # 额外的跨字段校验（如日期大小）
        date_params = {
            pw.cfg["key"]: pw.get_value()
            for pw in self._param_widgets
            if pw.cfg["type"] == "date" and pw.get_value()
        }
        if "start_date" in date_params and "end_date" in date_params:
            if date_params["start_date"] > date_params["end_date"]:
                messagebox.showerror("参数错误", "开始日期不能晚于结束日期！")
                return

        # 构建命令：位置参数按顺序，具名参数追加在后
        positional, named = [], []
        for pw in self._param_widgets:
            args = pw.to_cmd_args()
            if not args:
                continue
            if pw.cfg.get("flag"):
                named.extend(args)
            else:
                positional.extend(args)

        cmd = [sys.executable, "-u", script] + positional + named

        self._clear_log()
        self._append_log("执行命令: %s\n\n" % " ".join(cmd), "cmd")

        self.is_running   = True
        self.user_stopped = False
        self.run_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.script_combo.config(state="disabled")
        for pw in self._param_widgets:
            pw.set_state(False)
        self._set_status("执行中...", "#2e7d32")
        self.progress.start(12)

        threading.Thread(
            target=self._run_thread, args=(cmd, script), daemon=True
        ).start()

    def _run_thread(self, cmd: list, script: str):
        try:
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            env["PYTHONUTF8"]       = "1"

            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=os.path.dirname(os.path.abspath(script)),
                env=env,
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

    # ── 停止 ──
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

    # ── 日志轮询 ──
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
        for pw in self._param_widgets:
            pw.set_state(True)
        self.progress.stop()
        if self.user_stopped:
            self._set_status("已停止", "#c62828")
        else:
            self._set_status("就绪", "#1565c0")

    # ── 工具方法 ──
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