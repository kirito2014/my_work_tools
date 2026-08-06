#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
简历预览服务模块（独立模块，不依赖/不修改 resume_gui.py 的内部实现）

功能：
  在本地启动一个只监听 127.0.0.1 的HTTP服务，用于在浏览器中预览
  "选定的简历模板 + 随机一位人员" 的真实渲染效果（docx -> PDF -> 网页内嵌）。

设计说明：
  - 完全复用 render_from_docx.generate_resume_from_json() 做真实渲染，
    保证预览效果和正式生成的简历完全一致（包括 key_map_convert 长短键转换步骤）。
  - docx -> pdf 转换使用 Word COM（与项目里 doc_converter.py 相同的技术方案），
    不引入任何新的第三方依赖。仅在实际转换时才 import comtypes，
    因此即使当前环境没有装 comtypes / Word，本模块本身也可以正常被导入，
    只有在真正预览时才会报错提示。
  - 只依赖 Python 标准库（http.server）作为服务器，无需安装 Flask 等额外包，
    打包 exe 时不需要处理额外依赖。
"""

import os
import sys
import json
import random
import threading
import traceback
import importlib.util
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


# ============================================================
# 动态模块加载（与主项目现有的三级导入方式保持一致，保证独立可用）
# ============================================================
def _load_module(module_name, filename, base_dir):
    """依次尝试：包路径导入 -> 直接导入 -> 从文件动态加载"""
    try:
        return __import__(f"package.functions.{module_name}", fromlist=[module_name])
    except ImportError:
        pass
    try:
        return __import__(module_name)
    except ImportError:
        pass
    try:
        file_path = os.path.join(base_dir, 'package', 'functions', filename)
        if os.path.exists(file_path):
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            return module
    except Exception as e:
        print(f"[preview_server] 动态加载模块 {module_name} 失败: {e}")
    return None


# ============================================================
# 预览上下文：路径、已加载模块、核心业务逻辑
# ============================================================
class PreviewContext:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.template_dir = os.path.join(base_dir, "template")
        self.modify_json_dir = os.path.join(base_dir, "output", "modify_json")
        self.temp_dir = os.path.join(base_dir, "temp", "preview")
        os.makedirs(self.temp_dir, exist_ok=True)

        # 复用主项目已有的渲染模块和键值转换模块
        self.render_module = _load_module("render_from_docx", "render_from_docx.py", base_dir)
        self.key_map_convert = _load_module("key_map_convert", "key_map_convert.py", base_dir)

    # ---------------- 数据相关 ----------------
    def get_available_banks(self):
        """扫描template目录，返回所有可用的docx模板对应的银行名称"""
        banks = []
        suffix = "_简历模板.docx"
        if os.path.isdir(self.template_dir):
            for f in os.listdir(self.template_dir):
                if f.endswith(suffix):
                    banks.append(f[: -len(suffix)])
        return sorted(banks)

    def pick_random_person(self):
        """从 output/modify_json 目录中随机挑选一位人员的数据"""
        if not os.path.isdir(self.modify_json_dir):
            raise FileNotFoundError(f"JSON目录不存在: {self.modify_json_dir}，请先解析简历入库")

        json_files = [f for f in os.listdir(self.modify_json_dir) if f.endswith(".json")]
        if not json_files:
            raise FileNotFoundError("output/modify_json 目录下没有任何JSON文件，请先解析简历入库")

        random.shuffle(json_files)
        for fname in json_files:
            try:
                with open(os.path.join(self.modify_json_dir, fname), "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data:
                    person_name = random.choice(list(data.keys()))
                    return person_name, data[person_name]
            except Exception:
                continue
        raise ValueError("modify_json 目录下的JSON文件均为空或无法解析")

    # ---------------- 渲染相关 ----------------
    def render_preview_pdf(self, bank_name):
        """
        渲染 "指定模板 + 随机一位人员" 的简历，返回 (pdf_path, person_name)
        渲染链路与批量生成完全一致：JSON数据 -> (可选)key_map_convert长短键转换
        -> render_from_docx.generate_resume_from_json -> docx -> PDF
        """
        if self.render_module is None or not hasattr(self.render_module, "generate_resume_from_json"):
            raise RuntimeError("未能加载 render_from_docx 模块，无法渲染预览")

        template_path = os.path.join(self.template_dir, f"{bank_name}_简历模板.docx")
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"未找到该银行的docx模板: {template_path}")

        person_name, person_data = self.pick_random_person()

        # 与 batch_render_from_docx.py 中的逻辑保持一致：优先做长键转短键
        render_data = person_data
        if self.key_map_convert and hasattr(self.key_map_convert, "convert_resume_data"):
            try:
                converted = self.key_map_convert.convert_resume_data({person_name: person_data})
                render_data = converted.get(person_name, person_data)
            except Exception as e:
                print(f"[preview_server] key_map_convert转换失败，使用原始数据渲染: {e}")

        docx_path = self.render_module.generate_resume_from_json(
            render_data,
            template_path,
            self.temp_dir,
            f"预览_{person_name}",
            bank_name,
        )
        if not docx_path or not os.path.exists(docx_path):
            raise RuntimeError("docx渲染失败，详情请查看后台控制台日志")

        # Word COM不支持并发调用，整个转换过程加锁串行化；
        # 多个预览请求同时到达时会排队依次处理，而不是同时创建多个Word实例
        with _RENDER_LOCK:
            pdf_path = _convert_docx_to_pdf(docx_path, self.temp_dir)
        return pdf_path, person_name


_RENDER_LOCK = threading.Lock()


def _convert_docx_to_pdf(docx_path, output_dir):
    """
    使用 Word COM 将 docx 转换为 pdf。
    与 doc_converter.py 使用同一套技术方案（comtypes + Word.Application），
    但独立实现，不 import/依赖 doc_converter.py，避免任何交叉影响。

    注意：Word COM 不支持多线程并发调用，本函数调用方必须持有 _RENDER_LOCK。
    """
    import comtypes
    import comtypes.client  # 延迟导入：仅在真正需要转换时才要求环境已安装Word/comtypes

    pdf_path = os.path.join(output_dir, os.path.splitext(os.path.basename(docx_path))[0] + ".pdf")
    if os.path.exists(pdf_path):
        try:
            os.remove(pdf_path)
        except Exception:
            pass

    # ThreadingHTTPServer每个请求都在新线程里执行，COM单元状态不会自动继承，
    # 必须在当前线程显式初始化COM，否则CreateObject可能报错或行为异常
    comtypes.CoInitialize()
    word = None
    try:
        word = comtypes.client.CreateObject("Word.Application")
        word.Visible = False
        doc = word.Documents.Open(os.path.abspath(docx_path))
        doc.SaveAs(os.path.abspath(pdf_path), FileFormat=17)  # 17 = wdFormatPDF
        doc.Close(SaveChanges=False)
    except Exception as e:
        raise RuntimeError(f"docx转PDF失败（请确认本机已安装Word）: {e}")
    finally:
        if word is not None:
            try:
                word.Quit(SaveChanges=False)
            except Exception:
                pass
        comtypes.CoUninitialize()

    if not os.path.exists(pdf_path):
        raise RuntimeError("PDF文件未生成，转换可能失败")
    return pdf_path


def _warmup_word_com():
    """
    服务启动后在后台线程预热一次Word COM，提前触发comtypes typelib生成，
    避免用户第一次点预览时卡在这个一次性的耗时初始化上。失败静默忽略。
    """
    def _run():
        try:
            import comtypes
            import comtypes.client
            comtypes.CoInitialize()
            try:
                word = comtypes.client.CreateObject("Word.Application")
                word.Visible = False
                word.Quit(SaveChanges=False)
                print("[preview_server] Word COM 预热完成")
            finally:
                comtypes.CoUninitialize()
        except Exception as e:
            print(f"[preview_server] Word COM 预热失败（不影响后续正常使用，仅首次预览会稍慢）: {e}")

    threading.Thread(target=_run, daemon=True).start()


# ============================================================
# 网页模板
# ============================================================
_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>简历模板预览</title>
<style>
  body {{ font-family: "Microsoft YaHei", "微软雅黑", sans-serif; margin: 0; padding: 16px; background:#f5f6fa; }}
  h2 {{ margin: 0 0 12px 0; color: #333; font-size: 18px; }}
  .toolbar {{ display:flex; align-items:center; gap:12px; margin-bottom:12px; flex-wrap: wrap; }}
  select, button {{ padding:6px 12px; font-size:14px; border-radius:4px; border:1px solid #ccc; }}
  button {{ cursor:pointer; background:#4a90e2; color:#fff; border:none; }}
  button:hover {{ background:#357abd; }}
  iframe {{ width:100%; height:82vh; border:1px solid #ddd; background:#fff; border-radius:4px; }}
  #status {{ color:#888; margin-left:4px; }}
  #personName {{ color: #4a90e2; font-weight: bold; }}
</style>
</head>
<body>
  <h2>简历模板预览（随机人员渲染效果）</h2>
  <div class="toolbar">
    <label>选择简历模板：</label>
    <select id="bankSelect">{options}</select>
    <button id="reloadBtn" onclick="reload()">换一个人预览</button>
    <span id="status"></span>
    <span id="personName"></span>
  </div>
  <iframe id="previewFrame" src="about:blank"></iframe>
  <script>
    var currentController = null;
    var currentBlobUrl = null;

    function reload() {{
      var bank = document.getElementById('bankSelect').value;
      if (!bank) {{ return; }}

      // 主动取消上一个还没完成的请求，避免并发触发多个Word COM调用
      if (currentController) {{
        currentController.abort();
      }}
      currentController = new AbortController();

      var btn = document.getElementById('reloadBtn');
      btn.disabled = true;
      document.getElementById('status').innerText = '正在渲染，请稍候（首次预览可能需要几十秒）...';
      document.getElementById('personName').innerText = '';

      var url = '/render_pdf?bank=' + encodeURIComponent(bank) + '&r=' + Date.now();
      fetch(url, {{ signal: currentController.signal }})
        .then(function(resp) {{
          if (!resp.ok) {{ throw new Error('服务器返回状态码 ' + resp.status); }}
          var nameHex = resp.headers.get('X-Person-Name');
          if (nameHex) {{
            try {{
              var bytes = nameHex.match(/.{{2}}/g).map(function(h) {{ return parseInt(h, 16); }});
              var name = new TextDecoder('utf-8').decode(new Uint8Array(bytes));
              document.getElementById('personName').innerText = '当前预览人员：' + name;
            }} catch (e) {{}}
          }}
          return resp.blob();
        }})
        .then(function(blob) {{
          if (currentBlobUrl) {{ URL.revokeObjectURL(currentBlobUrl); }}
          currentBlobUrl = URL.createObjectURL(blob);
          document.getElementById('previewFrame').src = currentBlobUrl;
          document.getElementById('status').innerText = '';
          btn.disabled = false;
        }})
        .catch(function(err) {{
          if (err.name === 'AbortError') {{ return; }} // 被新请求取消，无需提示
          document.getElementById('status').innerText = '渲染失败: ' + err.message;
          btn.disabled = false;
        }});
    }}
    document.getElementById('bankSelect').addEventListener('change', reload);
    window.onload = function() {{
      if (document.getElementById('bankSelect').value) {{ reload(); }}
    }};
  </script>
</body>
</html>
"""

_ERROR_PAGE = """<html><body style="font-family:sans-serif;color:#c0392b;padding:24px;">
<h3>预览渲染失败</h3><p>{message}</p></body></html>"""

_EMPTY_PAGE = """<html><body style="font-family:sans-serif;color:#888;padding:24px;">
<p>未在 template 目录下找到任何 "xxx_简历模板.docx" 文件，请先配置模板。</p></body></html>"""


# ============================================================
# HTTP服务
# ============================================================
def _make_handler(ctx: "PreviewContext"):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # 静音默认访问日志，避免刷屏主程序控制台

        def do_GET(self):
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            try:
                if parsed.path == "/":
                    self._serve_index(qs)
                elif parsed.path == "/render_pdf":
                    self._serve_pdf(qs)
                else:
                    self.send_error(404, "Not Found")
            except Exception as e:
                traceback.print_exc()
                self._send_html(500, _ERROR_PAGE.format(message=str(e)))

        def _serve_index(self, qs):
            banks = ctx.get_available_banks()
            if not banks:
                self._send_html(200, _EMPTY_PAGE)
                return
            default_bank = qs.get("bank", [banks[0]])[0]
            if default_bank not in banks:
                default_bank = banks[0]
            options_html = "".join(
                '<option value="{b}"{sel}>{b}</option>'.format(
                    b=b, sel=' selected' if b == default_bank else ''
                )
                for b in banks
            )
            html = _PAGE_TEMPLATE.format(options=options_html)
            self._send_html(200, html)

        def _serve_pdf(self, qs):
            bank_name = qs.get("bank", [""])[0]
            if not bank_name:
                self._send_html(400, _ERROR_PAGE.format(message="缺少 bank 参数"))
                return
            try:
                pdf_path, person_name = ctx.render_preview_pdf(bank_name)
                with open(pdf_path, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/pdf")
                self.send_header("Content-Disposition", 'inline; filename="preview.pdf"')
                self.send_header("Content-Length", str(len(body)))
                # 姓名可能含中文，HTTP header需要用latin-1安全的编码方式传递
                self.send_header("X-Person-Name", person_name.encode("utf-8").hex())
                self.end_headers()
                self.wfile.write(body)
            except (ConnectionAbortedError, BrokenPipeError, ConnectionResetError):
                # 客户端中途取消了请求（比如又点了一次"换一个人"或刷新了页面），属正常现象，无需报错
                print("[preview_server] 客户端提前断开了连接（通常是重复请求导致，可忽略）")
            except Exception as e:
                traceback.print_exc()
                try:
                    self._send_html(200, _ERROR_PAGE.format(message=str(e)))
                except Exception:
                    pass

        def _send_html(self, status, html):
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


_server_instance = None
_server_thread = None
_server_port = None


def start_server_background(base_dir, preferred_port=8899, max_attempts=10):
    """
    在后台守护线程中启动预览服务器。
    幂等：重复调用不会重复启动，直接返回已启动的端口。
    自动处理端口占用：从 preferred_port 开始，最多尝试 max_attempts 次递增端口。
    """
    global _server_instance, _server_thread, _server_port
    if _server_instance is not None:
        return _server_port

    ctx = PreviewContext(base_dir)
    handler_cls = _make_handler(ctx)

    last_err = None
    for i in range(max_attempts):
        port = preferred_port + i
        try:
            _server_instance = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
            _server_port = port
            break
        except OSError as e:
            last_err = e
            continue
    else:
        print(f"[preview_server] 启动失败，端口 {preferred_port}-{preferred_port + max_attempts - 1} 均被占用: {last_err}")
        return None

    _server_thread = threading.Thread(target=_server_instance.serve_forever, daemon=True)
    _server_thread.start()
    print(f"[preview_server] 预览服务已在后台启动: http://127.0.0.1:{_server_port}/")

    # 后台预热一次Word COM，避免用户第一次点预览时卡在typelib生成上
    _warmup_word_com()

    return _server_port


def get_server_port():
    return _server_port


def get_preview_url(bank_name=None):
    if _server_port is None:
        return None
    url = f"http://127.0.0.1:{_server_port}/"
    if bank_name:
        url += f"?bank={bank_name}"
    return url