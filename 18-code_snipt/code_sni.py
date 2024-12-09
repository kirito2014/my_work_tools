from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, 
              QVBoxLayout, QHBoxLayout, QPushButton, 
              QLineEdit, QTextEdit, QComboBox, QStatusBar)
from PyQt6.QtCore import Qt
import sys
import json
import time

class SnippetManager(QMainWindow):
  def __init__(self):
    super().__init__()
    self.setWindowTitle("代码片段管理器 V1.0")
    self.setGeometry(100, 100, 800, 600)
    
    # 主布局
    main_widget = QWidget()
    self.setCentralWidget(main_widget)
    layout = QVBoxLayout(main_widget)
    
    # 顶部工具栏
    toolbar = QHBoxLayout()
    
    # 分类选择
    self.category_combo = QComboBox()
    self.category_combo.addItems(["Python", "SQL", "HTML", "其他"])
    toolbar.addWidget(self.category_combo)
    
    # 搜索框
    self.search_box = QLineEdit()
    self.search_box.setPlaceholderText("搜索代码片段...")
    toolbar.addWidget(self.search_box)
    
    layout.addLayout(toolbar)
    
    # 代码编辑区
    self.code_edit = QTextEdit()
    self.code_edit.setPlaceholderText("在这里输入你的代码...")
    layout.addWidget(self.code_edit)
    
    # 底部按钮
    button_layout = QHBoxLayout()
    
    self.save_btn = QPushButton("保存片段")
    self.copy_btn = QPushButton("复制代码")
    button_layout.addWidget(self.save_btn)
    button_layout.addWidget(self.copy_btn)
    
    layout.addLayout(button_layout)
    
    # Status bar setup
    self.status_bar = QStatusBar()
    self.setStatusBar(self.status_bar)
    
    # 绑定事件
    self.setup_connections()
    
  def setup_connections(self):
    self.save_btn.clicked.connect(self.save_snippet)
    self.copy_btn.clicked.connect(self.copy_snippet)
    self.search_box.textChanged.connect(self.search_snippets)
    # 初始化数据存储
    self.snippets = self.load_snippets()
  def save_snippet(self):
    code = self.code_edit.toPlainText()
    category = self.category_combo.currentText()
    
    if not code.strip():
      self.show_status("代码内容不能为空！")
      return
      
    # 生成唯一ID（这里用时间戳简单处理）
    snippet_id = str(int(time.time()))
    
    # 保存代码片段
    self.snippets[snippet_id] = {
      "code": code,
      "category": category,
      "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # 保存到文件
    self.save_to_file()
    self.show_status("代码片段保存成功！")
    
  def copy_snippet(self):
    code = self.code_edit.toPlainText()
    if code:
      clipboard = QApplication.clipboard()
      clipboard.setText(code)
      self.show_status("代码已复制到剪贴板！")
      
  def search_snippets(self):
    keyword = self.search_box.text().lower()
    # 实现实时搜索
    matched_snippets = []
    for id, snippet in self.snippets.items():
      if keyword in snippet["code"].lower():
        matched_snippets.append(snippet["code"])
        
    # 显示搜索结果
    self.code_edit.setText("\n\n".join(matched_snippets))
    
  def load_snippets(self):
    try:
      with open("snippets.json", "r", encoding="utf-8") as f:
        return json.load(f)
    except FileNotFoundError:
      return {}
      
  def save_to_file(self):
    with open("snippets.json", "w", encoding="utf-8") as f:
      json.dump(self.snippets, f, ensure_ascii=False, indent=2)

  def show_status(self, message):
    """Display a status message."""
    self.status_bar.showMessage(message, 5000)  # Display the message for 5000 ms

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = SnippetManager()
    window.show()
    sys.exit(app.exec())