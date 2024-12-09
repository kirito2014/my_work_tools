from PyQt6.QtGui import QSyntaxHighlighter, QTextCharFormat
from PyQt6.QtCore import Qt
import re

class PythonHighlighter(QSyntaxHighlighter):
  def __init__(self, parent=None):
    super().__init__(parent)
    self.highlighting_rules = []
    
    # 关键字高亮
    keyword_format = QTextCharFormat()
    keyword_format.setForeground(Qt.GlobalColor.blue)
    keywords = [
      "def", "class", "for", "if", "else", "in", "import",
      "return", "True", "False", "None", "and", "or", "not"
    ]
    
    for word in keywords:
      pattern = r'\b' + word + r'\b'
      self.highlighting_rules.append(
        (pattern, keyword_format.clone())
      )
      
    # 字符串高亮
    string_format = QTextCharFormat()
    string_format.setForeground(Qt.GlobalColor.darkGreen)
    self.highlighting_rules.append(
      (r'"[^"\\]*(\\.[^"\\]*)*"', string_format.clone())
    )
    self.highlighting_rules.append(
      (r"'[^'\\]*(\\.[^'\\]*)*'", string_format.clone())
    )
    
  def highlightBlock(self, text):
    for pattern, format in self.highlighting_rules:
      for match in re.finditer(pattern, text):
        self.setFormat(match.start(), match.end() - match.start(), format)