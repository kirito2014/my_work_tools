import sys
import os
import json
import subprocess
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QTextEdit, QFileDialog, QMessageBox, 
    QFormLayout, QDialog, QListWidget, QListWidgetItem, QComboBox, QInputDialog
)

class ConfigManager:
    def __init__(self, config_file='config/config.json'):
        self.config_file = config_file
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        self.load_config()

    def load_config(self):
        try:
            with open(self.config_file, 'r') as f:
                self.configs = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.configs = []

    def save_config(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.configs, f, indent=4)

    def add_script(self, script_config):
        self.configs.append(script_config)
        self.save_config()

class ScriptConfigDialog(QDialog):
    def __init__(self, parent=None, script=None):
        super().__init__(parent)
        self.script = script if script else {}
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("新增脚本配置")
        layout = QFormLayout()

        self.name_input = QLineEdit(self.script.get("name", ""))
        self.path_input = QLineEdit(self.script.get("path", ""))
        self.path_button = QPushButton("选择脚本文件")
        self.path_button.clicked.connect(self.browse_file)

        self.params_list = QListWidget()
        self.params_input = QLineEdit()
        self.param_type = QComboBox()
        self.param_type.addItems(["文本参数", "文件参数", "日期参数"])
        
        add_param_button = QPushButton("+")
        add_param_button.clicked.connect(self.add_param)

        layout.addRow("显示名称:", self.name_input)
        layout.addRow("脚本路径:", self.path_input)
        layout.addRow("", self.path_button)
        layout.addRow("脚本参数类型:", self.param_type)
        layout.addRow(self.params_input, add_param_button)
        layout.addRow("参数列表:", self.params_list)

        save_button = QPushButton("保存")
        save_button.clicked.connect(self.save_config)
        layout.addRow(save_button)

        self.setLayout(layout)

    def browse_file(self):
        filename, _ = QFileDialog.getOpenFileName(self, "选择脚本文件", "", "Python Files (*.py);;All Files (*.*)")
        if filename:
            self.path_input.setText(filename)

    def add_param(self):
        param_type = self.param_type.currentText()
        if param_type == "文件参数":
            param_name = f"选择文件{self.params_list.count() + 1}"
        else:
            param_name = self.params_input.text() or f"{param_type}{self.params_list.count() + 1}"

        if param_name:
            item = QListWidgetItem(f"{param_type}: {param_name}")
            self.params_list.addItem(item)
            self.params_input.clear()

    def save_config(self):
        self.script["name"] = self.name_input.text()
        self.script["path"] = self.path_input.text()
        self.script["params"] = [self.params_list.item(i).text() for i in range(self.params_list.count())]
        self.accept()

class ScriptManagerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.manager = ConfigManager()
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("Python 脚本管理程序")
        layout = QVBoxLayout()

        self.script_list = QListWidget()
        self.script_list.setFixedHeight(200)
        self.update_script_list()

        add_button = QPushButton("新增脚本")
        add_button.clicked.connect(self.add_script)

        self.run_button = QPushButton("执行脚本")
        self.run_button.clicked.connect(self.run_script)

        layout.addWidget(self.script_list)
        layout.addWidget(add_button)
        layout.addWidget(self.run_button)
        self.setLayout(layout)

        self.script_list.itemDoubleClicked.connect(self.handle_param_click)

    def update_script_list(self):
        self.script_list.clear()
        for idx, script in enumerate(self.manager.configs):
            item = QListWidgetItem(f"{idx + 1}. {script['name']} - {script['path']} (参数: {', '.join(script['params'])})")
            self.script_list.addItem(item)

    def add_script(self):
        dialog = ScriptConfigDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            self.manager.add_script(dialog.script)
            self.update_script_list()

    def run_script(self):
        current_script_index = self.script_list.currentRow()
        if current_script_index < 0:
            QMessageBox.warning(self, "警告", "请选择一个脚本配置！")
            return

        script = self.manager.configs[current_script_index]
        try:
            output = subprocess.check_output([f"python {script['path']}"] + script['params'], stderr=subprocess.STDOUT, shell=True)
            output = output.decode('utf-8')
            QMessageBox.information(self, "脚本输出", output)
        except subprocess.CalledProcessError as e:
            output = e.output.decode('utf-8')
            QMessageBox.critical(self, "错误", output)

    def handle_param_click(self, item):
        param = item.text().split(": ")
        param_type = param[0]
        param_value = param[1]

        if "文件参数" in param_type:
            filename, _ = QFileDialog.getOpenFileName(self, "选择文件", "", "All Files (*.*)")
            if filename:
                QMessageBox.information(self, "选择的文件", filename)
        elif "文本参数" in param_type:
            text, ok = QInputDialog.getText(self, "输入文本参数", "请输入文本：")
            if ok:
                QMessageBox.information(self, "输入的文本", text)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    manager = ScriptManagerApp()
    manager.resize(600, 400)
    manager.show()
    sys.exit(app.exec_())
