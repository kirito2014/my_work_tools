from flask import Flask, render_template, request, jsonify
import os
import subprocess

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/run-script', methods=['POST'])
def run_script():
    folder_path = request.form.get('folderPath')
    file_path = request.form.get('filePath')

    if not folder_path or not os.path.isdir(folder_path):
        return jsonify({"error": "请选择有效的文件夹"}), 400

    if not file_path or not os.path.isfile(file_path):
        return jsonify({"error": "请选择有效的目标文件"}), 400

    try:
        # 替换为实际的 Python 脚本路径和命令
        command = f'python case_collector.py "{folder_path}" "{file_path}"'
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({"output": result.stdout, "error": result.stderr}), 500
        return jsonify({"output": result.stdout})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
