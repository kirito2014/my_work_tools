from flask import Flask, render_template, request, send_from_directory, jsonify
import os
from werkzeug.utils import secure_filename
import threading
from time import sleep
import openpyxl

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output'
lock = threading.Lock()
processing = False
progress = 0

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

def run_script(txt_path, xlsx_path, output_path):
    global progress
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    with open(txt_path, 'r') as f:
        table_names = [line.strip() for line in f]

    for i, table_name in enumerate(table_names, 1):
        sleep(1)
        progress = int((i / len(table_names)) * 100)
    
    wb.save(output_path)
    progress = 100

@app.route('/')
def index():
    return render_template('template1.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file and file.filename.startswith('table_list-') and file.filename.endswith('.txt'):
        filename = secure_filename(file.filename)
        txt_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(txt_path)
        with open(txt_path, 'r') as f:
            lines = f.readlines()
        return jsonify({'status': 'success', 'file_type': 'txt', 'filename': filename, 'content': lines})
    
    elif file and file.filename.startswith('pub_cd_map-') and file.filename.endswith('.xlsx'):
        filename = secure_filename(file.filename)
        xlsx_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(xlsx_path)
        return jsonify({'status': 'success', 'file_type': 'xlsx', 'filename': filename})
    
    return jsonify({'status': 'error', 'message': 'Invalid file format'})

@app.route('/run', methods=['POST'])
def run():
    global processing, progress
    if processing:
        return jsonify({'status': 'error', 'message': 'Another process is running. Please wait.'})

    txt_file = request.json['txt_filename']
    xlsx_file = request.json['xlsx_filename']
    txt_path = os.path.join(app.config['UPLOAD_FOLDER'], txt_file)
    xlsx_path = os.path.join(app.config['UPLOAD_FOLDER'], xlsx_file)
    output_path = os.path.join(app.config['OUTPUT_FOLDER'], 'pub_cd_map.xlsx')

    if not os.path.exists(txt_path) or not os.path.exists(xlsx_path):
        return jsonify({'status': 'error', 'message': 'Required files are missing'})

    processing = True
    progress = 0
    threading.Thread(target=execute_script, args=(txt_path, xlsx_path, output_path)).start()
    return jsonify({'status': 'success'})

def execute_script(txt_path, xlsx_path, output_path):
    global processing
    try:
        run_script(txt_path, xlsx_path, output_path)
    finally:
        processing = False

@app.route('/progress')
def get_progress():
    return jsonify({'progress': progress})

@app.route('/download')
def download():
    return send_from_directory(app.config['OUTPUT_FOLDER'], 'pub_cd_map.xlsx', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
