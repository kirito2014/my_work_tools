from flask import Flask, render_template, request, send_from_directory, jsonify
import os,sys
from werkzeug.utils import secure_filename
import threading
from time import sleep
import openpyxl
import pandas as pd 
import xlwings as xw 

    
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output'
lock = threading.Lock()
processing = False
progress = 0

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False



def run_script(txt_path, xlsx_path, output_path): 

    #print(txt_path, xlsx_path, output_path)
    #sys.exit()
    global progress
    excel_app = xw.App(visible=False) 
    person_name=os.path.splitext(os.path.basename(txt_path))[0].split('-')[-1] 

    error_log=f"error_log_{person_name}.txt" 
    #output目录拼接
    #print(txt_path)
    table_list_file=txt_path
    #print(table_list_file)
    target_file=output_path
    #print(target_file)
    
    if os.path.exists(target_file):
        tgt_wb=xw.Book(target_file) 
    else: 
        tgt_wb=xw.Book()
        tgt_wb.save(target_file) 
    
    with open(table_list_file,'r',encoding='utf-8') as file: 
        table_names = [line.strip().upper() for line in file.readlines()]
        table_list=len(table_names) 
    print(f"本次共处理{table_list}张表") 
    processed_table_count=0 
    
    for table_name in table_names: 
        print(f"正在处理<{person_name}>-<{table_name}>的码值映射。") 
        # code_map_files = [ 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xlsx'), 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xls'), 
        #     os.path.join('uploads',f'pub_cd_map-{person_name}.xlsm') 
        #     ]
        
        #code_map_file = next((file for file in code_map_files if os.path.exists(file)),None)
        code_map_file = xlsx_path
        #print(code_map_files)
        if not code_map_file: 
            log_error(error_log,f"{person_name}的代码映射文件不存在.") 
            sys.exit()         
        try: 
            src_wb=xw.Book(code_map_file)
            rem_code_map_sheet='rem-代码映射' 
            if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]: 
                log_error(error_log,f"{code_map_file}中未找到代码映射sheet。") 
                src_wb.close() 
                sys.exit()

            src_cm_sheet=src_wb.sheets[rem_code_map_sheet] 
            clear_filters(src_cm_sheet)
            if rem_code_map_sheet not in [sheet.name for sheet in tgt_wb.sheets]: 
                tgt_wb.sheets.add(rem_code_map_sheet) 

            tgt_cm_sheet=tgt_wb.sheets(rem_code_map_sheet) 
            clear_filters(tgt_cm_sheet)
            #先删除目标文件中已存在的码值映射
            tgt_cm_data=tgt_cm_sheet.range('A1').expand('table').value 
            if tgt_cm_data: 
                tgt_cm_df=pd.DataFrame(tgt_cm_data[1:],columns=tgt_cm_data[1]) 
                tgt_cm_df=tgt_cm_df[tgt_cm_df['目标表英文名'] != table_name] 
                tgt_cm_sheet.clear_contents()
                tgt_cm_sheet.range('A1').value = [tgt_cm_data[0]] + tgt_cm_df.values.tolist()
            #读取并筛选源文件中的码值映射数据
            src_cm_data=src_cm_sheet.range('A1').expand('table').value 
            if src_cm_data: 
                src_cm_df =pd.DataFrame(src_cm_data[1:],columns=src_cm_data[1]) 
                filtered_src_cm_df=src_cm_df[src_cm_df['目标表英文名']==table_name] 
                if filtered_src_cm_df.empty: 
                    log_error(error_log,f"{code_map_file}中未找到{table_name}表的代码映射.")
                else:
                    start_row = tgt_cm_sheet.range('A1').expand('down').last_cell.row + 1
                    tgt_cm_sheet.range(f'A{start_row}').value = filtered_src_cm_df.values.tolist()
            tgt_wb.save()
        except Exception as e:
            log_error(error_log,f"处理{code_map_file}中{table_name} 表的代码映射时发生错误:{str(e)}.")
        finally:
            src_wb.close()
            #tgt_wb.close()
        processed_table_count = processed_table_count + 1
        progress = int((processed_table_count / len(table_names)) * 100)
        print(f"剩余{table_list-processed_table_count}个")
    tgt_wb.save(target_file)
    tgt_wb.close()
    progress = 100
    print('110000101010')

def log_error(log_file,message):
    with open(log_file,'a',encoding='utf-8') as log:
        log.write(message + '\n')
    print(message)


def run_script1(txt_path, xlsx_path, output_path):
    global progress
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active

    with open(txt_path, 'r',encoding='gbk') as f:
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
    app.run(host='0.0.0.0',port=5000,debug=True)
    #run_script()