from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import os
import uuid
from docx import Document

# 初始化Flask应用
app = Flask(__name__,
            template_folder=os.path.join('package', 'html'),
            static_folder=os.path.join('package', 'html'))
CORS(app)

# 配置文件上传和输出目录
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), 'output')
for folder in [UPLOAD_FOLDER, OUTPUT_FOLDER]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# 从JSON模板结构定义Jinja2字段列表
JINJA_FIELDS = [
    # 基本信息
    {"id": "name", "name": "姓名", "description": "员工的完整姓名", "preview": "{{ BasicInfo.name }}"},
    {"id": "employee_id", "name": "员工编号", "description": "员工的唯一识别编号", "preview": "{{ BasicInfo.employee_id }}"},
    {"id": "department", "name": "部门", "description": "员工所属部门", "preview": "{{ AdditionInfo.department }}"},
    {"id": "position", "name": "职位", "description": "员工的职位名称", "preview": "{{ AdditionInfo.position }}"},
    {"id": "hire_date", "name": "入职日期", "description": "员工入职的日期", "preview": "{{ AdditionInfo.hire_date }}"},
    {"id": "gender", "name": "性别", "description": "员工性别", "preview": "{{ AdditionInfo.gender }}"},
    {"id": "age", "name": "年龄", "description": "员工年龄", "preview": "{{ AdditionInfo.age }}"},
    {"id": "tenure", "name": "司龄", "description": "员工在公司的工作年限", "preview": "{{ AdditionInfo.tenure }}"},
    {"id": "education", "name": "学历", "description": "员工的最高学历", "preview": "{{ BasicInfo.education }}"},
    {"id": "graduation_school", "name": "毕业院校", "description": "员工的毕业院校", "preview": "{{ BasicInfo.graduation_school }}"},
    
    # 工作信息
    {"id": "project_experience", "name": "项目经验", "description": "员工参与的项目经历", "preview": "{% for exp in WorkExperience %}{{ exp.project_name }}{% endfor %}"},
    {"id": "job_responsibilities", "name": "工作职责", "description": "员工的主要工作职责", "preview": "{{ WorkAbility.job_responsibilities }}"},
    {"id": "skills", "name": "技能标签", "description": "员工掌握的专业技能", "preview": "{% for skill in WorkAbility.skills %}{{ skill }}{% endfor %}"},
    {"id": "certifications", "name": "证书", "description": "员工获得的专业证书", "preview": "{% for cert in WorkAbility.certifications %}{{ cert }}{% endfor %}"},
    {"id": "training_experience", "name": "培训经历", "description": "员工参加的培训课程", "preview": "{% for training in WorkAbility.training_experience %}{{ training }}{% endfor %}"},
    
    # 联系方式
    {"id": "phone", "name": "电话", "description": "员工的联系电话", "preview": "{{ BasicInfo.phone }}"},
    {"id": "email", "name": "邮箱", "description": "员工的电子邮箱", "preview": "{{ BasicInfo.email }}"},
    {"id": "address", "name": "地址", "description": "员工的居住地址", "preview": "{{ BasicInfo.address }}"},
    {"id": "emergency_contact", "name": "紧急联系人", "description": "员工的紧急联系人", "preview": "{{ BasicInfo.emergency_contact }}"},
    {"id": "emergency_phone", "name": "紧急联系电话", "description": "紧急联系人的电话", "preview": "{{ BasicInfo.emergency_phone }}"},
    
    # 其他信息
    {"id": "bank_account", "name": "银行账户", "description": "员工的银行账户信息", "preview": "{{ AdditionInfo.bank_account }}"},
    {"id": "id_number", "name": "身份证号", "description": "员工的身份证号码", "preview": "{{ AdditionInfo.id_number }}"},
    {"id": "marital_status", "name": "婚姻状况", "description": "员工的婚姻状况", "preview": "{{ AdditionInfo.marital_status }}"},
    {"id": "political_status", "name": "政治面貌", "description": "员工的政治面貌", "preview": "{{ AdditionInfo.political_status }}"}
]

# 路由定义
@app.route('/')
def index():
    return render_template('template.html', BasicInfo={}, AdditionInfo={}, WorkExperience=[], WorkAbility={})

@app.route('/api/fields')
def get_jinja_fields():
    return jsonify(JINJA_FIELDS)

@app.route('/api/upload', methods=['POST'])
def upload_document():
    if 'file' not in request.files:
        return jsonify({"error": "未找到文件"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "未选择文件"}), 400
    if file and file.filename.endswith('.docx'):
        # 保存上传的文件
        filename = str(uuid.uuid4()) + '.docx'
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        
        # 解析Word文档内容
        doc = Document(filepath)
        content = []
        for para in doc.paragraphs:
            content.append(para.text)
        
        return jsonify({
            "success": True,
            "filename": filename,
            "content": content
        })
    return jsonify({"error": "仅支持.docx格式文件"}), 400

@app.route('/api/save', methods=['POST'])
def save_document():
    data = request.json
    if not data or 'filename' not in data or 'content' not in data:
        return jsonify({"error": "缺少必要参数"}), 400
    
    # 创建新的Word文档
    doc = Document()
    for para_text in data['content']:
        doc.add_paragraph(para_text)
    
    # 保存文件
    output_filename = str(uuid.uuid4()) + '.docx'
    output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    doc.save(output_path)
    
    return jsonify({
        "success": True,
        "output_filename": output_filename,
        "download_url": f"/download/{output_filename}"
    })

@app.route('/download/<filename>')
def download_document(filename):
    filepath = os.path.join(OUTPUT_FOLDER, filename)
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True)
    return jsonify({"error": "文件不存在"}), 404

# 新增：简历生成器API
import pandas as pd
import threading
from queue import Queue
import time
from datetime import datetime

# 配置常量
CONFIG_FOLDER = os.path.join(os.path.dirname(__file__), 'config')
EMPLOYEE_DATA_PATH = os.path.join(CONFIG_FOLDER, 'emp_list.json')
BANK_LIST_PATH = os.path.join(CONFIG_FOLDER, 'bank_list.config')

# 创建必要的文件夹
if not os.path.exists(CONFIG_FOLDER):
    os.makedirs(CONFIG_FOLDER)

# 全局状态和队列
parse_queue = Queue()
generate_queue = Queue()
parse_progress = {}
generate_progress = {}

# 辅助函数：读取银行列表
def get_bank_list():
    if not os.path.exists(BANK_LIST_PATH):
        # 创建默认银行列表
        default_banks = ['长亮科技', '测试银行', '招商银行', '建设银行', '工商银行', '农业银行']
        with open(BANK_LIST_PATH, 'w', encoding='utf-8') as f:
            for bank in default_banks:
                f.write(f'{bank}\n')
        return default_banks
    
    with open(BANK_LIST_PATH, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

# 辅助函数：保存员工数据
def save_employee_data(employees):
    with open(EMPLOYEE_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(employees, f, ensure_ascii=False, indent=2)

# 辅助函数：读取员工数据
def get_employee_data():
    if not os.path.exists(EMPLOYEE_DATA_PATH):
        return []
    
    with open(EMPLOYEE_DATA_PATH, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except:
            return []

# 解析工作线程
def parse_worker():
    while True:
        task = parse_queue.get()
        task_id = task['task_id']
        folder_path = task['folder_path']
        parse_progress[task_id] = {'progress': 0, 'message': '开始解析简历...'}

        try:
            # 获取文件夹中的所有文件
            files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.doc', '.docx'))]
            total_files = len(files)

            if total_files == 0:
                parse_progress[task_id] = {
                    'progress': 100,
                    'message': '文件夹中没有找到简历文件'
                }
                continue

            # 模拟解析过程
            for i, file in enumerate(files):
                progress = int((i + 1) / total_files * 100)
                parse_progress[task_id] = {
                    'progress': progress,
                    'message': f'正在解析: {file}'
                }
                time.sleep(0.1)  # 模拟处理时间

            parse_progress[task_id] = {
                'progress': 100,
                'message': f'解析完成，共处理 {total_files} 个文件'
            }
        except Exception as e:
            parse_progress[task_id] = {
                'progress': 0,
                'message': f'解析失败: {str(e)}'
            }
        finally:
            parse_queue.task_done()
            # 5分钟后清除进度记录
            threading.Timer(300, lambda: parse_progress.pop(task_id, None)).start()

# 生成工作线程
def generate_worker():
    while True:
        task = generate_queue.get()
        task_id = task['task_id']
        employee_ids = task['employee_ids']
        bank_name = task['bank_name']
        generate_progress[task_id] = {'progress': 0, 'message': '开始生成简历...'}

        try:
            total_employees = len(employee_ids)

            if total_employees == 0:
                generate_progress[task_id] = {
                    'progress': 100,
                    'message': '没有选择员工'
                }
                continue

            # 模拟生成过程
            for i, emp_id in enumerate(employee_ids):
                progress = int((i + 1) / total_employees * 100)
                generate_progress[task_id] = {
                    'progress': progress,
                    'message': f'正在生成: {emp_id} 的简历'
                }
                time.sleep(0.2)  # 模拟处理时间

            generate_progress[task_id] = {
                'progress': 100,
                'message': f'简历生成完成，共生成 {total_employees} 份简历'
            }
        except Exception as e:
            generate_progress[task_id] = {
                'progress': 0,
                'message': f'生成失败: {str(e)}'
            }
        finally:
            generate_queue.task_done()
            # 5分钟后清除进度记录
            threading.Timer(300, lambda: generate_progress.pop(task_id, None)).start()

# 启动工作线程
threading.Thread(target=parse_worker, daemon=True).start()
threading.Thread(target=generate_worker, daemon=True).start()

@app.route('/resume-builder')
def resume_builder():
    return render_template('resume_builder.html')

@app.route('/api/banks')
def get_banks():
    try:
        banks = get_bank_list()
        return jsonify(banks)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'获取银行列表失败: {str(e)}'
        }), 500

@app.route('/api/employees')
def get_employees():
    try:
        employees = get_employee_data()
        return jsonify(employees)
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'获取员工列表失败: {str(e)}'
        }), 500

@app.route('/api/parse-tech-info', methods=['POST'])
def parse_tech_info():
    try:
        file_path = request.json.get('file_path')
        if not file_path or not os.path.exists(file_path):
            return jsonify({
                'status': 'error',
                'message': '文件不存在'
            }), 400

        # 尝试读取Excel文件
        try:
            df = pd.read_excel(file_path)
        except:
            # 尝试读取CSV文件
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except:
                df = pd.read_csv(file_path, encoding='gbk')

        # 转换为JSON并保存
        employees = df.to_dict('records')
        save_employee_data(employees)

        return jsonify({
            'status': 'success',
            'message': f'成功解析 {len(employees)} 条员工信息',
            'count': len(employees)
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'解析技术人员信息失败: {str(e)}'
        }), 500

@app.route('/api/parse-resumes', methods=['POST'])
def parse_resumes():
    try:
        folder_path = request.json.get('folder_path')
        if not folder_path or not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            return jsonify({
                'status': 'error',
                'message': '文件夹不存在'
            }), 400

        # 创建新任务
        task_id = datetime.now().strftime('%Y%m%d%H%M%S')
        parse_queue.put({
            'task_id': task_id,
            'folder_path': folder_path
        })

        return jsonify({
            'status': 'success',
            'task_id': task_id,
            'message': '已加入解析队列'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'开始解析失败: {str(e)}'
        }), 500

@app.route('/api/parse-progress')
def get_parse_progress():
    def event_stream():
        task_id = request.args.get('task_id')
        if not task_id:
            yield 'data: {"error": "缺少任务ID"}\n\n'
            return

        while task_id in parse_progress:
            data = parse_progress[task_id]
            yield f'data: {json.dumps(data)}\n\n'
            if data['progress'] == 100:
                break
            time.sleep(1)

    return Response(event_stream(), mimetype='text/event-stream')

@app.route('/api/generate-resumes', methods=['POST'])
def generate_resumes():
    try:
        employee_ids = request.json.get('employee_ids', [])
        bank_name = request.json.get('bank_name', '')

        if not employee_ids or not bank_name:
            return jsonify({
                'status': 'error',
                'message': '员工ID列表和银行名称不能为空'
            }), 400

        # 创建新任务
        task_id = datetime.now().strftime('%Y%m%d%H%M%S')
        generate_queue.put({
            'task_id': task_id,
            'employee_ids': employee_ids,
            'bank_name': bank_name
        })

        return jsonify({
            'status': 'success',
            'task_id': task_id,
            'message': '已加入生成队列'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'开始生成失败: {str(e)}'
        }), 500

@app.route('/api/generate-progress')
def get_generate_progress():
    def event_stream():
        task_id = request.args.get('task_id')
        if not task_id:
            yield 'data: {"error": "缺少任务ID"}\n\n'
            return

        while task_id in generate_progress:
            data = generate_progress[task_id]
            yield f'data: {json.dumps(data)}\n\n'
            if data['progress'] == 100:
                break
            time.sleep(1)

    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, port=5000)