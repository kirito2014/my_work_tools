from flask import Flask, render_template, jsonify
import os

app = Flask(__name__)

# 静态文件夹：Flask 默认会把静态文件放在/static文件夹中
app.config['STATIC_FOLDER'] = 'static'

# 路由：展示前端页面
@app.route('/')
def index():
    return render_template('index.html')

# 路由：获取文件列表（图表）
@app.route('/api/get_file_list', methods=['GET'])
def get_file_list():
    folder_path = os.path.join(app.config['STATIC_FOLDER'], 'charts')  # 图表存储的文件夹
    files = []

    # 获取所有以 .html 结尾的文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.html'):
            # 提取姓名和日期
            name, date = filename.split('_成绩单_趋势_')
            date = date.replace('.html', '')
            files.append({
                'name': name,
                'date': date,
                'file': os.path.join('static', 'charts', filename)  # 前端访问静态文件时需要加上 /static
            })

    return jsonify(files)

if __name__ == "__main__":
    app.run(debug=True)
