from flask import Flask, render_template, request, jsonify, send_file
import yaml
import os
import tempfile

app = Flask(__name__)

# 配置文件路径
CONFIG_PATH = "config.yaml"

def load_config():
    """加载配置文件"""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}
    except yaml.YAMLError as e:
        return {"error": f"配置文件格式错误: {e}"}

def save_config(config):
    """保存配置文件"""
    try:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        return True
    except Exception as e:
        return False

@app.route('/')
def index():
    """主页面"""
    config = load_config()
    return render_template('index.html', config=config)

@app.route('/api/config', methods=['GET'])
def get_config():
    """获取配置"""
    config = load_config()
    return jsonify(config)

@app.route('/api/config', methods=['POST'])
def update_config():
    """更新配置"""
    try:
        new_config = request.json
        if save_config(new_config):
            return jsonify({"success": True, "message": "配置已保存"})
        else:
            return jsonify({"success": False, "message": "保存失败"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@app.route('/api/config/upload', methods=['POST'])
def upload_config():
    """上传配置文件"""
    try:
        if 'file' not in request.files:
            return jsonify({"success": False, "message": "未找到文件"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"success": False, "message": "未选择文件"}), 400
        
        if file and file.filename.endswith('.yaml'):
            # 读取上传的文件内容
            content = file.read().decode('utf-8')
            config = yaml.safe_load(content)
            
            # 保存到本地
            if save_config(config):
                return jsonify({"success": True, "message": "配置文件已上传"})
            else:
                return jsonify({"success": False, "message": "保存失败"}), 500
        else:
            return jsonify({"success": False, "message": "只支持 YAML 文件"}), 400
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/config/download', methods=['GET'])
def download_config():
    """下载配置文件"""
    if os.path.exists(CONFIG_PATH):
        return send_file(CONFIG_PATH, as_attachment=True, download_name='config.yaml')
    else:
        return jsonify({"success": False, "message": "配置文件不存在"}), 404

if __name__ == '__main__':
    app.run(debug=True)