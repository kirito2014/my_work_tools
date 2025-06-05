from flask import Flask, request, jsonify
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 配置信息
CONFIG = {
    "veid": "1632978",
    "api_key": "private_HgVQsmwXZrpIOTTUFvKaCtsN",
    "api_base": "https://api.64clouds.com/v1"
}

def call_64clouds_api(endpoint):
    try:
        response = requests.get(
            f"{CONFIG['api_base']}/{endpoint}",
            params={
                'veid': CONFIG['veid'],
                'api_key': CONFIG['api_key']
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": True, "message": str(e)}

@app.route('/api/server-status', methods=['GET'])
def get_server_status():
    return jsonify(call_64clouds_api('getServiceInfo'))

@app.route('/api/server-start', methods=['POST'])
def start_server():
    return jsonify(call_64clouds_api('start'))

@app.route('/api/server-stop', methods=['POST'])
def stop_server():
    return jsonify(call_64clouds_api('stop'))

@app.route('/api/server-restart', methods=['POST'])
def restart_server():
    return jsonify(call_64clouds_api('restart'))

@app.route('/api/server-kill', methods=['POST'])
def kill_server():
    return jsonify(call_64clouds_api('kill'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5100, debug=True)