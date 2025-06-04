from flask import Flask, request, jsonify
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 配置信息
CONFIG = {
    "veid": "1632978",
    "api_key": "private_HgVQsmwXZrpIOTTUFvKaCtsN",
    "get_server_info_url": "https://api.64clouds.com/v1/getServiceInfo"
}

@app.route('/api/server-status', methods=['GET'])
def get_server_status():
    try:
        # 调用64Clouds API
        response = requests.get(
            CONFIG['get_server_info_url'],
            params={
                'veid': CONFIG['veid'],
                'api_key': CONFIG['api_key']
            },
            timeout=10
        )
        
        # 检查响应状态
        response.raise_for_status()
        
        # 返回JSON数据
        return jsonify(response.json())
        
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": True,
            "message": f"API请求失败: {str(e)}"
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)