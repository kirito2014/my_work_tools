import http.server
import socketserver

PORT = 8000  # 设置端口号
DIRECTORY = "D://"  

class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

with socketserver.TCPServer(("", PORT), CustomHTTPRequestHandler) as httpd:
    print(f"Serving on port {PORT}. Access via http://<your-ip>:{PORT}")
    httpd.serve_forever()
