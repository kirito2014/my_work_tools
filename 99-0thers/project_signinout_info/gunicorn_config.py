# Gunicorn配置文件

# 绑定地址和端口
bind = "127.0.0.1:8000"

# 工作进程数，推荐设置为 (2 x CPU核心数 + 1)
workers = 4

# 工作模式，使用gevent异步模式
worker_class = "gevent"

# 最大并发客户端数
worker_connections = 1000

# 进程名称
proc_name = "attendance_system"

# 访问日志文件路径
accesslog = "/var/log/attendance/access.log"

# 错误日志文件路径
errorlog = "/var/log/attendance/error.log"

# 日志级别
loglevel = "info"

# 超时时间（秒）
timeout = 30

# 保持连接时间（秒）
keepalive = 2

# 最大请求数，防止内存泄漏
max_requests = 1000
max_requests_jitter = 50

# 环境变量
raw_env = [
    "FLASK_ENV=production",
    "PYTHONPATH=/var/www/attendance_system"
]