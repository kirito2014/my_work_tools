# 考勤系统Linux服务器部署说明书

## 一、环境要求
- 操作系统：Ubuntu 20.04/22.04 LTS 或 CentOS 7/8
- 最低配置：2核CPU，2GB内存，20GB磁盘空间
- 网络要求：开放8080端口（HTTP），3306端口（MySQL，可选）

## 二、部署架构说明
本项目采用Nginx+Gunicorn+Flask架构：
- **Nginx**：作为前端反向代理服务器，处理静态资源并转发动态请求
- **Gunicorn**：作为WSGI应用服务器，运行Python Flask应用
- **Flask**：Python Web应用框架
- **MySQL**：数据库服务器（可独立部署）

## 三、安装步骤

### 3.1 服务器环境准备
```bash
# Ubuntu更新系统
sudo apt update && sudo apt upgrade -y

# CentOS更新系统
# sudo yum update -y && sudo yum upgrade -y

# 安装基础依赖
sudo apt install -y python3 python3-pip python3-venv git nginx mysql-server
# CentOS使用:
# sudo yum install -y python3 python3-pip python3-venv git nginx mariadb-server
```

### 3.2 数据库配置
```bash
# 启动MySQL服务
systemctl start mysql
# 设置开机自启
systemctl enable mysql

# 安全配置（设置root密码等）
mysql_secure_installation

# 登录MySQL并创建数据库
mysql -u root -p
CREATE DATABASE attendance;
CREATE USER 'attendance_user'@'localhost' IDENTIFIED BY 'your_secure_password';
GRANT ALL PRIVILEGES ON attendance.* TO 'attendance_user'@'localhost';
FLUSH PRIVILEGES;
EXIT;

# 导入数据库结构
mysql -u root -p attendance < database_schema.sql
```

### 3.3 项目部署
```bash
# 创建项目目录
sudo mkdir -p /var/www/attendance_system
cd /var/www/attendance_system

sudo chown $USER:$USER /var/www/attendance_system

# 克隆/上传项目文件
git clone <your_repository_url> .
# 或使用scp上传本地文件
# scp -r * username@server_ip:/var/www/attendance_system/

# 创建并激活虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
pip install gunicorn gevent

deactivate

# 设置目录权限
chown -R www-data:www-data /var/www/attendance_system
chmod -R 750 /var/www/attendance_system

# 安装依赖
pip install -r requirements.txt
pip install gunicorn  # 安装Gunicorn

# 修改配置文件
cp config.py config.py.bak
# 编辑配置文件，更新数据库连接信息
sed -i 's/localhost/127.0.0.1/g' config.py
sed -i 's/root/attendance_user/g' config.py
sed -i 's/password/your_secure_password/g' config.py
sed -i 's/attendance/attendance/g' config.py
```

### 3.4 Gunicorn配置
# 创建应用用户和组
# 注意：系统已存在www-data用户和组
# groupadd -r www-data
# useradd -r -g www-data -d /var/www/attendance_system -s /sbin/nologin www-data

# 创建日志目录
mkdir -p /var/log/attendance
chown www-data:www-data /var/log/attendance
chmod 750 /var/log/attendance

创建Gunicorn服务文件：
```bash
sudo nano /etc/systemd/system/attendance.service
```

添加以下内容：
```ini
[Unit]
Description=Attendance System Gunicorn Service
After=network.target mysql.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/attendance_system
Environment="PATH=/var/www/attendance_system/venv/bin"
ExecStart=/var/www/attendance_system/venv/bin/gunicorn -c /var/www/attendance_system/gunicorn_config.py app:app
Restart=always

[Install]
WantedBy=multi-user.target
```

启动Gunicorn服务：
```bash
# 准备日志目录
  mkdir -p /var/log/attendance && chmod 755 /var/log/attendance
  
  # 复制服务配置文件
  cp attendance.service /etc/systemd/system/
  systemctl daemon-reload
  
  # 启动服务并设置开机自启
  systemctl start attendance
  systemctl enable attendance
  
  # 配置Nginx
  # 复制站点配置文件
  cp nginx_attendance.conf /etc/nginx/conf.d/
  
  # 禁用默认站点（如有）
  rm -f /etc/nginx/conf.d/default.conf
  
  # 测试Nginx配置
  nginx -t
  
  # 启动/重启Nginx服务
  systemctl restart nginx
  
  # 设置Nginx开机自启
  systemctl enable nginx

# 检查服务状态
ps aux | grep gunicorn
ps aux | grep nginx
```

### 3.5 Nginx配置
创建Nginx站点配置：
```bash
# 使用tee命令创建配置文件以避免编辑器权限问题
mkdir -p /etc/nginx/sites-available
tee /etc/nginx/sites-available/attendance > /dev/null <<'EOF'
server {
    listen 8080;  # Changed from 80 to avoid port conflicts
    server_name your_domain.com;  # 替换为您的域名或服务器IP

    location /static {
        alias /var/www/attendance_system/static;
        expires 30d;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

# 验证配置文件权限
sudo chmod 644 /etc/nginx/sites-available/attendance
```

添加以下内容：
```nginx
server {
    listen 8080;  # Changed from 80 to avoid port conflicts
    server_name your_domain.com;  # 替换为您的域名或服务器IP

    location /static {
        alias /var/www/attendance_system/static;
        expires 30d;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

启用站点并重启Nginx：
```bash
mkdir -p /etc/nginx/sites-enabled
ln -s /etc/nginx/sites-available/attendance /etc/nginx/sites-enabled/
nginx -t  # 测试配置是否有误
nginx -s stop || true && nginx  # Stop existing Nginx if running, then start fresh
# nginx -s reload is unnecessary after fresh start
# systemctl is not available; use above commands for Nginx management
# If you see 'Address already in use' errors, check for other processes using port 8080
# and stop them manually or change the listen port in Nginx configuration
```

### 3.6 防火墙配置
```bash
# 开放8080端口
# Ubuntu/Debian
ufw allow 8080/tcp  # Updated to match new Nginx port
ufw allow 443/tcp  # 如果后续配置HTTPS

ufw enable
# CentOS
# firewall-cmd --permanent --add-service=http
# firewall-cmd --permanent --add-service=https
# firewall-cmd --reload
```

## 四、部署验证
1. 访问服务器IP或域名：`http://your_server_ip`
2. 检查是否能正常加载页面和数据
3. 查看服务状态确保运行正常：
```bash
systemctl status nginx
systemctl status attendance
systemctl status mysql
```

## 五、维护命令
### 5.1 应用维护
```bash
# 查看应用日志
journalctl -u attendance -f

# 重启应用
systemctl restart attendance

# 更新代码
git pull
# 或重新上传文件后重启服务
systemctl restart attendance
```

### 5.2 Nginx维护
```bash
# 查看Nginx日志
tail -f /var/log/nginx/access.log
tail -f /var/log/nginx/error.log

# 重启Nginx
systemctl restart nginx
```

### 5.3 数据库维护
```bash
# 备份数据库
mysqldump -u root -p attendance > backup_$(date +%Y%m%d).sql
```
# 查看数据库状态
systemctl status mysql
```

## 六、常见问题解决
1. **页面无法访问**：检查Nginx和attendance服务是否运行，防火墙是否开放端口
2. **数据库连接错误**：检查config.py配置和MySQL服务状态
3. **静态文件加载失败**：检查Nginx静态文件路径配置
4. **502 Bad Gateway**：检查Gunicorn服务是否运行正常

## 七、安全建议
1. 配置HTTPS（使用Let's Encrypt）
2. 定期备份数据库
3. 限制SSH访问
4. 定期更新系统和依赖包
5. 不要在代码中硬编码敏感信息