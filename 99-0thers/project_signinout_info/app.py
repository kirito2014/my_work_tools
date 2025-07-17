from flask import Flask, jsonify, render_template
import random
from datetime import datetime, timedelta
import pymysql
from config import DB_CONFIG

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 支持中文显示

# 模拟员工数据
EMPLOYEES = 22  # 总员工数
DEPARTMENTS = ["对公组", "零售组", "信贷组", "资管组", "通用组", "自助分析"]

def get_db_connection():
    """建立数据库连接"""
    connection = pymysql.connect(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        port=DB_CONFIG['port'],
        charset=DB_CONFIG['charset'],
        ssl={'ca': '/path/to/ca.pem'},
        allow_public_key=True,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def query_db(query, params=None):
    """执行数据库查询并返回结果"""
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            result = cursor.fetchall()
        connection.commit()
        return result
    except Exception as e:
        print(f"Database query error: {str(e)}")
        return None
    finally:
        if connection:
            connection.close()

def generate_checkin_trend(time_range):
    """生成签到趋势数据"""
    data = []
    now = datetime.now()
    
    # 从数据库获取签到趋势数据
    if time_range == "today":
        # 查询今日每小时签到数据
        query = """
            SELECT HOUR(checkin_time) as hour, COUNT(*) as checkin
            FROM attendance_records
            WHERE DATE(checkin_time) = CURDATE()
            GROUP BY HOUR(checkin_time)
            ORDER BY hour
        """
        results = query_db(query)
        print(results)
        
        if results:
            for row in results:
                data.append({
                    "time": f"{row['hour']}:00",
                    "checkin": row['checkin']
                })
        else:
        # 模拟数据 - 当数据库查询失败时使用
            for hour in range(8, 19):  # 8:00-18:00
                checkin = random.randint(5, 20)
                data.append({
                    "time": f"{hour}:00",
                    "checkin": checkin
                })
    elif time_range == "week":
        # 本周每天数据
        for day in range(7):
            date = (now - timedelta(days=6 - day)).strftime("%m-%d")
            checkin = random.randint(100, 140)
            data.append({
                "date": date,
                "checkin": checkin
            })
    elif time_range == "month":
        # 本月每周数据
        for week in range(4):
            data.append({
                "week": f"第{week+1}周",
                "checkin": random.randint(450, 550)
            })
    elif time_range == "quarter":
        # 本季度每月数据
        for month in range(3):
            data.append({
                "month": f"{now.month-2+month}月",
                "checkin": random.randint(1200, 1500)
            })
    
    return data

def generate_late_distribution(time_range):
    """生成迟到分布散点图数据"""
    data = []
    # 从数据库获取迟到分布数据
    if time_range == "today":
        # 查询今日迟到记录
        query = """
            SELECT TIME_FORMAT(checkin_time, '%H:%i') as time,
                   TIMESTAMPDIFF(MINUTE, '08:30:00', checkin_time) as delay
            FROM attendance_records
            WHERE DATE(checkin_time) = CURDATE()
              AND checkin_time > '08:30:00'
        """
        results = query_db(query)
        
        if results:
            for row in results:
                data.append({
                    "time": row['time'],
                    "delay": row['delay']
                })
        else:
        # 模拟数据 - 当数据库查询失败时使用
            for _ in range(30):  # 30个迟到样本
                hour = random.randint(8, 9)
                minute = random.randint(1, 59)
                delay = random.randint(1, 60)
                data.append({
                    "time": f"{hour}:{minute:02d}",
                    "delay": delay
                })
    else:
        # 其他时间范围按天分布
        days = 7 if time_range == "week" else 30 if time_range == "month" else 90
        for day in range(days):
            data.append({
                "day": day + 1,
                "late_count": random.randint(5, 20)
            })
    return data

def generate_batch_distribution():
    """生成签到批次分布数据
    8:15批次和8:50批次人数
    """
    # 从数据库获取签到批次分布数据
    query = """
        SELECT batch_name as name, COUNT(*) as value
        FROM attendance_records
        WHERE DATE(checkin_time) = CURDATE()
          AND batch_name IN ('8:15批次', '8:50批次')
        GROUP BY batch_name
    """
    results = query_db(query)
    
    if results:
        # 确保两个批次都有数据，缺失的批次补0
        batch_map = {item['name']: item['value'] for item in results}
        return [
            {"name": "8:15批次", "value": batch_map.get('8:15批次', 0)},
            {"name": "8:50批次", "value": batch_map.get('8:50批次', 0)}
        ]
    else:
        # 模拟数据 - 当数据库查询失败时使用
        batch1 = 14  # 8:15批次人数
        batch2 = 8  # 8:50批次人数
        return [
            {"name": "8:15批次", "value": batch1},
            {"name": "8:50批次", "value": batch2}
        ]


def generate_status_distribution():
    """生成签到状态占比数据
    签到人数和未签到人数
    """
    today_checkin = random.randint(5, 20)
    not_checkin = EMPLOYEES - today_checkin
    return [
        {"name": "已签到", "value": today_checkin},
        {"name": "未签到", "value": not_checkin}
    ]


def generate_overtime_data(time_range):
    """生成部门加班时长数据"""
    data = []
    # 从数据库获取部门加班数据
    time_clause = ""
    if time_range == "today":
        time_clause = "DATE(overtime_date) = CURDATE()"
    elif time_range == "week":
        time_clause = "YEARWEEK(overtime_date, 1) = YEARWEEK(NOW(), 1)"
    elif time_range == "month":
        time_clause = "DATE_FORMAT(overtime_date, '%Y-%m') = DATE_FORMAT(NOW(), '%Y-%m')"
    else:  # quarter
        time_clause = "QUARTER(overtime_date) = QUARTER(NOW()) AND YEAR(overtime_date) = YEAR(NOW())"
    
    query = f"""
        SELECT department, SUM(overtime_hours) as overtime
        FROM department_overtime
        WHERE {time_clause}
        GROUP BY department
    """
    results = query_db(query)
    print(results)
    
    if results:
        # 将查询结果转换为所需格式
        result_map = {item['department']: item['overtime'] for item in results}
        for dept in DEPARTMENTS:
            data.append({
                "department": dept,
                "overtime": result_map.get(dept, 0)
            })
    else:
        # 模拟数据 - 当数据库查询失败时使用
        for dept in DEPARTMENTS:
            if time_range == "today":
                hours = random.randint(1, 4)  # 今日加班小时
            elif time_range == "week":
                hours = random.randint(8, 20)  # 本周加班小时
            elif time_range == "month":
                hours = random.randint(30, 60)  # 本月加班小时
            else:
                hours = random.randint(100, 180)  # 本季度加班小时
            data.append({
                "department": dept,
                "overtime": hours
            })
    return data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/overview/<time_range>')
def get_overview(time_range):
    """从数据库获取概览统计数据"""
    # 获取当前和上一周期的时间范围
    def get_date_ranges(tr):
        now = datetime.now()
        if tr == 'today':
            current_start = now.strftime('%Y-%m-%d')
            current_end = current_start
            prev_start = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_end = prev_start
            prev_name = 'yesterday'
        elif tr == 'week':
            # 本周一到今天
            current_start = (now - timedelta(days=now.weekday())).strftime('%Y-%m-%d')
            current_end = now.strftime('%Y-%m-%d')
            # 上周一到上周日
            prev_start = (now - timedelta(days=now.weekday() + 7)).strftime('%Y-%m-%d')
            prev_end = (now - timedelta(days=now.weekday() + 1)).strftime('%Y-%m-%d')
            prev_name = 'last_week'
        elif tr == 'month':
            current_start = now.replace(day=1).strftime('%Y-%m-%d')
            current_end = now.strftime('%Y-%m-%d')
            # 上个月
            last_month = now.month - 1 if now.month > 1 else 12
            last_year = now.year if now.month > 1 else now.year - 1
            prev_start = datetime(last_year, last_month, 1).strftime('%Y-%m-%d')
            prev_end = (datetime(now.year, now.month, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_name = 'last_month'
        else:  # quarter
            # 当前季度
            quarter = (now.month - 1) // 3 + 1
            current_start = datetime(now.year, 3*(quarter-1)+1, 1).strftime('%Y-%m-%d')
            current_end = now.strftime('%Y-%m-%d')
            # 上一季度
            prev_quarter = quarter - 1 if quarter > 1 else 4
            prev_year = now.year if quarter > 1 else now.year - 1
            prev_start = datetime(prev_year, 3*(prev_quarter-1)+1, 1).strftime('%Y-%m-%d')
            prev_end = (datetime(now.year, 3*(quarter-1)+1, 1) - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_name = 'last_quarter'
        return current_start, current_end, prev_start, prev_end, prev_name
    
    # 获取时间范围
    current_start, current_end, prev_start, prev_end, prev_period = get_date_ranges(time_range)
    
    # 查询当前周期数据
    current_query = """
        SELECT 
            COUNT(*) as checkin,
            SUM(CASE WHEN checkin_time > '08:30:00' THEN 1 ELSE 0 END) as late
        FROM attendance_records
        WHERE DATE(checkin_time) BETWEEN %s AND %s
    """
    current_data = query_db(current_query, (current_start, current_end))[0] if query_db(current_query, (current_start, current_end)) else None
    
    # 查询上一周期数据
    prev_query = """
        SELECT 
            COUNT(*) as checkin,
            SUM(CASE WHEN checkin_time > '08:30:00' THEN 1 ELSE 0 END) as late
        FROM attendance_records
        WHERE DATE(checkin_time) BETWEEN %s AND %s
    """
    prev_data = query_db(prev_query, (prev_start, prev_end))[0] if query_db(prev_query, (prev_start, prev_end)) else None
    
    # 查询总员工数
    total_query = "SELECT COUNT(*) as total FROM employees"
    total_data = query_db(total_query)[0] if query_db(total_query) else None
    total_employees = total_data['total'] if total_data else 22
    
    # 如果数据库查询失败，使用模拟数据
    if not current_data or not prev_data:
        if time_range == 'today':
            current_checkin = random.randint(16, 22)
            current_late = random.randint(5, 15)
            prev_checkin = random.randint(13, 22)
            prev_late = random.randint(3, 12)
        elif time_range == 'week':
            current_checkin = random.randint(120, 220)
            current_late = random.randint(20, 40)
            prev_checkin = random.randint(120, 220)
            prev_late = random.randint(18, 38)
        elif time_range == 'month':
            current_checkin = random.randint(1400, 1600)
            current_late = random.randint(80, 120)
            prev_checkin = random.randint(1300, 1500)
            prev_late = random.randint(70, 110)
        else:  # quarter
            current_checkin = random.randint(4200, 4800)
            current_late = random.randint(240, 360)
            prev_checkin = random.randint(3900, 4500)
            prev_late = random.randint(220, 340)
    else:
        current_checkin = current_data['checkin']
        current_late = current_data['late']
        prev_checkin = prev_data['checkin']
        prev_late = prev_data['late']
    
    current_rate = round(current_late / current_checkin * 
100, 1) if current_checkin > 0 else 0
    prev_rate = round(prev_late / prev_checkin * 100, 1) if prev_checkin > 0 else 0
    
    # 计算趋势和百分比变化
    def calculate_trend(current, previous, is_percent=False):
        if previous == 0:
            return ('up', '100.0%') if current > 0 else ('flat', '0.0%')
        diff = current - previous
        percent = (diff / previous) * 100
        if percent > 0:
            return ('up', f'{percent:.1f}%')
        elif percent < 0:
            return ('down', f'{abs(percent):.1f}%')
        else:
            return ('flat', '0.0%')
    
    checkin_trend, checkin_pct = calculate_trend(current_checkin, prev_checkin)
    late_trend, late_pct = calculate_trend(current_late, prev_late)
    rate_trend, rate_pct = calculate_trend(current_rate, prev_rate, is_percent=True)
    total_trend, total_pct = ('flat', '0.0%')  # 总人数固定不变
    
    data = {
        'today_checkin': current_checkin,
        'today_late': current_late,
        'total_employees': total_employees,
        'late_rate': f'{current_rate}%',
        'checkin_compare': {'trend': checkin_trend, 'value': checkin_pct},
        'late_compare': {'trend': late_trend, 'value': late_pct},
        'total_compare': {'trend': total_trend, 'value': total_pct},
        'rate_compare': {'trend': rate_trend, 'value': rate_pct},
        'prev_period': prev_period
    }
    return jsonify(data)

@app.route('/api/trend/<time_range>')
def get_trend(time_range):
    """获取签到趋势数据"""
    return jsonify(generate_checkin_trend(time_range))

@app.route('/api/late_distribution/<time_range>')
def get_late_distribution(time_range):
    """获取迟到分布数据"""
    return jsonify(generate_late_distribution(time_range))

@app.route('/api/overtime/<time_range>')
def get_overtime(time_range):
    """获取加班数据"""
    return jsonify(generate_overtime_data(time_range))


@app.route('/api/batch_distribution')
def get_batch_distribution():
    """获取签到批次分布数据"""
    return jsonify(generate_batch_distribution())


@app.route('/api/status_distribution')
def get_status_distribution():
    """获取签到状态占比数据"""
    return jsonify(generate_status_distribution())

if __name__ == '__main__':
    app.run(debug=True)