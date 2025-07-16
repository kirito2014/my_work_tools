from flask import Flask, jsonify, render_template
import random
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 支持中文显示

# 模拟员工数据
EMPLOYEES = 22  # 总员工数
DEPARTMENTS = ["对公组", "零售组", "信贷组", "资管组", "通用组", "自助分析"]

def generate_checkin_trend(time_range):
    """生成签到趋势数据"""
    data = []
    now = datetime.now()
    
    if time_range == "today":
        # 今日每小时数据
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
    if time_range == "today":
        # 今日迟到时间分布（分钟）
        for _ in range(30):  # 30个迟到样本
            hour = random.randint(8, 9)  # 8-9点迟到
            minute = random.randint(1, 59)
            delay = random.randint(1, 60)  # 迟到1-60分钟
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
    # 生成当前周期数据
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
    
    total_employees = 22
    current_rate = round(current_late / current_checkin * 100, 1) if current_checkin > 0 else 0
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
        'rate_compare': {'trend': rate_trend, 'value': rate_pct}
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