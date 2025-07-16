from flask import Flask, render_template, jsonify
import random
import time
from datetime import datetime, timedelta

app = Flask(__name__)


# 模拟数据
class DataGenerator:
    def __init__(self):
        self.reset_data()

    def reset_data(self):
        # 时间标签 (24小时)
        self.time_labels = [(datetime.now() - timedelta(hours=i)).strftime('%H:%M') for i in range(23, -1, -1)]

        # 销售数据
        self.sales_trend = [random.randint(1000, 5000) for _ in range(24)]
        self.region_sales = [
            {"name": "华东", "value": random.randint(10000, 30000)},
            {"name": "华北", "value": random.randint(8000, 20000)},
            {"name": "华南", "value": random.randint(12000, 25000)},
            {"name": "西北", "value": random.randint(5000, 15000)},
            {"name": "西南", "value": random.randint(6000, 18000)},
            {"name": "东北", "value": random.randint(7000, 16000)},
        ]

        # 用户数据
        self.user_active = [random.randint(500, 2000) for _ in range(24)]
        self.user_device = [
            {"name": "移动端", "value": random.randint(60, 85)},
            {"name": "PC端", "value": random.randint(10, 30)},
            {"name": "平板", "value": random.randint(5, 10)}
        ]

        # 商品分类销售
        self.category_sales = [
            {"name": "电子产品", "value": random.randint(20000, 50000)},
            {"name": "服装鞋帽", "value": random.randint(15000, 40000)},
            {"name": "家居用品", "value": random.randint(10000, 30000)},
            {"name": "食品饮料", "value": random.randint(8000, 25000)},
            {"name": "美妆个护", "value": random.randint(12000, 35000)},
        ]

        # 实时交易
        self.realtime_transactions = []
        for i in range(10):
            self.realtime_transactions.append({
                "id": f"TX{i+1:04d}",
                "time": (datetime.now() - timedelta(minutes=random.randint(0, 60))).strftime('%H:%M:%S'),
                "product": random.choice(["iPhone", "卫衣", "沙发", "牛奶", "口红"]),
                "amount": random.randint(100, 2000),
                "user": f"用户{random.randint(1000, 9999)}"
            })

    def update_data(self):
        # 更新销售趋势数据
        self.sales_trend.pop(0)
        self.sales_trend.append(random.randint(1000, 5000))

        # 更新用户活跃数据
        self.user_active.pop(0)
        self.user_active.append(random.randint(500, 2000))

        # 更新区域销售数据
        for item in self.region_sales:
            item["value"] = max(5000, item["value"] + random.randint(-2000, 2000))

        # 更新设备分布
        total = sum(item["value"] for item in self.user_device)
        mobile = random.randint(60, 85)
        pc = random.randint(10, 30)
        pad = 100 - mobile - pc
        self.user_device = [
            {"name": "移动端", "value": mobile},
            {"name": "PC端", "value": pc},
            {"name": "平板", "value": pad}
        ]

        # 更新分类销售
        for item in self.category_sales:
            item["value"] = max(8000, item["value"] + random.randint(-5000, 5000))

        # 添加新交易
        self.realtime_transactions.pop(0)
        self.realtime_transactions.append({
            "id": f"TX{random.randint(1000, 9999):04d}",
            "time": datetime.now().strftime('%H:%M:%S'),
            "product": random.choice(["iPhone", "卫衣", "沙发", "牛奶", "口红"]),
            "amount": random.randint(100, 2000),
            "user": f"用户{random.randint(1000, 9999)}"
        })


# 初始化数据生成器
data_gen = DataGenerator()


# API路由
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/sales_trend')
def get_sales_trend():
    return jsonify({
        "time": data_gen.time_labels,
        "value": data_gen.sales_trend
    })


@app.route('/api/region_sales')
def get_region_sales():
    return jsonify(data_gen.region_sales)


@app.route('/api/user_active')
def get_user_active():
    return jsonify({
        "time": data_gen.time_labels,
        "value": data_gen.user_active
    })


@app.route('/api/user_device')
def get_user_device():
    return jsonify(data_gen.user_device)


@app.route('/api/category_sales')
def get_category_sales():
    return jsonify(data_gen.category_sales)


@app.route('/api/realtime_transactions')
def get_realtime_transactions():
    return jsonify(data_gen.realtime_transactions)


# 定时更新数据
def update_data_loop():
    while True:
        data_gen.update_data()
        time.sleep(1)  # 每秒更新一次


# 启动数据更新线程
import threading
data_thread = threading.Thread(target=update_data_loop)
data_thread.daemon = True
data_thread.start()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)