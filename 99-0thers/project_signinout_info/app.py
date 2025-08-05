from flask import Flask, jsonify, render_template
from decimal import Decimal
from json import JSONEncoder
import random
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error

from config import DB_CONFIG

app = Flask(__name__)

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(CustomJSONEncoder, self).default(obj)

app.json_encoder = CustomJSONEncoder
app.config['JSON_AS_ASCII'] = False  # 支持中文显示

# 模拟员工数据
EMPLOYEES = 22  # 总员工数
DEPARTMENTS = ["对公组", "零售组", "信贷组", "资管组", "通用组", "自助分析"]

def get_db_connection():
    """建立数据库连接并返回连接对象，失败时返回None"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG.get('port', 3306),
            charset=DB_CONFIG.get('charset', 'utf8mb4'),

        )
        return connection
    except Error as e:
        print(f"数据库连接错误: {e}")
        return None


def query_db(query, params=None):
    """执行数据库查询并返回结果"""
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, params or ())
            result = cursor.fetchall()
        connection.commit()
        return result
    except Exception as e:
        print(f"Database query error: {str(e)}")
        print(f"Failed query: {query}")
        print(f"Query parameters: {params}")
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
            WITH RECURSIVE hours AS (
                SELECT 0 AS hour
                UNION ALL
                SELECT hour + 1 FROM hours WHERE hour < 23
            ),
            latest_date AS (
                SELECT max(atten_dt) AS max_dt 
                FROM ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp
            ),
            checkin_data AS (
                SELECT
                    HOUR(p1.EARLIEST_SINGIN_TM) AS hour,
                    COUNT(*) AS checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                WHERE
                    p1.ATTEN_DT = (SELECT max_dt FROM latest_date)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    HOUR(p1.EARLIEST_SINGIN_TM)
            )
            SELECT 
                h.hour,
                COALESCE(c.checkin, 0) AS checkin
            FROM 
                hours h
            LEFT JOIN 
                checkin_data c ON h.hour = c.hour
            ORDER BY 
                h.hour;
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
        query = """
            WITH date_range AS (
                SELECT 
                    DATE_SUB(CURRENT_DATE(), INTERVAL n DAY) AS date
                FROM (
                    SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 
                    UNION SELECT 4 UNION SELECT 5 UNION SELECT 6
                ) AS days
            ),
            batch_0815 AS (
                SELECT
                    DATE(p1.STD_ATTEN_DT) AS date,
                    '0815' AS batch,
                    COUNT(*) AS checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND DATE(p1.STD_ATTEN_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    DATE(p1.STD_ATTEN_DT)
            ),
            batch_0850 AS (
                SELECT
                    DATE(p1.STD_ATTEN_DT) AS date,
                    '0850' AS batch,
                    COUNT(*) AS checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND DATE(p1.STD_ATTEN_DT) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    DATE(p1.STD_ATTEN_DT)
            )
            SELECT 
                d.date as date,
                COALESCE(b0815.checkin, 0) + COALESCE(b0850.checkin, 0) AS checkin
            FROM 
                date_range d
            LEFT JOIN 
                batch_0815 b0815 ON d.date = b0815.date
            LEFT JOIN 
                batch_0850 b0850 ON d.date = b0850.date
            ORDER BY 
                d.date asc;
        """
        results = query_db(query)
        print(results)
        if results:
            for row in results:
                data.append({
                    "date": row['date'].strftime("%m-%d"),
                    "checkin": row['checkin']
                })
        else:
            # 模拟数据 - 当数据库查询失败时使用
            for day in range(7):
                date = (now - timedelta(days=6 - day)).strftime("%m-%d")
                checkin = random.randint(100, 140)
                data.append({
                    "date": date,
                    "checkin": checkin
                })
    elif time_range == "month":
        # 本月每周数据
        query = """
            WITH 
            -- 获取当前月份的第一天和最后一天
            current_month AS (
                SELECT 
                    DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01') AS first_day,
                    LAST_DAY(CURRENT_DATE()) AS last_day
            ),
            -- 生成当前月的所有周（最多6周）
            month_weeks AS (
                SELECT 
                    1 AS week_seq,
                    '第1周' AS week_name,
                    first_day AS week_start,
                    LEAST(DATE_ADD(first_day, INTERVAL 6 DAY), last_day) AS week_end
                FROM current_month
                UNION ALL SELECT 2, '第2周', DATE_ADD(first_day, INTERVAL 7 DAY), LEAST(DATE_ADD(first_day, INTERVAL 13 DAY), last_day) FROM current_month
                UNION ALL SELECT 3, '第3周', DATE_ADD(first_day, INTERVAL 14 DAY), LEAST(DATE_ADD(first_day, INTERVAL 20 DAY), last_day) FROM current_month
                UNION ALL SELECT 4, '第4周', DATE_ADD(first_day, INTERVAL 21 DAY), LEAST(DATE_ADD(first_day, INTERVAL 27 DAY), last_day) FROM current_month
                UNION ALL SELECT 5, '第5周', DATE_ADD(first_day, INTERVAL 28 DAY), last_day FROM current_month
                UNION ALL SELECT 6, '第6周', DATE_ADD(first_day, INTERVAL 35 DAY), last_day FROM current_month
                WHERE DATE_ADD(first_day, INTERVAL 35 DAY) <= last_day
            ),
            -- 0815批次每周统计
            batch_0815 AS (
                SELECT 
                    CEILING(DAYOFMONTH(p1.STD_ATTEN_DT)/7.0) AS week_seq,
                    COUNT(*) AS checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT first_day FROM current_month) AND (SELECT last_day FROM current_month)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    CEILING(DAYOFMONTH(p1.STD_ATTEN_DT)/7.0)
            ),
            -- 0850批次每周统计
            batch_0850 AS (
                SELECT 
                    CEILING(DAYOFMONTH(p1.STD_ATTEN_DT)/7.0) AS week_seq,
                    COUNT(*) AS checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT first_day FROM current_month) AND (SELECT last_day FROM current_month)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    CEILING(DAYOFMONTH(p1.STD_ATTEN_DT)/7.0)
            )
            -- 最终结果
            SELECT 
                mw.week_name AS week,
            --     COALESCE(b0815.checkin, 0) AS '0815批次签到数',
            --     COALESCE(b0850.checkin, 0) AS '0850批次签到数',
                COALESCE(b0815.checkin, 0) + COALESCE(b0850.checkin, 0) AS checkin
            FROM 
                month_weeks mw
            LEFT JOIN 
                batch_0815 b0815 ON mw.week_seq = b0815.week_seq
            LEFT JOIN 
                batch_0850 b0850 ON mw.week_seq = b0850.week_seq
            WHERE
                mw.week_start <= (SELECT last_day FROM current_month)
            ORDER BY 
                mw.week_seq;
        """
        results = query_db(query)
        print(results)
        if results:
            for row in results:
                data.append({
                    "week": row['week'],
                    "checkin": row['checkin']
                })
        else:
            # 模拟数据 - 当数据库查询失败时使用
            for week in range(4):
                data.append({
                    "week": f"第{week+1}周",
                    "checkin": random.randint(450, 550)
                })
    elif time_range == "quarter":
        # 本季度每月数据
        query = """
            WITH 
            -- 生成最近3个月的月份范围
            month_range AS (
                SELECT 
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), '%Y-%m-01') AS month_start,
                    LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)) AS month_end,
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), '%m') AS month_num,
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), '%M') AS month_name_en
                UNION ALL
                SELECT 
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%Y-%m-01'),
                    LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)),
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%m'),
                    DATE_FORMAT(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), '%M')
                UNION ALL
                SELECT 
                    DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01'),
                    LAST_DAY(CURRENT_DATE()),
                    DATE_FORMAT(CURRENT_DATE(), '%m'),
                    DATE_FORMAT(CURRENT_DATE(), '%M')
            ),
            -- 中文月份映射
            month_mapping AS (
                SELECT '01' AS num, '一月' AS name UNION ALL
                SELECT '02', '二月' UNION ALL
                SELECT '03', '三月' UNION ALL
                SELECT '04', '四月' UNION ALL
                SELECT '05', '五月' UNION ALL
                SELECT '06', '六月' UNION ALL
                SELECT '07', '七月' UNION ALL
                SELECT '08', '八月' UNION ALL
                SELECT '09', '九月' UNION ALL
                SELECT '10', '十月' UNION ALL
                SELECT '11', '十一月' UNION ALL
                SELECT '12', '十二月'
            ),
            -- 合并签到数据
            checkin_data AS (
                SELECT 
                    DATE_FORMAT(p1.STD_ATTEN_DT, '%Y-%m') AS month_key,
                    COUNT(*) AS total_checkin
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                WHERE
                    p1.STD_ATTEN_DT BETWEEN DATE_SUB(DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01'), INTERVAL 2 MONTH) 
                                        AND LAST_DAY(CURRENT_DATE())
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                GROUP BY
                    DATE_FORMAT(p1.STD_ATTEN_DT, '%Y-%m')
            )
            -- 最终结果
            SELECT 
                mm.name AS 'month',
                COALESCE(cd.total_checkin, 0) AS 'checkin'
            FROM 
                month_range mr
            JOIN 
                month_mapping mm ON mr.month_num = mm.num
            LEFT JOIN 
                checkin_data cd ON DATE_FORMAT(mr.month_start, '%Y-%m') = cd.month_key
            ORDER BY 
                mr.month_start;
        """
        results = query_db(query)
        print(results)
        if results:
            for row in results:
                data.append({
                    "month": row['month'],
                    "checkin": row['checkin']
                })
        else:
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
            WITH latest_date AS (
                SELECT max(atten_dt) AS max_dt 
                FROM ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp
            ),
            batch_0815 AS (
                SELECT 
                    '0815' AS batch,
                    TIME_FORMAT(p1.EARLIEST_SINGIN_TM, '%H:%i') AS time,
                    TIMESTAMPDIFF(MINUTE, TIME('08:15:00'),time(p1.EARLIEST_SINGIN_TM)) AS delay
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND p1.ATTEN_DT = (SELECT max_dt FROM latest_date)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND p1.EARLIEST_SINGIN_TM > TIME('08:15:00')
            ),
            batch_0850 AS (
                SELECT 
                    '0850' AS batch,
                    TIME_FORMAT(p1.EARLIEST_SINGIN_TM, '%H:%i') AS time,
                    TIMESTAMPDIFF(MINUTE, TIME('08:50:00'),time(p1.EARLIEST_SINGIN_TM)) AS delay
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND p1.ATTEN_DT = (SELECT max_dt FROM latest_date)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND p1.EARLIEST_SINGIN_TM > TIME('08:50:00')
            )
            SELECT time,delay FROM batch_0815
            UNION ALL
            SELECT time,delay FROM batch_0850
            ORDER BY  time;
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
    elif time_range == "week":
        #查询本周的迟到记录
        query = """
            WITH 
            -- 获取本周一日期
            week_start AS (
                SELECT DATE_SUB(CURRENT_DATE(), INTERVAL WEEKDAY(CURRENT_DATE()) DAY) AS monday
            ),
            -- 生成完整一周的日期（周一到周日）
            week_days AS (
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 0 DAY) AS day,
                    '1' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 1 DAY) AS day,
                    '2' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 2 DAY) AS day,
                    '3' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 3 DAY) AS day,
                    '4' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 4 DAY) AS day,
                    '5' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 5 DAY) AS day,
                    '6' AS day_name
                UNION ALL
                SELECT 
                    DATE_ADD((SELECT monday FROM week_start), INTERVAL 6 DAY) AS day,
                    '7' AS day_name
            ),
            -- 0815批次当周迟到数据
            batch_0815_late AS (
                SELECT 
                    DATE(p1.STD_ATTEN_DT) AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT monday FROM week_start) 
                                        AND DATE_ADD((SELECT monday FROM week_start), INTERVAL 6 DAY)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:15:00')
                GROUP BY
                    DATE(p1.STD_ATTEN_DT)
            ),
            -- 0850批次当周迟到数据
            batch_0850_late AS (
                SELECT 
                    DATE(p1.STD_ATTEN_DT) AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT monday FROM week_start) 
                                        AND DATE_ADD((SELECT monday FROM week_start), INTERVAL 6 DAY)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:50:00')
                GROUP BY
                    DATE(p1.STD_ATTEN_DT)
            ),
            -- 合并两个批次的迟到数据
            combined_late AS (
                SELECT day, late_count FROM batch_0815_late
                UNION ALL
                SELECT day, late_count FROM batch_0850_late
            ),
            -- 按天汇总迟到人数
            daily_late AS (
                SELECT 
                    day,
                    SUM(late_count) AS total_late
                FROM 
                    combined_late
                GROUP BY 
                    day
            )
            -- 最终结果：显示完整一周，包括没有迟到记录的日期
            SELECT 
                
                w.day_name AS day,
                COALESCE(d.total_late, 0) AS late_count
            FROM 
                week_days w
            LEFT JOIN 
                daily_late d ON w.day = d.day
            ORDER BY 
                w.day_name;
        """
        results = query_db(query)
        
        if results:
            for row in results:
                data.append({
                    "day": row['day'],
                    "late_count": row['late_count']
                })
        else:
        # 模拟数据 - 当数据库查询失败时使用
            days = 7
            for day in range(days):
                data.append({
                    "day": day + 1,
                    "late_count": random.randint(5, 20)
                })  
    elif time_range == "month":
        #查询本月的迟到记录
        query = """
            WITH 
            -- 获取当月1号和当前日期
            current_month AS (
                SELECT 
                    DATE_FORMAT(CURRENT_DATE(), '%Y-%m-01') AS first_day,
                    CURRENT_DATE() AS today
            ),
            -- 生成当月1号到今天的日期序列
            month_days AS (
                SELECT 
                    day_of_month AS day,
                    DATE_ADD((SELECT first_day FROM current_month), INTERVAL day_of_month-1 DAY) AS full_date
                FROM (
                    SELECT 1 AS day_of_month UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
                    UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15
                    UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20
                    UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24 UNION SELECT 25
                    UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29 UNION SELECT 30
                    UNION SELECT 31
                ) AS days
                WHERE 
                    day_of_month <= DAY((SELECT today FROM current_month))
            ),
            -- 0815批次当月迟到数据
            batch_0815_late AS (
                SELECT 
                    DAY(p1.STD_ATTEN_DT) AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT first_day FROM current_month) 
                                        AND (SELECT today FROM current_month)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:15:00')
                GROUP BY
                    DAY(p1.STD_ATTEN_DT)
            ),
            -- 0850批次当月迟到数据
            batch_0850_late AS (
                SELECT 
                    DAY(p1.STD_ATTEN_DT) AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT first_day FROM current_month) 
                                        AND (SELECT today FROM current_month)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:50:00')
                GROUP BY
                    DAY(p1.STD_ATTEN_DT)
            ),
            -- 合并两个批次的迟到数据
            combined_late AS (
                SELECT day, late_count FROM batch_0815_late
                UNION ALL
                SELECT day, late_count FROM batch_0850_late
            ),
            -- 按天汇总迟到人数
            daily_late AS (
                SELECT 
                    day,
                    SUM(late_count) AS total_late
                FROM 
                    combined_late
                GROUP BY 
                    day
            )
            -- 最终结果：显示当月1号到今天的每天迟到人数
            SELECT 
                m.day,
                COALESCE(d.total_late, 0) AS late_count
            FROM 
                month_days m
            LEFT JOIN 
                daily_late d ON m.day = d.day
            ORDER BY 
                m.day;
        """
        results = query_db(query)
        
        if results:
            for row in results:
                data.append({
                    "day": row['day'],
                    "late_count": row['late_count']
                })
        else:
        # 模拟数据 - 当数据库查询失败时使用
            days = 30
            for day in range(days):
                data.append({
                    "day": day + 1,
                    "late_count": random.randint(5, 20)
                })  
    elif time_range == "quarter":
        #查询本月的迟到记录
        query = """
            WITH 
            -- 获取当前季度的第一天和当前日期
            current_quarter AS (
                SELECT 
                    CASE 
                        WHEN MONTH(CURRENT_DATE()) BETWEEN 1 AND 3 THEN DATE_FORMAT(CURRENT_DATE(), '%Y-01-01')
                        WHEN MONTH(CURRENT_DATE()) BETWEEN 4 AND 6 THEN DATE_FORMAT(CURRENT_DATE(), '%Y-04-01')
                        WHEN MONTH(CURRENT_DATE()) BETWEEN 7 AND 9 THEN DATE_FORMAT(CURRENT_DATE(), '%Y-07-01')
                        ELSE DATE_FORMAT(CURRENT_DATE(), '%Y-10-01')
                    END AS quarter_start,
                    CURRENT_DATE() AS today
            ),
            -- 生成季度开始到今天的日期序列（带序号）
            quarter_days AS (
                SELECT 
                    ROW_NUMBER() OVER () AS day,
                    date_series.date
                FROM (
                    SELECT 
                        DATE_ADD((SELECT quarter_start FROM current_quarter), INTERVAL seq DAY) AS date
                    FROM (
                        SELECT 0 AS seq UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
                        UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14
                        UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
                        UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23 UNION SELECT 24
                        UNION SELECT 25 UNION SELECT 26 UNION SELECT 27 UNION SELECT 28 UNION SELECT 29
                        UNION SELECT 30 UNION SELECT 31 UNION SELECT 32 UNION SELECT 33 UNION SELECT 34
                        UNION SELECT 35 UNION SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39
                        UNION SELECT 40 UNION SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44
                        UNION SELECT 45 UNION SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49
                        UNION SELECT 50 UNION SELECT 51 UNION SELECT 52 UNION SELECT 53 UNION SELECT 54
                        UNION SELECT 55 UNION SELECT 56 UNION SELECT 57 UNION SELECT 58 UNION SELECT 59
                        UNION SELECT 60 UNION SELECT 61 UNION SELECT 62 UNION SELECT 63 UNION SELECT 64
                        UNION SELECT 65 UNION SELECT 66 UNION SELECT 67 UNION SELECT 68 UNION SELECT 69
                        UNION SELECT 70 UNION SELECT 71 UNION SELECT 72 UNION SELECT 73 UNION SELECT 74
                        UNION SELECT 75 UNION SELECT 76 UNION SELECT 77 UNION SELECT 78 UNION SELECT 79
                        UNION SELECT 80 UNION SELECT 81 UNION SELECT 82 UNION SELECT 83 UNION SELECT 84
                        UNION SELECT 85 UNION SELECT 86 UNION SELECT 87 UNION SELECT 88 UNION SELECT 89
                        UNION SELECT 90 UNION SELECT 91 UNION SELECT 92 -- 最多92天（3个月+）
                    ) AS seq_nums
                    WHERE 
                        DATE_ADD((SELECT quarter_start FROM current_quarter), INTERVAL seq DAY) <= 
                        (SELECT today FROM current_quarter)
                ) AS date_series
            ),
            -- 0815批次当季迟到数据
            batch_0815_late AS (
                SELECT 
                    DATEDIFF(p1.STD_ATTEN_DT, (SELECT quarter_start FROM current_quarter)) + 1 AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0815'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT quarter_start FROM current_quarter) 
                                        AND (SELECT today FROM current_quarter)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:15:00')
                GROUP BY
                    DATEDIFF(p1.STD_ATTEN_DT, (SELECT quarter_start FROM current_quarter)) + 1
            ),
            -- 0850批次当季迟到数据
            batch_0850_late AS (
                SELECT 
                    DATEDIFF(p1.STD_ATTEN_DT, (SELECT quarter_start FROM current_quarter)) + 1 AS day,
                    COUNT(*) AS late_count
                FROM
                    ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
                INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON p1.EMPLY_NAME = p2.EMPLY_NAME AND p2.IMPL_FLAG = 'Y'
                INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON p2.emply_name = p3.emply_name
                WHERE
                    p3.atten_batch = '0850'
                    AND p1.STD_ATTEN_DT BETWEEN (SELECT quarter_start FROM current_quarter) 
                                        AND (SELECT today FROM current_quarter)
                    AND p1.EARLIEST_SINGIN_TM IS NOT NULL
                    AND TIME(p1.EARLIEST_SINGIN_TM) > TIME('08:50:00')
                GROUP BY
                    DATEDIFF(p1.STD_ATTEN_DT, (SELECT quarter_start FROM current_quarter)) + 1
            ),
            -- 合并两个批次的迟到数据
            combined_late AS (
                SELECT day, late_count FROM batch_0815_late
                UNION ALL
                SELECT day, late_count FROM batch_0850_late
            ),
            -- 按天汇总迟到人数
            daily_late AS (
                SELECT 
                    day,
                    SUM(late_count) AS total_late
                FROM 
                    combined_late
                GROUP BY 
                    day
            )
            -- 最终结果：显示当季第1天到今天的每天迟到人数
            SELECT 
                q.day,
                COALESCE(d.total_late, 0) AS late_count
            FROM 
                quarter_days q
            LEFT JOIN 
                daily_late d ON q.day = d.day
            ORDER BY 
                q.day;
        """
        results = query_db(query)
        
        if results:
            for row in results:
                data.append({
                    "day": row['day'],
                    "late_count": row['late_count']
                })
        else:
        # 模拟数据 - 当数据库查询失败时使用
            days = 100
            for day in range(days):
                data.append({
                    "day": day + 1,
                    "late_count": random.randint(5, 20)
                })  
    # else:
    #     # 其他时间范围按天分布
    #     days = 7 if time_range == "week" else 30 if time_range == "month" else 90
    #     for day in range(days):
    #         data.append({
    #             "day": day + 1,
    #             "late_count": random.randint(5, 20)
    #         })
    return data

def generate_batch_distribution():
    """生成签到批次分布数据
    8:15批次和8:50批次人数
    """
    # 从数据库获取签到批次分布数据
    query = """
        select oibabi.atten_batch as name ,count(1) as value  from ods_sunline.ods_sunline_psn_binfo ospb 
        left join ods_sunline.ods_in_bank_atten_base_info oibabi 
        on oibabi.emply_name =ospb.emply_name
        where ospb.impl_flag ='Y'
        group by oibabi.atten_batch;
    """
    results = query_db(query)
    print(results)
    
    if results:
        # 确保两个批次都有数据，缺失的批次补0
        batch_map = {item['name']: item['value'] for item in results}
        return [
            {"name": "8:15批次", "value": batch_map.get('0815', 0)},
            {"name": "8:50批次", "value": batch_map.get('0850', 0)}
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
    # 查询在职员工总数
    query = """
    SELECT COUNT(*) as count FROM ods_sunline.ods_sunline_psn_binfo WHERE IMPL_FLAG = 'Y'
    """
    result = query_db(query)
    TOTAL_EMPLOYEES = result[0]['count'] if result else 0
    query = """
    select count(*) from ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1  
        inner join  ods_sunline.ods_sunline_psn_binfo p2 
        on p1.EMPLY_NAME  = p2.EMPLY_NAME and  IMPL_FLAG = 'Y'
        where p1.ATTEN_DT  = 
        (select max(atten_dt) from ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp)
        and p1.EARLIEST_SINGIN_TM  is not null;    """
    result = query_db(query)
    today_checkin = result[0]['count'] if result else 0
    not_checkin = TOTAL_EMPLOYEES - today_checkin
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
        time_clause = "atten_dt  = (SELECT max(atten_dt) AS max_dt     FROM ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp)"
    elif time_range == "week":
        time_clause = "DATE_FORMAT(atten_dt,'%Y-%m-%d')  >=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL (DAYOFWEEK(NOW()) - 2) DAY),'%Y-%m-%d')"
    elif time_range == "month":
        time_clause = "DATE_FORMAT(atten_dt,'%Y-%m-%d')  >=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL (DAYOFMONTH(NOW()) -1 ) DAY),'%Y-%m-%d')"
    else:  # quarter
        time_clause = "QUARTER(atten_dt) = QUARTER(NOW()) AND YEAR(atten_dt) = YEAR(NOW())"
    
    query = f"""
            select department ,case when ot <0 then 0 else ot end as overtime
            from (SELECT
                p3.CUST_FUNCTION department,
                sum(CASE
                    WHEN p3.ATTEN_BATCH = '0815' THEN TIMESTAMPDIFF(HOUR, time('18:15:00'), TIME(p1.latst_signout_tm) )
                    WHEN p3.ATTEN_BATCH = '0850' THEN TIMESTAMPDIFF(HOUR,  time('18:50:00'),TIME(p1.latst_signout_tm) )
                end ) 
                as ot
            FROM
                ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1
            INNER JOIN ods_sunline.ods_sunline_psn_binfo p2 
                    ON
                p1.EMPLY_NAME = p2.EMPLY_NAME
                AND IMPL_FLAG = 'Y'
            INNER JOIN ods_sunline.ods_in_bank_atten_base_info p3 
                    ON
                p2.emply_name = p3.emply_name
            WHERE {time_clause}
            GROUP BY
                p3.CUST_FUNCTION
                ) t
    """
    results = query_db(query)
    print(results)
    
    if results:
        # 将查询结果转换为所需格式
        result_map = {item['department']: item['overtime'] for item in results}
        for dept in set(item['department']  for item in results):
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
    print(data)
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
            with c0815 as (
            SELECT
                COUNT(*) as checkin_0815,
                SUM(CASE WHEN p1.EARLIEST_SINGIN_TM  > TIME('08:15:00') THEN 1 ELSE 0 END) as late_0815
            FROM
                ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1  
            inner join  ods_sunline.ods_sunline_psn_binfo p2 
            on p1.EMPLY_NAME  = p2.EMPLY_NAME and  IMPL_FLAG = 'Y'
            inner join ods_sunline.ods_in_bank_atten_base_info p3 
            on p2.emply_name =p3.emply_name

            WHERE
            p3.atten_batch = '0815'
            and DATE(p1.STD_ATTEN_DT) BETWEEN %s AND %s
            ),
            c0850 as (
            SELECT
                COUNT(*) as checkin_0850,
                SUM(CASE WHEN p1.EARLIEST_SINGIN_TM  > TIME('08:50:00') THEN 1 ELSE 0 END) as late_0850
            FROM
                ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1  
            inner join  ods_sunline.ods_sunline_psn_binfo p2 
            on p1.EMPLY_NAME  = p2.EMPLY_NAME and  IMPL_FLAG = 'Y'
            inner join ods_sunline.ods_in_bank_atten_base_info p3 
            on p2.emply_name =p3.emply_name

            WHERE
            p3.atten_batch = '0850'
            and DATE(p1.STD_ATTEN_DT) BETWEEN %s AND %s
            )
            SELECT 
                (coalesce(c0815.checkin_0815,0) + coalesce(c0850.checkin_0850,0)) AS total_checkin,
                (coalesce(c0815.late_0815,0) + coalesce(c0850.late_0850,0)) AS total_late 
            FROM 
                c0815, c0850;
    """
    current_results = query_db(current_query, (current_start, current_end,current_start, current_end))
    print("current_results:",current_results)
    current_data = current_results[0] if current_results else None
    
    # 查询上一周期数据
    prev_query = """
            with c0815 as (
            SELECT
                COUNT(*) as checkin_0815,
                SUM(CASE WHEN p1.EARLIEST_SINGIN_TM  > TIME('08:15:00') THEN 1 ELSE 0 END) as late_0815
            FROM
                ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1  
            inner join  ods_sunline.ods_sunline_psn_binfo p2 
            on p1.EMPLY_NAME  = p2.EMPLY_NAME and  IMPL_FLAG = 'Y'
            inner join ods_sunline.ods_in_bank_atten_base_info p3 
            on p2.emply_name =p3.emply_name

            WHERE
            p3.atten_batch = '0815'
            and DATE(p1.STD_ATTEN_DT) BETWEEN %s AND %s
            ),
            c0850 as (
            SELECT
                COUNT(*) as checkin_0850,
                SUM(CASE WHEN p1.EARLIEST_SINGIN_TM  > TIME('08:50:00') THEN 1 ELSE 0 END) as late_0850
            FROM
                ods_sunline.ods_in_bank_psn_atten_dtl_in_bank_exp p1  
            inner join  ods_sunline.ods_sunline_psn_binfo p2 
            on p1.EMPLY_NAME  = p2.EMPLY_NAME and  IMPL_FLAG = 'Y'
            inner join ods_sunline.ods_in_bank_atten_base_info p3 
            on p2.emply_name =p3.emply_name

            WHERE
            p3.atten_batch = '0850'
            and DATE(p1.STD_ATTEN_DT) BETWEEN %s AND %s
            )
            SELECT 
                (coalesce(c0815.checkin_0815,0) + coalesce(c0850.checkin_0850,0)) AS total_checkin,
                (coalesce(c0815.late_0815,0) + coalesce(c0850.late_0850,0)) AS total_late 
            FROM 
                c0815, c0850;
    """
    prev_results = query_db(prev_query, (prev_start, prev_end, prev_start, prev_end))
    print("prev_results:" ,prev_results)
    prev_data = prev_results[0] if prev_results else None
    
    # 查询总员工数
    total_query = "SELECT COUNT(*) as count FROM ods_sunline.ods_sunline_psn_binfo WHERE IMPL_FLAG = 'Y'"
    total_data = query_db(total_query)[0] if query_db(total_query) else None
    total_employees = total_data['count'] if total_data else 22
    print("current_data:",current_data)
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
        current_checkin = current_data.get('total_checkin', 0) or 0
        current_late = current_data.get('total_late', 0) or 0
        prev_checkin = prev_data.get('total_checkin', 0) or 0
        prev_late = prev_data.get('total_late', 0) or 0
        #prev_rate = prev_data.get('late_percentage', 0) or 0
    print("current_checkin:",current_checkin)
    current_rate = round(current_late / current_checkin * 
100, 1) if current_checkin > 0 else 0
    prev_rate = round(prev_late / prev_checkin * 100, 1) if prev_checkin > 0 else 0
    print(current_rate)
    print(prev_rate)
    # 计算趋势和百分比变化
    def calculate_trend(current, previous, is_percent=False):
        # 确保 current 和 previous 是相同类型（转换为 Decimal）
        from decimal import Decimal
        if isinstance(current, float):
            current = Decimal(str(current))
        if isinstance(previous, float):
            previous = Decimal(str(previous))

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

@app.route('/api/total_present')
def get_total_present():
    try:
        # 查询在职员工总数
        query = "SELECT COUNT(*) as count FROM ods_sunline.ods_sunline_psn_binfo WHERE IMPL_FLAG = 'Y'"
        result = query_db(query)
        count = result[0]['count'] if result else 0
        print(f"Total present: {count}")
        return jsonify({'total_present': count})
    except Exception as e:
        print(f"Database query error: {e}")
        return jsonify({'total_present': 0})

if __name__ == '__main__':
    app.run(debug=True)