import mysql.connector
from mysql.connector import Error
import config

def test_mysql_connection():
    """
    测试MySQL数据库连接的函数
    使用config.py中的数据库配置进行连接测试
    """
    connection = None
    try:
        # 从配置文件获取数据库连接信息
        db_config = {
            'host': config.DB_CONFIG['host'],
            'database': config.DB_CONFIG['database'],
            'user': config.DB_CONFIG['user'],
            'password': config.DB_CONFIG['password'],
            'port': config.DB_CONFIG.get('port', 3306)  # 默认MySQL端口
        }

        # 尝试建立连接
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"成功连接到MySQL服务器，版本: {db_info}")

            # 执行简单查询测试
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION();")
            record = cursor.fetchone()
            print(f"数据库版本: {record}")

            # 检查数据库是否存在
            cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_config['database']}';")
            db_exists = cursor.fetchone()
            if db_exists:
                print(f"数据库 '{db_config['database']}' 存在")
            else:
                print(f"警告: 数据库 '{db_config['database']}' 不存在")

            return True

    except Error as e:
        print(f"连接错误: {e}")
        return False
    finally:
        # 确保连接关闭
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL连接已关闭")

if __name__ == "__main__":
    success = test_mysql_connection()
    # 程序退出码：0表示成功，1表示失败
    exit(0 if success else 1)