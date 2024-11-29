import requests
import json
import os 

class LoginManager:
    def __init__(self, login_url, check_url, login_payload, headers):
        self.login_url = login_url
        self.check_url = check_url
        self.login_payload = login_payload
        self.headers = headers
        self.authorization = None

    def login(self):
        """
        调用登录接口获取新的 Authorization 值
        """
        try:
            response = requests.post(self.login_url, json=self.login_payload, headers=self.headers)
            if response.status_code != 200:
                print(f"登录请求失败，状态码: {response.status_code}")
                return False

            response_data = response.json()
            return_code = response_data.get("head", {}).get("returnCode")
            if return_code != "00000":
                print(f"登录失败，原因: {response_data.get('head', {}).get('returnMessage', '未知错误')}")
                return False

            # 获取 bizContent 中的 Authorization（假设是 token）
            self.authorization = response_data.get("bizContent", {}).get("token")
            if self.authorization:
                print(f"登录成功，Authorization: {self.authorization}")
                self.headers["Authorization"] = self.authorization
                #将 Authorization 值保存到 txt文档 中，以便后续请求使用
                with open("authorization.txt", "w") as f:
                    f.write(self.authorization)
                return True
            else:
                print("登录成功，但未返回 Authorization 值")
                return False

        except Exception as e:
            print(f"登录时发生错误: {e}")
            return False

    def check_login(self):
        """
        检查登录状态，返回是否有效
        """
        try:
            # 从 txt文档 中读取 Authorization 值,如果不存在，则执行login()
            #如果txt 不存在则为首次登录直接返回false
            #headers 还有一个Content-Type: application/json

            if not os.path.exists("authorization.txt"):
                return False

            with open("authorization.txt", "r") as f:
                self.authorization = f.read()
                self.headers["Authorization"] = self.authorization

            if self.authorization:
                print(f"当前 Authorization: {self.authorization}")
            else:
                print("Authorization 值不存在，请先登录")
                return False
            response = requests.get(self.check_url, headers=self.headers)

            if response.status_code == 200:
                print("登录有效")
                return True
            else:
                print(f"登录无效，状态码: {response.status_code}")
                return False
        except Exception as e:
            print(f"检查登录状态时发生错误: {e}")
            return False

    def logout(self):
        """
        调用登出接口登出
        """
        try:
            response = requests.post(self.logout_url, headers=self.headers)
            if response.status_code != 200:
                print(f"登出请求失败，状态码: {response.status_code}")
                return False

            response_data = response.json()
            return_code = response_data.get("head", {}).get("returnCode")
            if return_code != "00000":
                print(f"登出失败，原因: {response_data.get('head', {}).get('returnMessage', '未知错误')}")
                return False

            print("登出成功")
            return True
        except Exception as e:
            print(f"登出时发生错误: {e}")
            return False



    def get_authorization(self):
        """
        获取有效的 Authorization 值
        """
        if self.check_login():
            print(f"当前 Authorization: {self.authorization}")
            return self.authorization
        else:
            print("重新登录...")
            if self.login():
                return self.authorization
            else:
                print("获取 Authorization 失败")
                return None

# 示例使用
login_url = "https://example.com/api/login"  # 登录接口地址
check_url = "https://example.com/api/check"  # 检查登录状态接口地址
logout_url = "https://example.com/api/logout"  # 登出接口地址
login_payload = {
    "username": "your_username",
    "password": "your_password"
}  # 登录参数
headers = {
    "Content-Type": "application/json"
}  # 请求头

# 创建 LoginManager 实例
login_manager = LoginManager(login_url, check_url, login_payload, headers)

# 获取 Authorization 值
authorization = login_manager.get_authorization()
if authorization:
    print(f"最终 Authorization: {authorization}")
else:
    print("无法获取有效的 Authorization")
