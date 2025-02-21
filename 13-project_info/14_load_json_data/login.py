import requests
import json
import os
import time
#登录模块
#检查是否登录
#登出模块
#10122 返回值为超时登录


class LoginManager:
    def __init__(self,login_url,check_url,logout_url,headers,auth_file):
        self.login_url = login_url
        self.check_url = check_url
        self.logout_url = logout_url
        self.headers = headers
        self.auth_file = auth_file
        self.authorization = None

    def login(self):
        """
        调用登录接口获取新的认证串
        """
        try:
            #login_url = "http://158.219.232.42:7777/api/login"
            payload = "{\"password\":\"cdf4a007e2b02a0c49fc9b7ccfbb8a10c644f635e1765dcf2a7ab794ddc7edac\",\"username\":\"wangmujun\"}"
            # headers = {
            # 'Content-Type': 'application/json;charset=UTF-8'
            # }
            response = requests.request("POST", self.login_url, headers=self.headers, data=payload)
            if response.status_code !=200:
                print(f"登录请求失败,状态码：{response.status_code},错误内容：{response.text}.")
                return False
            #解析登录信息
            response_data = response.json()
            #检查head中的returnCode
            head = response_data.get("head",{})
            return_code = head.get("returnCode")
            if return_code != "00000":
                return_message = head.get("returnMessage","错误")
                print(f"登录失败,{return_message}.")
                return False
            #提取bizContent
            self.authorization = response_data.get("bizContent")
            if not self.authorization:
                print("登录成功,但未返回认证信息.")
                return False
            print(f"登录成功,已获取认证串.")
            self.headers["Authorization"] = self.authorization
            with open(self.auth_file,"w") as f:
                f.write(self.authorization)
            return True
        except Exception as e:
            print(f"处理过程发生错误: {e}.")
            return False


    def check_login(self):
        """
        检查登录状态
        """
        # headers = {
        # 'Content-Type': 'application/json;charset=UTF-8'
        # }
        try:
            #check_url="http://158.219.232.42:7777/api/bdsp/common/app"
            payload = "{}"
            # headers = {
            # 'Content-Type': 'application/json;charset=UTF-8'
            # }
            #从txt文件读取Authorization的值，如果不存在，则执行，若认证文件不存在则直接返回fasle
            if not os.path.exists(self.auth_file):
                print(f"未发现认证文件,将要重新认证.")
                return False
            with open(self.auth_file,'r') as f:
                self.authorization = f.read()
                self.headers["Authorization"] = self.authorization

            if self.authorization:
                print(f"获取已保存的认证信息.")
            else:
                print(f"认证串不存在,即将重新登录.")
                return False
            response = requests.request("POST",self.check_url,headers=self.headers,data=payload)
            response = response.json()
            head = response.get("head",{})
            return_code = head.get("returnCode")
            if return_code == "10122":
                return_message = head.get("returnMessage","错误")
                print(f"登录失败:{return_message}.")
                return False
            elif return_code == "00000":
                return_message = head.get("returnMessage","成功")
                print(f"登录有效无需重复登录,{return_message}.")
                #authorization = self.authorization
                return True
        except Exception as e:
            print(f"检查登录状态时发生错误：{e}.")
            return False

    def get_authorization(self):
        if self.check_login():
            print(f"已获取存量认证文件.")
            return self.authorization
        else:
            print("重新登录获取认证中...")
            if self.login():
                return self.authorization
            else:
                print("获取认证失败.")
                return None
    #登出网站，检查登录状态，获取认证文件，若已经过期则默认已到期，如有效则执行登出 并将认证文件中的内容清空
    def logout(self):
        if not self.check_login():
            print(f"登录已过期,无需登出.")
            return 
        try:
            #print(f"存量11111：{self.authorization}")
            headers["Authorization"] = self.authorization
            response = requests.request("POST",self.logout_url,headers=self.headers).json()
            head = response.get("head",{})
            return_code = head.get("returnCode")
            if  return_code == "00000":
                return_message = head.get("returnMessage","成功")
                print(f"登出成功,{return_message}.")
                #authorization = self.authorization
                return True
                self.authorization = None
                self.headers.pop("Authorization",None)
                with open(self.auth_file,"w") as lo:
                    lo.write("")
            else:
                print(f"登出失败,{return_message}.")
        except Exception as e:
            print(f"登出失败,{e}.")

###################################################
#
#  下面是引用案例
#
###################################################
login_url = "http://158.219.232.42:7777/api/login"
check_url="http://158.219.232.42:7777/api/bdsp/common/app"
logout_url="http://158.219.232.42:7777/api/logout"
headers = {
'Content-Type': 'application/json;charset=UTF-8'
}
auth_file="authorization.txt"

login_manager = LoginManager(login_url,check_url,logout_url,headers,auth_file)
# authorization = login_manager.get_authorization()

# print(authorization)
# if authorization:
#     #测试登出
#     time.sleep(20)
#     #login_manager.logout()
# else:
#     print(f"无法获取.")