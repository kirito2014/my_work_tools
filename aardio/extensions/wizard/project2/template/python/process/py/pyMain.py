# coding: utf-8

import sys;
import json
from jsonrpyc import RPC;      

#定义允许客户端调用的类
class MyTarget(object):

    def greet(self, name): 
        return "Hi, %s!" % name
 
    def add(self, a,b): 
        return a + b

if __name__ == "__main__":
    RPC( MyTarget() ) #启动 JSON-RPC 服务端