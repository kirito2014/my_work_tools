#-*-coding:utf-8-*-
import requests
import aardio

def getHtml(url): 
    '''
    如果开全局代理，Python 可能会报错（SSLEOFError），
    这个问题是 Python 的问题与 aardio 无关，解决方案请自行上网搜索。
    也可以改用 aardio 中的 inet.http 或 web.rest.client 等抓取网页。
    '''
    r = requests.get(url,verify=True)
    r.encoding='utf-8'
    aardio.setText(r.text)
    return r.text 