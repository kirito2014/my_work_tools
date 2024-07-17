#!/usr/bin/python
#encoding:utf-8
'''
编写目的：获取文件配置中的公司介绍信息写入excel中 
编写人：王穆军                                    
修订时间：2021-6-3   新增   王穆军   v1.0         
修订日志：
'''
import re
import os
import sys 
libs = ['xlrd','xlwt']  #需要使用的库名称，避免因为没有第三方库报错
libs_url = r'https://mirrors.aliyun.com/pypi/simple/'  #阿里云镜像网站
try:
    import xlrd
    import xlwt
    print('--所有依赖库已经安装')
except ModuleNotFoundError:
    print("\033[31m"+'--你的电脑里没有安装应有的库，现在进行安装'+"\033[0m")
    for lib in libs:
        print("--开始安装 {0}".format(lib))
        os.system('pip install %s -i %s'%(lib,libs_url))
        print('{0} 安装成功'.format(lib))
    print('--所有缺失库已经安装 ')
    import xlrd #（excel read）来读取Excel文件
    import xlwt #（excel write）来生成Excel文件

#import xlrd #（excel read）来读取Excel文件
#import xlwt #（excel write）来生成Excel文件
workbook = xlwt.Workbook()  # 新建一个工作簿
sheet = workbook.add_sheet("sheet_name")  # 在工作簿中新建一个表格
os.system('')
def write_excel_xls(path,value,inum):
    index = len(value)  # 获取需要写入数据的行数
    # print("index is",index)
    for num in range(0, index):
        sheet.write(inum,num,value[num])
    # for i in range(0, index):
    #     for j in range(0, len(value[i])):
    #         sheet.write(i, j, value[i][j])  # 像表格中写入数据（对应的行和列）

    print("xls格式表格写入数据成功！")
# import xlutils #暂时没有办法安装xlutils
# def write_excel_xls_append(path, value):
#     index = len(value)  # 获取需要写入数据的行数
#     workbook = xlrd.open_workbook(path)  # 打开工作簿
#     sheets = workbook.sheet_names()  # 获取工作簿中的所有表格
#     worksheet = workbook.sheet_by_name(sheets[0])  # 获取工作簿中所有表格中的的第一个表格
#     rows_old = worksheet.nrows  # 获取表格中已存在的数据的行数
#     new_workbook = xlutils.copy(workbook)  # 将xlrd对象拷贝转化为xlwt对象
#     new_worksheet = new_workbook.get_sheet(0)  # 获取转化后工作簿中的第一个表格
#     for i in range(0, index):
#         for j in range(0, len(value[i])):
#             new_worksheet.write(i+rows_old, j, value[i][j])  # 追加写入数据，注意是从i+rows_old行开始写入
#     new_workbook.save(path)  # 保存工作簿
#     print("xls格式表格【追加】写入数据成功！")



# coding=UTF-8
script_path = os.path.realpath(__file__)
script_dir = os.path.dirname(script_path)
file_folder= script_dir + '/'
file_name="stock_workbook.xls"
file_source="stock.txt"

# print(file_foler+file_name)

#构造url
url_source=open(file_folder+file_source)
f=open(file_folder+file_source,'rb')
stock = []
for line in f.readlines():
    line=line.decode('utf-8')
    # print(line,end = '')
    line = line.replace('\n','')
    stock.append(line)
#print(stock)
f.close()
#print(stock)
iinum = 1
for each in stock:
    # print(each[2:])
    each1=each[0:6]
    # print(each)
    url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_CorpInfo/stockid/'+each1+'.phtml'

    # 开始爬虫
    import requests
    headers = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}
    # print(url)
    response = requests.get(url, headers=headers)
    response.encoding = 'gbk'  # 解决乱码问题
    html_text = response.text
    # 开始爬虫

    import lxml
    from lxml import etree
    selector = etree.HTML(html_text)
    title = selector.xpath('//table/tr/td//text()')

    # 去除其中的冒号
    title_1 = []
    for i in title:
        i = i.strip()
        i = i.strip('：')
        title_1.append(i)
    print("title_1", title_1[0:40])
    # 去除其中的冒号

    # 存入excel
    path1 = file_folder + '3' + file_name
    print(path1)
    write_excel_xls(path1, title_1,iinum)
    workbook.save(path1)  # 保存工作簿
    # 存入excel
    iinum=iinum+1

print("\033[42m"+'--所有年报已下载完成，到相应文件夹中查看--'+"\033[0m")
