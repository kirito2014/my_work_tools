# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 19:35:25 2025

@author: ZXY-PC
"""

'''
文档功能：工具类或函数
'''

import os
import time
import magic
from collections import Counter
import shutil
import comtypes.client as ct #将doc转换为 docx 仅限windows系统
from sqlalchemy import create_engine,text
import pandas as pd
import sys

from logconfig.log_settings import log #Logger,current_file_path


'''
功能：用于根据value，在dict中找对应的key
参数：
    dict_obj   dict字典 
    value      字典值 
返回：
    key       字典值对应的字典键 
注：用于1:1对应文件    
'''
def find_key(dict_obj, value):
    for key, val in dict_obj.items():
        if val == value:
            return key
    return None


'''
功能: 输出生成的银行简历
参数： 
    empl_ID   员工ID
    name      员工名称
    bankName  银行名称
    opt_doc   填充后的银行模板docx文件
    BASE_DIR 项目所在路径
   数据简历默认存放在：BASE_DIR/sources/RESUME2BANKMODEL/bankName
'''
def bankResumeOutput(empl_ID, name, bankName, opt_doc, BASE_DIR):
    # 输出到文件夹    
    log.logger.info(empl_ID + '-' + name + bankName +'简历生成中... at ' + time.ctime(time.time()))
    output_dir = os.path.join(BASE_DIR, r'sources/RESUME2BANKMODEL')
    dir_nm = bankName #r'光大银行'
    output_path = os.path.join(output_dir, dir_nm)
    if os.path.isdir(output_path) == False:
        os.mkdir(output_path)
    # output_path = r'E:\\_02work\\_07_公司内部\\rsm2bank\\sources\\RESUME2BANKMODEL\\光大银行'
    output_file = os.path.join(output_path, r'{}.docx'.format(empl_ID + '-' + name + '-' + bankName))
    
    opt_doc.save(output_file)
    # print(empl_ID + '-' + name +' 光大银行简历转换完成')
    log.logger.info(empl_ID + '-' + name + bankName +'简历已生成 at ' + time.ctime(time.time()))
    log.logger.info('生成简历路径：' + output_file)
    return

'''
按915沟通逻辑
功能: 输出生成的银行简历 单份版
参数： 
    empl_ID   员工ID
    name      员工名称
    bankName  银行名称
    opt_doc   填充后的银行模板docx文件
    # BASE_DIR 项目所在路径
    outputDir #结果输入文件夹路径（包含：生成简历、其它输出）
'''
def bankResumeOutput_v1(    empl_ID   #员工ID
                            ,name      #员工名称
                            ,bankName  #银行名称
                            ,opt_doc   #填充后的银行模板docx文件
                            # ,BASE_DIR  #项目所在路径 数据简历默认存放在：BASE_DIR/sources\RESUME2BANKMODEL/bankName
                            ,outputDir #结果输入文件夹路径（包含：生成简历、其它输出）
                        ):
    # 输出到文件夹    
    log.logger.info(empl_ID + '-' + name + bankName +'简历生成中... at ' + time.ctime(time.time()))

    output_path = os.path.join(outputDir,f'生成简历/{bankName}/单份版')
    os.makedirs(output_path,exist_ok = True)
    
    # output_path = r'E:\\_02work\\_07_公司内部\\rsm2bank\\sources\\RESUME2BANKMODEL\\光大银行'
    output_file = os.path.join(output_path, r'{}.docx'.format(empl_ID + '-' + name + '-' + bankName))
    
    opt_doc.save(output_file)
    # print(empl_ID + '-' + name +' 光大银行简历转换完成')
    log.logger.info(empl_ID + '-' + name + bankName +'简历已生成 at ' + time.ctime(time.time()))
    log.logger.info('生成简历路径：' + output_file)
    return


'''
功能: 判断是否docx文件（通过文件头magic number）
参数： 
    file_path   文件绝对路径
返回:
    True or False
'''
def is_real_docx(file_path):
	with open(file_path,'rb') as f:
		header = f.read(4)	
	return header == b'PK\x03\x04' #docx文件前4个字节

'''
功能: 判断是否doc文件
参数： 
    file_path   文件绝对路径
返回:
    True or False
'''
def is_doc_file(file_path):
    with open(file_path, 'rb') as file:
        header = file.read(8)
    return header[:4] == b'\xD0\xCF\x11\xE0'         # 检查是否为DOC文件的签名


'''
功能: 检查文件类型 不能用 待检查
参数： 
    file_path   文件绝对路径
返回:
    文件类型
'''
def check_file_type(file_path):
    mime = magic.Magic(mime=True)
    file_type = mime.from_file(file_path)
    print(f"Actual file type: {file_type}")
    return file_type

'''
功能: 遍历文件夹目录找到所有文件，返回文件全路径list
参数： 
    dir_nm   文件夹目录
返回:
	file_abs_path_list #文件全路径list
	file_dir_list      #文件所在文件夹全路径list
	file_nm_list       #文件名list    
''' 
def search_dir_files(dir_nm):
	file_abs_path_list = [] #文件全路径
	file_dir_list = []      #文件所在文件夹全路径
	file_nm_list = []       #文件名
	for root, dirs, files in os.walk(dir_nm):
# 		print(root,dirs,files)
		for f in files:
			file_path_tmp = os.path.join(root, f)   #文件全路径
			file_abs_path_list.append(file_path_tmp)
			file_dir_list.append(root)
			file_nm_list.append(f)
	return file_abs_path_list,file_dir_list,file_nm_list
 
'''
功能: list中重复元素查找
参数： 
    lst   列表
返回:
	重复元素列表    
''' 
def find_duplicates(lst):
    count = Counter(lst)
    return [item for item, freq in count.items() if freq > 1]

'''
功能: 最高学历转换位最高学位
参数： 
    high_Edu   最高学历 
返回:
	high_Degree 最高学位    
''' 
def dilopma2degree(high_Edu):
    #学历_学位映射
    edu_degree_dict = { "本科":"学士",
                        "硕士":"硕士",
                        "博士":"博士"}
    high_Degree = '/'
    for kywd in edu_degree_dict.keys():
        if kywd in high_Edu:
            high_Degree = edu_degree_dict[kywd]            
    return high_Degree

'''
功能: 判断字符串（字段）是否在docx文档的表中
参数： 
    	docxTable, 	#docx表
    	r,	   		#row索引
    	c,			#cell索引
    	txt			#text 字段名
返回:
	Boolean    
''' 
def getPosCell(	docxTable, 	#docx表
				r,	   		#row索引
				c,			#cell索引
				txt			#text 字段名
				):
	cellTxt = docxTable.rows[r].cells[c].text.replace('\n','').replace(' ','')
	if cellTxt == txt:
		cellTxtNext = docxTable.rows[r].cells[c+1].text.replace('\n','').replace(' ','')
		if cellTxtNext != txt:
			return True
	return False

'''
功能: 复制文件到指定路径，不存在会自动创建路径
        复制解析异常简历 到 指定文件夹
参数： 
        file_path, #文件路径
        target_dir #目标文件夹路径
返回: None  
''' 
def cp_rsm2dir(file_path, #文件路径
               target_dir #目标文件夹路径
               ):
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(file_path,target_dir) #复制文件，包括元数据

'''
功能: doc转换为docx 仅限windows系统
参数： 
        file_path_doc, #doc文件全路径
        file_path_docx #输出docx文件全路径
返回: None  
''' 
def doc2docx(file_path_doc, #doc文件全路径
             file_path_docx #输出docx文件全路径
             ):
    word_doc = ct.CreateObject('Word.Application')
    word_doc.Visible = False  # 设置为False以隐藏Word界面
    doc = word_doc.Documents.Open(file_path_doc)
    doc.SaveAs2(file_path_docx, FileFormat=12)  # FileFormat=12代表Word 2007 XML格式
    doc.Close()
    word_doc.Quit()

'''
功能: 将文件夹目录中doc 文件替换为 docx，原doc文件移动到目标文件夹后删除
参数： 
        dir_nm, #文件目录(doc将被替换为docx)
        dir_doc #文件目录(被替换为docx的doc存放到该文件夹)  
返回: None  
''' 
def doc2docx_dir(dir_nm, #文件目录(doc将被替换为docx)，原doc文件删除
                 dir_doc #文件目录(被替换为docx的doc存放到该文件夹)                   
                 ):
    log.logger.info(f'ERP简历文件夹doc转换为docx开始（路径：{dir_nm}）...  at ' + time.ctime(time.time()))  
    #文件全路径 文件所在目录全路径 文件名
    file_abs_path_list,file_dir_list,file_nm_list = search_dir_files(dir_nm)
    for f in file_abs_path_list:
        if is_doc_file(f):
            file_path_docx = os.path.join(os.path.split(f)[0],os.path.split(f)[1].replace(r'.doc',r'.docx'))
            doc2docx(f, file_path_docx)
            cp_rsm2dir(f, dir_doc) # 原doc存放于dir_doc目录
            os.remove(f) #删除原doc
    log.logger.info(f'ERP简历文件夹doc转换为docx完成（原doc文件存放路径：{dir_doc}）...  at ' + time.ctime(time.time()))  


'''
功能: 找到docx表格一行中包含titles中字段的单元格开始和结束列索引
参数： 
         docx_tb   #docx表格
         r         #表格查找行号
         titles    #查找的字段list
返回: 
    col_bgn_list #开始索引dict
    col_end_list #结束索引dict 
'''
def find_pos_bgn_end_c( docx_tb   #docx表格
                       ,r         #表格查找行号
                       ,titles    #查找的字段list
                       ):
    col_bgn_list = {}  #经历模块 每个字段开始的列索引
    col_end_list = {}  #经历模块 每个字段结束的列索引
    for c, cell in enumerate(docx_tb.rows[r].cells):
        # print(c, cell.text)
        for w in titles: 
            if w in cell.text.replace(' ','').replace('\n',''):
                if w not in col_bgn_list.keys():
                    col_bgn_list[w] = c  #开始列号
                col_end_list[w] = c      #结束列号
    return col_bgn_list, col_end_list


'''
功能: 找到docx表格中包含titles中字段的单元格开始 结束行索引
参数： 
         docx_tb   #docx表格
         titles    #查找的字段list
返回: 
    row_bgn_list #开始索引dict
    row_end_list #结束索引dict 
'''
def find_pos_bgn_end_r( docx_tb   #docx表格
                       ,titles    #查找的字段list
                       ):
    row_bgn_list = {}  # 每个字段开始的行索引
    row_end_list = {}  # 每个字段开始的行索引
    for r, row in enumerate(docx_tb.rows):
        for c, cell in enumerate(row.cells):
            for w in titles: 
                if w == cell.text.replace(' ','').replace('\n',''):
                    if w not in row_bgn_list.keys():
                        row_bgn_list[w] = r  #开始行号
                    row_end_list[w] = r      #结束行号
    return row_bgn_list, row_end_list


'''
功能：将DataFrame分批导入临时数据表
参数：
    data_df     #需导入的DataFrame
    target_tb   #数据库临时表
    conn        #数据库连接
    batch_size  #每次导入数据量 默认 100
'''
def df2db(   data_df     #需导入的DataFrame
            ,target_tb   #数据库临时表
            ,targetDB    #数据库连接
            ,batch_size = 100  #每次导入数据量
         ): 
    #所有字段去除左右空格 换行符
    data_df = data_df.apply(lambda x: x.astype(str).str.strip())
    #批次数量     
    data_rows = data_df.shape[0]
    batch_size = int(batch_size)
    batch = data_rows // batch_size if data_rows % batch_size == 0 else data_rows // batch_size + 1
    
    log.logger.info(f'上传数据库表{target_tb}开始...  at ' + time.ctime(time.time()))  
    log.logger.info(f'数据条数: {data_rows}, 批次：{batch}，批量：{batch_size}  at ' + time.ctime(time.time()))  

    s = 0     
    e = batch_size
    for b in range(batch):
        log.logger.info(f'第{b+1}批次：[{s} : {e}]，数据条数: {data_rows}, 批次：{batch}，批量：{batch_size}  at' + time.ctime(time.time()))  
        try:
            data_df[s:e].to_sql(target_tb, con=targetDB.conn,if_exists='append',index=False) #replace append
        except Exception as err:
            log.logger.error(f'第{b+1}批次：[{s} : {e}]，报错：{err}  at' + time.ctime(time.time()))  
            sys.exit(1)
        s = e 
        e += batch_size    

###S4 接入数据库类 
class DataBase:
    def __init__(self,host,port,database,username,password):
        self.host = host
        self.port = port 
        self.database = database
        self.username = username
        self.password = password
        self.conn = self.getConn()  # mysql连接
        self.cursor = self.conn.connect()  # 游标
    def getConn(self):
        conn = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}')
        return conn
    def searchSql(self, sql):
        sql_tmp = ''
        try:
            connection = self.cursor
            sql_e = text(sql.strip())
            sql_tmp = sql_e
            log.logger.info(f'SQL: {sql_e}') 
            ans = connection.execute(sql_e)
            connection.commit()
            return ans.fetchall() #list
        except Exception as e:
            log.logger.error(f'查询sql报错：{e}. sql:{sql_tmp}  at' + time.ctime(time.time())) 
            sys.exit(1)
    def executeSql(self, sql):
        sql_tmp = ''
        try:
            connection = self.cursor
            sql_e = text(sql.strip())
            sql_tmp = sql_e
            log.logger.info(f'SQL: {sql_e}') 
            connection.execute(sql_e)
            connection.commit()
        except Exception as e:
            log.logger.error(f'执行sql报错：{e}. sql:{sql_tmp}  at' + time.ctime(time.time())) 
            sys.exit(1)    
    def executeManySql(self, sqltxt):
        sql_tmp = ''
        try:
            connection = self.cursor
            sql_list = sqltxt.split(';')
            sql_list_n_empty = [s.strip() for s in sql_list if s.strip() != '']
            for sql in sql_list_n_empty:
                sql_e = text(sql)
                sql_tmp = sql_e
                log.logger.info(f'SQL: {sql_e}')               
                connection.execute(sql_e)
            connection.commit()
        except Exception as e:
            log.logger.error(f'执行sql报错：{e}. sql:{sql_tmp}  at' + time.ctime(time.time())) 
            sys.exit(1)         
    def sql2DataFrame(self, sql):
        return pd.read_sql_query(sql, self.conn)
    def truncate_tb(self, tb):
        sql_tmp = ''
        try:
            connection = self.cursor
            sql_e = text(f'truncate table {tb};')
            sql_tmp = sql_e
            log.logger.info(f'SQL: {sql_e}') 
            connection.execute(sql_e)
            connection.commit()
        except Exception as e:
            log.logger.error(f'清空表sql报错：{e}. sql:{sql_tmp}  at' + time.ctime(time.time())) 
            sys.exit(1)                 
    def endDB(self):
        # 关闭链接
        self.cursor.close()

'''
功能：返回数据库执行sql
	当将临时表数据插入目标表时，将目标表重复数据删除
参数：
返回：执行sql语句
'''	
def sql_txt_del( rsm_date 	#简历收集日期 202506 表示2025上半年
				,tb_nm 		#要删除重复信息的表名
				,tb_nm_s 	#重复信息表
				):
    sql_txt_tmp = f'''
delete from {tb_nm} t1 /*ERP简历基本信息表*/
where	rsm_date = \'{rsm_date}\' /*'202506'  简历创建时间*/
	and exists (select 1
				from {tb_nm_s} t2 /*ERP简历基本信息临时表*/
				where t1.empl_ID  = t2.empl_ID
				) 
;
'''	
    return sql_txt_tmp


#ERP简历基本信息表 从临时表更新
def sql_txt_inst_bas_info( rsm_date 	#简历收集日期 202506 表示2025上半年
						 ):
	sql_txt_inst_bas_info_tmp = f'''
	/*将临时表数据插入目标表*/
insert into erp_resume_base_info /*ERP简历基本信息表*/
(
 empl_ID		/*'员工编号'*/
,name			/*'姓名'*/
,work_years	    /*'工作年限'*/
,grad_date		/*'毕业时间'*/
,grad_school	/*'毕业学校'*/
,major			/*'专业'*/
,high_Edu		/*'最高学历'*/
,high_Degree	/*'最高学位'*/
,department	    /*'所在部门'*/
,Jop_Title		/*'职称'*/
,Per_Profile	/*'个人简介'*/
,tech_skill	    /*'业务与技术能力详述'*/
,credential	    /*'资质认证'*/
,training		/*'参与培训'*/
,Skill_tags	    /*'技能标签'*/
,rsm_date		/*'简历收集日期 202506'*/
)
select
 empl_ID		/*'员工编号'*/
,name			/*'姓名'*/
,work_years	    /*'工作年限'*/
,grad_date		/*'毕业时间'*/
,grad_school	/*'毕业学校'*/
,major			/*'专业'*/
,high_Edu		/*'最高学历'*/
,high_Degree	/*'最高学位'*/
,department	    /*'所在部门'*/
,Jop_Title		/*'职称'*/
,Per_Profile	/*'个人简介'*/
,tech_skill	    /*'业务与技术能力详述'*/
,credential	    /*'资质认证'*/
,training		/*'参与培训'*/
,Skill_tags	    /*'技能标签'*/
,\'{rsm_date}\' /*'简历收集日期 202506'*/
from erp_resume_base_info_tmp t2 /*ERP简历基本信息临时表*/
;	
	'''
	return sql_txt_inst_bas_info_tmp


#ERP简历工作及项目经历表 从临时表更新
def sql_txt_inst_work_proj_exp( rsm_date 	#简历收集日期 202506 表示2025上半年
								):
	sql_txt_inst_work_proj_exp_tmp = f'''
	/*将临时表数据插入目标表*/
insert into erp_resume_work_proj_exp /*ERP简历工作及项目经历表*/
(
 empl_ID		/*'员工编号'*/
,name			/*'姓名'*/
,Wk_Prj_type	/*'工作/项目：1/2'*/
,start_date	    /*'开始时间'*/
,end_date		/*'结束时间'*/
,Comp_Prj_nm	/*'公司/项目名称'*/
,position		/*'担任职务/项目角色'*/
,Job_Desc		/*'工作/项目职责说明'*/
,rsm_date		/*'简历收集日期 202506'*/
)
select
 empl_ID		/*'员工编号'*/
,name			/*'姓名'*/
,Wk_Prj_type	/*'工作/项目：1/2'*/
,start_date	    /*'开始时间'*/
,end_date		/*'结束时间'*/
,Comp_Prj_nm	/*'公司/项目名称'*/
,position		/*'担任职务/项目角色'*/
,Job_Desc		/*'工作/项目职责说明'*/
,\'{rsm_date}\' /*'简历收集日期 202506'*/
from erp_resume_work_proj_exp_tmp t2 /*ERP简历工作及项目经历临时表*/
;	
	'''
	return sql_txt_inst_work_proj_exp_tmp


#ERP简历教育经历表 从临时表更新
def sql_txt_inst_edu_bg( rsm_date 	#简历收集日期 202506 表示2025上半年
						):
	sql_txt_inst_edu_bg_tmp = f'''
	/*将临时表数据插入目标表*/
insert into erp_resume_edu_bg /*ERP简历教育经历表*/
(
 empl_ID		/*'员工编号'*/
,grad_date		/*'毕业年月'*/
,grad_school	/*'毕业院校'*/
,major			/*'专业'*/
,remark		    /*'备注:多行学历信息'*/
,rsm_date		/*'简历收集日期 202506'*/

)
select
 empl_ID		/*'员工编号'*/
,grad_date		/*'毕业年月'*/
,grad_school	/*'毕业院校'*/
,major			/*'专业'*/
,remark		    /*'备注:多行学历信息'*/
,\'{rsm_date}\' /*'简历收集日期 202506'*/
from erp_resume_edu_bg_tmp t2 /*ERP简历教育经历临时表*/
;	
	'''
	return sql_txt_inst_edu_bg_tmp


'''
mysql 进程查看 及死锁后处理  
SELECT * FROM information_schema.innodb_trx;
KILL [trx_mysql_thread_id];   
'''