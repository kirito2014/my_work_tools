# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 17:48:05 2025

@author: ZXY-PC

按915沟通逻辑

功能：指定文件夹中docx简历解析为三张表，以DataFrame存储
        jl_df_tmp: 简历信息字段（不含：工作经历 项目经历） 1:1
        Project_Data_tmp: 简历信息字段（只含：工作经历 项目经历） 1:n
        edu_bg_split: 简历信息字段（只含教育经历）    1:n        
                      
        exception_empl_ID_df    教育经历提取异常员工编号df    

提供服务示例：
jl_df_tmp,Project_Data_tmp = docxResume2DF.docx_data( docxResumeDir    #简历所在文件夹
                                            ,dir_doc          #文件目录(被替换为docx的doc存放到该文件夹)
                                            ,target_dir       #未解析的格式有问题简历存放文件夹   
                                            )

edu_bg_split,exception_empl_ID_df = docxResume2DF.edu_bg_2_DF( jl_df_tmp      #基本信息df
                                                              )
col_mapping
Project_col_mapping
col_mapping_edu_bg
"""

import os 
# import sys
import re
# os.path.abspath('.')
# sys.version
# os.system('version')

from docx import Document
import pandas as pd
# from docx.oxml.ns import qn
# from docx.shared import Pt,RGBColor
# from docx.enum.text import WD_ALIGN_PARAGRAPH
# import sqlite3
# import numpy as np
# import pymysql
#import schedule
import time
# import datetime
# from sqlalchemy import create_engine,text,DDL

# 尝试导入日志模块，失败则使用默认日志
import logging
import sys

# 配置默认日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

try:
    from logconfig.log_settings import log
    print("✓ 使用自定义日志配置")
except ImportError:
    print("✗ 自定义日志配置导入失败，使用模拟日志对象")
    # 模拟日志对象，确保 log.logger 可用
    class MockLog:
        def __init__(self):
            self.logger = logging.getLogger(__name__)
    log = MockLog()

# 尝试导入 core.tools 模块，失败则模拟所需函数
try:
    from core.tools import is_real_docx,cp_rsm2dir,getPosCell,dilopma2degree,find_pos_bgn_end_r,find_pos_bgn_end_c,search_dir_files,find_duplicates,is_doc_file
    print("✓ 使用真实 core.tools 模块")
except ImportError:
    print("✗ core.tools 模块导入失败，使用模拟函数")
    
    # 模拟所需函数
    def is_real_docx(file_path):
        """模拟 is_real_docx 函数"""
        return file_path.lower().endswith('.docx')
    
    def cp_rsm2dir(src, dst):
        """模拟 cp_rsm2dir 函数"""
        import shutil
        shutil.copy(src, dst)
    
    def getPosCell(table, r, c, text):
        """模拟 getPosCell 函数"""
        return text in table.cell(r, c).text
    
    def dilopma2degree(edu):
        """模拟 dilopma2degree 函数"""
        degree_map = {
            '博士': '博士学位',
            '硕士': '硕士学位',
            '本科': '学士学位',
            '专科': '专科',
            '大专': '专科'
        }
        for key, value in degree_map.items():
            if key in edu:
                return value
        return edu
    
    def find_pos_bgn_end_r(table, titles):
        """模拟 find_pos_bgn_end_r 函数"""
        row_bgn = {}
        row_end = {}
        for r, row in enumerate(table.rows):
            for cell in row.cells:
                for title in titles:
                    if title in cell.text:
                        row_bgn[title] = r
        # 简单处理，假设每个模块的结束是下一个模块的开始
        sorted_titles = sorted(row_bgn.keys(), key=lambda x: row_bgn[x])
        for i, title in enumerate(sorted_titles):
            if i < len(sorted_titles) - 1:
                row_end[title] = row_bgn[sorted_titles[i+1]]
            else:
                row_end[title] = len(table.rows)
        return row_bgn, row_end
    
    def find_pos_bgn_end_c(table, row_idx, titles):
        """模拟 find_pos_bgn_end_c 函数"""
        col_bgn = {}
        col_end = {}
        if row_idx < len(table.rows):
            row = table.rows[row_idx]
            for c, cell in enumerate(row.cells):
                for title in titles:
                    if title in cell.text:
                        col_bgn[title] = c
        # 简单处理，假设每个列的结束是下一个列的开始
        sorted_titles = sorted(col_bgn.keys(), key=lambda x: col_bgn[x])
        for i, title in enumerate(sorted_titles):
            if i < len(sorted_titles) - 1:
                col_end[title] = col_bgn[sorted_titles[i+1]]
            else:
                col_end[title] = len(row.cells)
        return col_bgn, col_end
    
    def search_dir_files(dir_path):
        """模拟 search_dir_files 函数"""
        import os
        file_abs_path_list = []
        file_dir_list = []
        file_nm_list = []
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                file_abs_path_list.append(file_path)
                file_dir_list.append(root)
                file_nm_list.append(file)
        return file_abs_path_list, file_dir_list, file_nm_list
    
    def find_duplicates(lst):
        """模拟 find_duplicates 函数"""
        seen = set()
        duplicates = set()
        for item in lst:
            if item in seen:
                duplicates.add(item)
            seen.add(item)
        return list(duplicates)
    
    def is_doc_file(file_path):
        """模拟 is_doc_file 函数"""
        return file_path.lower().endswith('.doc')
# from core.tools import doc2docx_dir
# print(current_file_path)


'''
简历解析函数
功能: 将公司简历格式按字段提取成两个dataFrame，仅解析docx格式简历
参数说明：
    file_path: 简历路径
    filename: 简历文件名 
    jl_df_tmp: 简历信息字段（不含：工作经历 项目经历）
    Project_Data_tmp: 简历信息字段（只含：工作经历 项目经历）
    target_dir: 未解析的简历存放文件夹
返回：
    jl_df_tmp: 简历信息字段（不含：工作经历 项目经历）
    Project_Data_tmp: 简历信息字段（只含：工作经历 项目经历）
    exception_flag:     异常标识位（0 正常, 1 异常）
'''
def Resume_Data2table( file_path        #待解析简历绝对路径
                      ,filename         #待解析简历文件名
                      ,jl_df_tmp        #简历信息字段（不含：工作经历 项目经历）
                      ,Project_Data_tmp #简历信息字段（只含：工作经历 项目经历）    
                      ,target_dir       #未解析的格式有问题简历存放文件夹
                      ):
    #test 辅助
    # file_path = r'E:\_02work\_07_公司内部\rsm2bank\sources\RESUME_RESOV\2025年上半年ERP简历收集_bak\泛金融业务部\泛金融业务1A部\23749+张立松+工作简历.docx'   
    # filename = r'23749+张立松+工作简历.docx'      

    # 员工编号
    empl_ID = str(filename.split('+')[0])
    
    try: #格式正常解析 否则记录错误日志后继续解析 异常简历复制到target_dir
        # 不是docx格式简历 跳过，将简历复制到target_dir
        if not is_real_docx(file_path): #不是docx格式简历
            log.logger.info('--不是docx文件: ' + file_path + '  at' + time.ctime(time.time()))       
        
        # docx文档
        ori_doc = Document(file_path)
        # 解析公司简历
        ori_tables = ori_doc.tables
        
        # 简历为空 跳过，将简历复制到target_dir
        if len(ori_tables) < 1: #简历数据为空
            log.logger.info('--docx表为空: ' + file_path + '  at' + time.ctime(time.time())) 
    
            #1. 基本信息提取  
        name        = '/' #姓名
        work_years  = '/' #工作年限
        grad_date   = '/' #毕业时间
        grad_school = '/' #毕业学校
        major       = '/' #专业
        high_Edu    = '/' #最高学历
        high_Degree = '/' #最高学位  由最高学历转换
        department  = '/' #所在部门
        Jop_Title   = '/' #职称
        Per_Profile = '/' #个人简介
        tech_skill  = '/' #业务与技术能力
        credential  = '/' #资质认证
        training    = '/' #参与培训
        Skill_tags  = '/' #技能标签
    
        for r, row in enumerate(ori_tables[0].rows):
            for c, cell in enumerate(row.cells):
                # print(r,c)
                # print(cell.text)
                if getPosCell(ori_tables[0],r,c,'姓名'				) : name		= ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'工作年限'  			) : work_years  = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'毕业时间'  			) : grad_date   = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'毕业学校'  			) : grad_school = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'专业'      			) : major       = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'最高学历'  			) : high_Edu    = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'所在部门'  			) : department  = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'职称'      			) : Jop_Title   = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'个人简介'  			) : Per_Profile = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'业务与技术能力'		) : tech_skill  = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'业务与技术能力详述'	) : tech_skill  = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'资质认证'			) : credential  = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'参与培训'			) : training    = ori_tables[0].rows[r].cells[c+1].text
                if getPosCell(ori_tables[0],r,c,'技能标签'			) : Skill_tags  = ori_tables[0].rows[r].cells[c+1].text
        
        high_Degree = dilopma2degree(high_Edu)
        
        if jl_df_tmp[jl_df_tmp['empl_ID'].isin([empl_ID])].shape[0]>0:
            log.logger.info(empl_ID + '-' + name + ' 简历数据开始更新 at' + time.ctime(time.time()))
        else:
            log.logger.info(empl_ID + '-' + name + ' 简历数据开始新增 at' + time.ctime(time.time()))
            #   (1) 删除已存在的empl_ID
        jl_df_tmp = jl_df_tmp.drop(jl_df_tmp[jl_df_tmp['empl_ID'].isin([empl_ID])].index)
            #   (2) 进行新增
        new_data = [[empl_ID, name, work_years, grad_date, grad_school, major
                     , high_Edu, high_Degree,department, Jop_Title, Per_Profile
                     , tech_skill, credential, training, Skill_tags
                     ]]   
        new_row = pd.DataFrame(new_data, columns=jl_df_tmp.columns)
        jl_df_tmp = pd.concat([jl_df_tmp,new_row], ignore_index=True)
        log.logger.info('--' + empl_ID + '-' + name + ' 基本信息解析完成 at' + time.ctime(time.time()))
        
        #2 工作/项目经历数据提取
        # （1）先对存在的empl_ID进行数据删除 
        Project_Data_tmp = Project_Data_tmp.drop(Project_Data_tmp[Project_Data_tmp['empl_ID'].isin([empl_ID])].index)
        #  (2)进行新增 
            #模块开始行号
        titles_module = ['工作经历（由近至远）','项目经历（由近至远）','工作经历','项目经历','能力与资质']
        row_bgn_list, row_end_list = find_pos_bgn_end_r( ori_tables[0]   #docx表格
                                                        ,titles_module    #查找的字段list
                                                        )
        wk_index = None
        for wk in ['工作经历（由近至远）','工作经历']:
            if row_bgn_list.get(wk) is not None:
                wk_index    = row_bgn_list.get(wk)
                break
        prj_index = None
        for prj in ['项目经历（由近至远）','项目经历']:
            if row_bgn_list.get(prj) is not None:
                prj_index    = row_bgn_list.get(prj) 
                break
        tech_index  = row_bgn_list.get('能力与资质')
        
        if tech_index is None:
            log.logger.info('--能力与资质模块表头未找到，' + filename + ' at' + time.ctime(time.time()))
        
        if prj_index is None:
            prj_index = tech_index
        
        # 工作/项目经历表头字段开始 结束列号
        wk_col = ['开始时间','结束时间','公司名称','担任职务','职责']
        prj_col = ['开始时间','结束时间','项目名称','项目角色','职责']    
        #工作经历模块 每个字段开始 结束列索引
        wk_col_bgn_list, wk_col_end_list = find_pos_bgn_end_c( ori_tables[0]   #docx表格
                                                              ,wk_index+1         #表格查找行号
                                                              ,wk_col    #查找的字段list
                                                              )
        for w in wk_col:
            if wk_col_bgn_list.get(w) is None:
                log.logger.info('--工作经历模块表头：'+ w + '未找到，'+ filename + ' at' + time.ctime(time.time()))
        
        #项目经历模块 每个字段开始 结束列索引
        prj_col_bgn_list, prj_col_end_list = find_pos_bgn_end_c( ori_tables[0]   #docx表格
                                                                ,prj_index+1         #表格查找行号
                                                                ,prj_col    #查找的字段list
                                                                )    
        for p in prj_col:
            if prj_col_bgn_list.get(p) is None:
                log.logger.info('--项目经历模块表头：'+ p + '未找到，'+ filename + ' at' + time.ctime(time.time()))
        
        # 工作/项目经历
        for s in range(wk_index+2,tech_index):
            Wk_Prj_type = '0'
            if s<prj_index:
                Wk_Prj_type = '1'
            elif s>=prj_index+2:
                Wk_Prj_type = '2'
            if Wk_Prj_type in ('1','2'):
                col_start_date  = wk_col_bgn_list['开始时间'] if Wk_Prj_type == '1' else prj_col_bgn_list['开始时间']
                col_end_date    = wk_col_bgn_list['结束时间'] if Wk_Prj_type == '1' else prj_col_bgn_list['结束时间']
                col_Comp_Prj_nm = wk_col_bgn_list['公司名称'] if Wk_Prj_type == '1' else prj_col_bgn_list['项目名称']
                col_position    = wk_col_bgn_list['担任职务'] if Wk_Prj_type == '1' else prj_col_bgn_list['项目角色']
                col_Job_Desc    = wk_col_bgn_list['职责'] if Wk_Prj_type == '1' else prj_col_bgn_list['职责']
                start_date  = ori_tables[0].rows[s].cells[col_start_date ].text
                end_date    = ori_tables[0].rows[s].cells[col_end_date   ].text
                Comp_Prj_nm = ori_tables[0].rows[s].cells[col_Comp_Prj_nm].text
                position    = ori_tables[0].rows[s].cells[col_position   ].text
                Job_Desc    = ori_tables[0].rows[s].cells[col_Job_Desc   ].text         
                new_data = [[empl_ID, name, Wk_Prj_type, start_date, end_date, Comp_Prj_nm, position, Job_Desc]]
                new_row = pd.DataFrame(new_data, columns=Project_Data_tmp.columns)
                Project_Data_tmp = pd.concat([Project_Data_tmp,new_row], ignore_index=True)
        log.logger.info('--' + empl_ID + '-' + name + ' 工作/项目经历信息解析完成 at' + time.ctime(time.time()))
        log.logger.info('--' + empl_ID + '-' + name + ' 简历解析完成 at' + time.ctime(time.time()))
        return jl_df_tmp,Project_Data_tmp,'0'

    except Exception as e:
        log.logger.error(f'{filename}简历格式异常错误：{e} at' + time.ctime(time.time()))
        cp_rsm2dir(file_path, target_dir) #异常复制到指定文件夹后 继续
        return jl_df_tmp,Project_Data_tmp,'1'

###word简历解析后字段映射 与模板中一致
# 基础信息jl_df_tmp
col_mapping = {'empl_ID': '员工编号',
    'name':			'姓名',
    'work_years':	'工作年限',
    'grad_date':	'毕业时间',
    'grad_school':	'毕业学校',
    'major':		'专业',
    'high_Edu':		'最高学历',
    'high_Degree': 	'最高学位',
    'department':	'所在部门',
    'Jop_Title':	'职称',
    'Per_Profile':	'个人简介',
    'tech_skill':	'业务与技术能力详述',
    'credential':	'资质认证',
    'training':		'参与培训',
    'Skill_tags':	'技能标签'}
jl_df_column = col_mapping.keys()

# 工作/项目经历表Project_Data_tmp
Project_col_mapping = {'empl_ID': '员工编号',
    'name':			'姓名',
    'Wk_Prj_type':	'工作/项目：1/2',
    'start_date':	'开始时间',
    'end_date':	    '结束时间',
    'Comp_Prj_nm':	'公司/项目名称',
    'position':		'担任职务/项目角色',
    'Job_Desc': 	'工作/项目职责说明'}
Project_Data_column = Project_col_mapping.keys()

   ###教育背景 模板映射 
col_mapping_edu_bg = {'empl_ID': '员工编号',
    'grad_date':	'毕业年月',
    'grad_school':	'毕业院校',
    'major':		'专业',
    'remark':	'备注:多行学历信息'}

### S3 遍历文件夹中简历，解析数据落表
'''
功能: 遍历根据名单选择的ERP简历，解析数据为 基础信息数据 项目及工作经历数据
参数说明：
    docxResumeDir word简历所在文件夹
返回：
    jl_df           基础信息数据
    Project_Data    项目及工作经历数据 
    rsm_list        #名单信息获取情况(erp简历，技术人员信息，简历解析情况)
'''
def docx_data_from_rsm_list( ERP_rsm_path_list   #名单中可获取的ERP简历绝对路径list
                          ,rsm_list            #名单信息获取情况(erp简历，技术人员信息)
                          ,target_dir          #未解析的格式有问题简历存放文件夹   
                          ):
    #1. 遍历list中简历，解析数据落表 基本信息数据 项目及工作经历数据
    log.logger.info('解析根据交付人员名单获取ERP简历数据开始... at ' + time.ctime(time.time()))
        #创建空基本信息 工作/项目经历 教育经历 临时表
    jl_df_tmp = pd.DataFrame(columns=jl_df_column)
    Project_Data_tmp = pd.DataFrame(columns=Project_Data_column)       
        #简历内容提取
    exception_rsm_list = list() #解析异常简历列表
    k = 0    
    for f in ERP_rsm_path_list:
        k += 1 
        log.logger.info(f'第{k}份简历信息开始提取：{f} at' + time.ctime(time.time()))
        filename = os.path.split(f)[1]
        if '工作简历' in filename and '~$' not in filename: #'~$' 临时文件标识符
            log.logger.info(f'{filename} 简历信息提取中（第{k}份）... at' + time.ctime(time.time()))
            if os.path.isfile(f):
                jl_df_tmp,Project_Data_tmp,exception_flag = Resume_Data2table( f        #待解析简历绝对路径
                                                                              ,filename         #待解析简历文件名
                                                                              ,jl_df_tmp        #简历信息字段（不含：工作经历 项目经历）
                                                                              ,Project_Data_tmp #简历信息字段（只含：工作经历 项目经历）    
                                                                              ,target_dir       #未解析的格式有问题简历存放文件夹
                                                                              )
                #解析异常简历添加到
                if exception_flag == '1': exception_rsm_list.append(f)
    # log.logger.info(f'交付人员名单获取ERP简历基本信息：\n{jl_df_tmp}')
    # log.logger.info(f'交付人员名单获取ERP简历工作及项目经历：\n{Project_Data_tmp}')
    #2. 交付名单获取ERP简历解析情况
    exception_rsm_empl_ID = [os.path.split(f)[1].split(r'+')[0].strip() for f in exception_rsm_list]
    # log.logger.info(f'exception_rsm_empl_ID: \n{exception_rsm_empl_ID}')    
    # rsm_list['isGetErpRsmParseData'] = ~rsm_list.empl_ID.isin(exception_rsm_empl_ID)
    rsm_list['tmp'] = ~rsm_list.empl_ID.isin(exception_rsm_empl_ID)
    rsm_list['isGetErpRsmParseData'] = rsm_list.apply(lambda row: True if (row['tmp'] == True and row['isFindERPrsm'] == True) else False, axis=1)
    rsm_list = rsm_list.drop(['tmp'],axis=1)
    log.logger.info(f'ERP简历解析情况：\n{rsm_list}')

        #数量统计
    rsm_list_cnt = rsm_list.shape[0] #交付人员数量
    erp_rsm_cnt = len(ERP_rsm_path_list) #获取erp简历数量 
    erp_rsm_parse_e_cnt = len(exception_rsm_list) #erp简历解析异常数量 
    erp_rsm_parse_cnt = erp_rsm_cnt - erp_rsm_parse_e_cnt #erp简历正常解析数量
    log.logger.info(f'解析根据交付人员名单获取的ERP简历情况：交付名单人员{rsm_list_cnt}个，获取ERP简历{erp_rsm_cnt}份，其中解析正常{erp_rsm_parse_cnt}份，解析异常{erp_rsm_parse_e_cnt}份。at ' + time.ctime(time.time()))
    log.logger.info(f'解析异常简历（存放位置：{target_dir}）：\n{pd.DataFrame(exception_rsm_list,columns=['exception_rsm']) }')
    log.logger.info('解析根据交付人员名单获取ERP简历数据完成。 at ' + time.ctime(time.time()))
    return jl_df_tmp,Project_Data_tmp,rsm_list


'''
功能: 日志ERP文件夹简历基本情况说明
参数说明：
    srcErpRsmDIR #文件目录(doc将被替换为docx)，原doc文件删除
返回：None    
'''
def erpRsmOv(srcErpRsmDIR #文件目录(doc将被替换为docx)，原doc文件删除
             ):
        #遍历ERP简历目录 获取所有简历绝对路径list，简历情况说明
    file_abs_path_list,file_dir_list,file_nm_list = search_dir_files(srcErpRsmDIR)
    #简历数量
    rsm_cnt = len(file_abs_path_list)
    #名称重复简历list
    duplicate_ele_list = find_duplicates(file_nm_list)
    rsm_dup = len(duplicate_ele_list)

    log.logger.info(f'ERP简历情况：共{rsm_cnt}份简历，其中{rsm_dup}份重复，重复简历名称：')    
    n = 0
    for f in duplicate_ele_list:
        n += 1
        print(f)
        log.logger.info(f'重复简历(第{n}份)：{f}')
    #简历格式统计
    not_docx = [f for f in file_abs_path_list if is_real_docx(f) == False]
    is_docx =  [f for f in file_abs_path_list if is_real_docx(f) == True]
    is_doc =  [f for f in file_abs_path_list if is_doc_file(f) == True]
    docx_n_cnt = len(not_docx)
    docx_cnt = len(is_docx)
    doc_cnt = len(is_doc)
    log.logger.info(f'共{rsm_cnt}份简历，其中docx文档{docx_cnt}份，非docx文档{docx_n_cnt}份，doc文档{doc_cnt}份,非docx文档名称：')
    m = 0
    for f in not_docx:
        m += 1
        print(f)
        log.logger.info(f'非docx文档(第{m}份)：{f}')
    return


'''
功能: 从基本信息表提取教育经历信息，行拆分多行落表edu_bg_split: 教育经历
参数说明：
    jl_df_tmp      #基本信息df
    rsm_list       #名单信息获取情况(erp简历，技术人员信息，简历解析情况)
返回：
    edu_bg_split            教育经历数据df
    exception_empl_ID_df    异常员工编号df  
    rsm_list                名单信息获取情况(erp简历，技术人员信息，简历解析，教育经历解析情况)
'''
def edu_bg_2_DF( jl_df_tmp      #基本信息df
                ,rsm_list        #名单信息获取情况(erp简历，技术人员信息，简历解析情况)
                ):
    log.logger.info('word简历教育经历开始解析 at' + time.ctime(time.time()))
    
    #创建空基本信息 工作/项目经历 教育经历 临时表
    edu_bg_split = pd.DataFrame(columns=col_mapping_edu_bg)
    
    col_selected_edu = ['empl_ID', 'grad_date', 'grad_school', 'major']
    edu_bg = jl_df_tmp[col_selected_edu]

        #将学历相关字段 转为 list
    edu_bg['grad_date']		=edu_bg['grad_date'		].apply(lambda x: x.replace('最高学历，','最高学历:').replace('第一学历，','第一学历:'))
    edu_bg['grad_school']	=edu_bg['grad_school'	].apply(lambda x: x.replace('最高学历，','最高学历:').replace('第一学历，','第一学历:'))
    edu_bg['major']			=edu_bg['major'			].apply(lambda x: x.replace('最高学历，','最高学历:').replace('第一学历，','第一学历:'))   
        
    edu_bg['grad_date_list'] 	= edu_bg['grad_date'].apply(lambda x: sorted(x.split('\n')))
    edu_bg['grad_school_list'] 	= edu_bg['grad_school'].apply(lambda x: sorted(x.split('\n')))
    edu_bg['major_list'] 		= edu_bg['major'].apply(lambda x: sorted(x.split('\n')))   
    
    rn = 0	#行号
    exception_empl_ID = list() #转换异常的empl_ID
    for r in range(0, edu_bg.shape[0]):
        tmp_empl_ID = edu_bg.loc[r,'empl_ID'		]
        log.logger.info(f'{tmp_empl_ID} 教育经历提取开始（第{r}个）... at' + time.ctime(time.time()))
        try: #正常格式解析
            # 将list中空格 或 空字符剔除
            tmp_grad_date_list 		= list(filter(lambda x: x.replace(' ','').replace('\n','') != '', edu_bg.loc[r,'grad_date_list'		]))
            tmp_grad_school_list 	= list(filter(lambda x: x.replace(' ','').replace('\n','') != '', edu_bg.loc[r,'grad_school_list'	]))
            tmp_major_list 			= list(filter(lambda x: x.replace(' ','').replace('\n','') != '', edu_bg.loc[r,'major_list'	    	]))        
            #毕业日期为空跳转下一条
            if len(tmp_grad_date_list)==0 or tmp_grad_date_list is None:   
                continue 
            if len(tmp_grad_date_list)==1:
                print('---1 单条')
                edu_bg_split.loc[rn,'empl_ID'    ] = tmp_empl_ID
                edu_bg_split.loc[rn,'grad_date'  ] = tmp_grad_date_list[0]
                edu_bg_split.loc[rn,'grad_school'] = tmp_grad_school_list[0]
                edu_bg_split.loc[rn,'major'      ] = tmp_major_list[0]
                edu_bg_split.loc[rn,'remark'     ] = '' #备注：学历类型
                rn += 1 
            else:
                print('---2 多条')        
                for s in range(0, len(tmp_grad_date_list)):
                          
                    edu_bg_split.loc[rn,'empl_ID'    ] = tmp_empl_ID
        
                    if re.match(r'最高学历[\d]{4}',tmp_grad_date_list[s]): #多条没有分隔符，添加:分隔符
                        tmp_grad_date_list_s = tmp_grad_date_list[s].replace('最高学历','最高学历:')
                        edu_bg_split.loc[rn,'grad_date'  ] = re.split(r'[:,：]', tmp_grad_date_list_s)[1].strip()            
                    else:    
                        edu_bg_split.loc[rn,'grad_date'  ] = re.split(r'[:,：]', tmp_grad_date_list[s])[1].strip()
                    
                    if len(tmp_grad_school_list) ==1:
                        edu_bg_split.loc[rn,'grad_school'] = tmp_grad_school_list[0]
                    else:
                        edu_bg_split.loc[rn,'grad_school'] = re.split(r'[:,：]', tmp_grad_school_list[s])[1].strip()
                               
                    if re.match(r'专/本科：计算机信息管理',tmp_major_list[0]):
                        tmp_major_list.remove(r'专/本科：计算机信息管理')
                        tmp_major_list.append('专科：计算机信息管理')
                        tmp_major_list.append('本科：计算机信息管理')
                        tmp_major_list.sort()
                    if len(tmp_major_list) <= 1:
                        edu_bg_split.loc[rn,'major'      ] = tmp_major_list[0].strip()            
                    else:
                        edu_bg_split.loc[rn,'major'      ] = re.split(r'[:,：]', tmp_major_list[s])[1].strip()
                    edu_bg_split.loc[rn,'remark'     ] = re.split(r'[:,：]', tmp_grad_date_list[s])[0].strip() if len(tmp_grad_date_list)>1 else '' #备注：学历类型
                    rn += 1 
            log.logger.info(f'{tmp_empl_ID} 教育经历提取完成（第{r}个）. at' + time.ctime(time.time()))
        except Exception as e:
            log.logger.error(f'{tmp_empl_ID} 教育经历异常错误（第{r}个）：{e} at' + time.ctime(time.time()))
            exception_empl_ID.append(tmp_empl_ID)
            continue

    # log.logger.info(f'交付人员名单获取ERP简历教育经历：\n{edu_bg_split}')

    #打印提取异常的教育经历
    emp_ID_cnt = edu_bg.shape[0] #emp_ID条数
    except_cnt = len(exception_empl_ID)
    log.logger.info(f'共提取{emp_ID_cnt}个员工，提取教育经历{rn}条，其中{except_cnt}个员工提取异常，异常员工编号： at' + time.ctime(time.time()))
    k = 0
    for id in exception_empl_ID:
        print(id)
        k += 1
        log.logger.info(f'异常员工编号：{id}，第{k}个， at' + time.ctime(time.time()))
    # exception_empl_ID_df = pd.DataFrame(exception_empl_ID,columns=['empl_ID'])
        #简历解析情况
    rsm_list['tmp'] = ~rsm_list.empl_ID.isin(exception_empl_ID)
    rsm_list['isGetErpRsmParseDataEdu'] = rsm_list.apply(lambda row: True if (row['tmp'] == True and row['isGetErpRsmParseData'] == True) else False, axis=1)
    rsm_list = rsm_list.drop(['tmp'],axis=1)
    log.logger.info(f'ERP简历教育信息解析情况：\n{rsm_list}')
    
    #排序
    edu_bg_split = edu_bg_split.sort_values(by=['empl_ID','grad_date'],ascending=[True,False])
    log.logger.info('word简历教育经历解析完成 at' + time.ctime(time.time()))
    return edu_bg_split,rsm_list
