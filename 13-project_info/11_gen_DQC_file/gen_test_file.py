"""
文件名(File Name)               :gen_test_file.py
作者(Author)                    :wangmujun@Sunline
编写时间(CreateTime)            :2024-05-01
版本号(Version)                 :V1.2.1
使用方法(Usage)                 :python gen_test_file.py <元数据文件> <模板文件> <生成人员/版本信息>
功能描述(Descriptions)          :
    本脚本主要用于实现以下功能:
    1、根据通用SDM模板生成DQC质量检核平台的导入模板
    2、
依赖库(Dependences):
    - pandas >= 2.2.3
    - xlwings >= 0.33.4
修改历史(Histories):
    v1.0.0 - 2024-05-01 - 初始版本
    v1.1.0 - 2024-11-03 - 修复生成后缺失部分文件的错误，补充了检查文件合法性的代码语句
    v1.2.0 - 2025-02-05 - 修改生成模板，根据质量平台最新模板调整生成语句及插入字段的顺序,调整代码检查语句增加主键PK
    v1.2.1 - 2025-02-08 - 优化打印输出格式，优化首行判断合法性
"""


#以下是代码正文

import sys
import os 
import pandas as pd
import xlwings as xw
import shutil

def get_max_row(sheet):
    """获取最大行"""
    max_row = sheet.max_row
    for row in range(max_row,0,-1):
        for cell in sheet[row]:
            if cell.value is not None:
                return row
    return 0

def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False
def check_vaild_excel(sheet):
    """检查最大行是否一致如果一致则返回True，不一致则返回False"""
    max_row_a=sheet.range('A1').expand('down').last_cell.row
    max_row_i=sheet.range('I1').expand('down').last_cell.row
    if max_row_a and max_row_i and max_row_i != max_row_a:
        print(f"[ ERROR ] {sheet.name} 首列非空校验不通过.\n")
        return False
    elif max_row_a and max_row_i and max_row_i == max_row_a:
        print(f"[ INFO ] {sheet.name} 首列非空校验通过.\n")
        if sheet.name == 'rem-代码映射':
            if sheet.range('A1').value != 'SRC_TAB_LIB_NAME' or sheet.range('A1').value == 'None':
                print(f"[ ERROR ] {sheet.name} 首列内容校验不通过，确认是否错行缺失.\n")
                return False
        return True

def copy_excel_file(template_file,person_name):
    """复制模板文件为目标文件"""
    print('----------------------------------------------------------------\n')
    if not os.path.isfile(template_file):
        print(f"[ ERROR ] 模板文件不存在！！")
        return
    new_file_name = f"开发_{person_name}.xls"
    if os.path.isfile(new_file_name):
        print(f"[ INFO  ] 目标文件< {new_file_name} >已存在,删除旧文件.\n")
        os.remove(new_file_name)
    shutil.copyfile(template_file,new_file_name)
    print(f"[ INFO  ] 目标文件< {new_file_name} >已创建.\n")
    return new_file_name
def update_progress(progress):
    print(f"执行进度:当前已完成{progress}% .",end="\r")

def copy_sheets_and_metadata(source_file,template_file):
    """总程序处理DQC文件生成"""
    #初始化错误列表
    error_list = []
    table_name_list = []
    table_list = []
    code_columns_list = []
    code_columns_cn_list = []

    #应用显示用于调试,关闭屏幕刷新
    excel_app = xw.App(visible=False)
    excel_app.screen_updating = False
    
    try:
        #print(source_file)
        src_wb = excel_app.books.open(source_file) 
        #检查源文件的代码映射和数据字典是否存在，代码映射检查首列是否存在空格(首列空格输出的数据不全)
        rem_data_dict_sheet = 'rem-数据字典'
        if rem_data_dict_sheet not in [sheet.name for sheet in src_wb.sheets]:
            print(f"[ ERROR ] 源SDM文件未找到数据字典sheet页.\n")
            sys.exit()
        else:
            src_dd_sheet = src_wb.sheets(rem_data_dict_sheet)
            clear_filters(src_dd_sheet)
            if not check_vaild_excel(src_dd_sheet):
                print(f"[ ERROR ] 数据字典 中首列存在空格，输出代码会导致缺失请检查.\n")
                sys.exit()

        rem_code_map_sheet = 'rem-代码映射'
        if rem_code_map_sheet not in [sheet.name for sheet in src_wb.sheets]:
            print(f"[ ERROR ] 处理源SDM文件代码映射sheet页，忽略处理代码映射检核语句的生成.\n")
            sys.exit()
        else:
            src_cm_sheet = src_wb.sheets(rem_code_map_sheet)
            #去除筛选条件
            clear_filters(src_cm_sheet)
            #检查代码映射是否有效
            if not check_vaild_excel(src_cm_sheet):
                print(f"[ ERROR ] 代码映射 中首列存在空格，输出代码会导致缺失请检查.\n")
                sys.exit()

        #检查模板文件对应sheet页是否存在
        if os.path.exists(template_file):
            tgt_wb = excel_app.books.open(template_file)
            tgt_sheet = tgt_wb.sheets['模板字段']
        else:
            tgt_wb = xw.Book()
            tgt_sheet = tgt_wb.sheets['模板字段']
            tgt_wb.save(template_file)


        #删除目标文件已存在的数据
        if tgt_sheet.range('A3').value is not None:
            max_row = tgt_sheet.range('A1').expand('down').last_cell.row
            tgt_sheet.range(f'A3:AB{max_row}').clear_contents()

        #提取来源中重要信息
        index_data = pd.read_excel(source_file, sheet_name = 'index', usecols="C,D,E,M,N,P")
        index_data = index_data.iloc[1:] #从第2行开始
        index_data.columns = ['table_id','table_name','table_ch_name','enable_flag','template_flag','info_pk_list'] 
        index_data['table_id'] = index_data['table_id'].apply(lambda x:x.split("'")[1] if "'" in x else x)
        filtered_index = index_data[index_data['enable_flag'] == 'Y']

        y_count = filtered_index.shape[0]
        total_files = y_count
        print(f"-==========- 检测到 {total_files} 张表 -==========-\n")
        processed_files = 0

        table_list = [f"{row['table_name']} \n" for i,row in filtered_index.iterrows()]

        data_count_query = ''
        pk_query = ''
        pk_null_query = ''
        code_query = ''
        PK_CODE_CHECK = ''

        for _,row in filtered_index.iterrows():

            table_name = row['table_name']
            table_id = row['table_id']
            table_ch_name = row['table_ch_name']
            template_flag = row['template_flag']

            #基本信息下的主键信息
            info_pk_list = row['info_pk_list']
            table_name_list.append(table_name)
            
            #检查数据字典，是否存在多个同一张表的数据字典
        
            #找到对应的数据字典
            print(f"[ INFO ] 处理表 <{table_name}>的数据字典信息.\n")
            src_data = src_dd_sheet.range('A1').expand('table').options(pd.DataFrame,header =1).value
            #去除列名中的空格
            src_data.columns = src_data.columns.str.strip()
            table_data = src_data[src_data['表英文名'] == table_name.strip()]
            #检查数据字典是否为空
            if table_data.empty:
                error_list.append(f"[ ERROR ] 源SDM文件未找到<{table_name}>对应的数据字典.\n")
            #检查数据字典是否对应多个
            if table_data['字段序号'].duplicated().any():
                error_list.append(f"[ ERROR ] 数据字典中找到<{table_name}>对应重复数据字典.\n")
            #处理主键信息
            pk_list = ','.join(table_data[table_data['是否主键'] == 'Y']['字段英文名'])
            pk_list_cn = ','.join(table_data[table_data['是否主键'] == 'Y']['字段中文名'])
            #根据标识选择主键查询类型
            #主键为空的情况,判断算法模板并拼接实际的查询语句
            if template_flag == 'AGL-PKA' :
            #记录数检查
                data_count_query = (f"SELECT \n"
                                    f"    ''\n"
                                    f"    ,'REC_CNT' \n"
                                    f"    ,CAST(COUNT(1) AS STRING)\n"
                                    f"    ,MIN(etl_create_dt) \n"
                                    "    ,'999000' \n"
                                    "    ,MAX(PT_DT)\n" 
                                    f"FROM  AGL.{table_name} \n"
                                    f"    WHERE PT_DT = '${{process_date}}' \n"
                                    "       AND DEL_F = '0' ;"
                                    )
            else:
                data_count_query = (f"SELECT \n"
                                    f"    ''\n"
                                    f"    ,'REC_CNT' \n"
                                    f"    ,CAST(COUNT(1) AS STRING)\n"
                                    f"    ,MIN(PT_DT) \n"
                                    "    ,'999000' \n"
                                    "    ,MAX(PT_DT)\n" 
                                    f"FROM  AGL.{table_name} \n"
                                    f"    WHERE PT_DT = '${{process_date}}';"
                                    )
            #插入目标表
            last_row = tgt_sheet.range('A2').expand('down').last_cell.row
            
            tgt_sheet.range(f'A{last_row + 1}').value=[(table_ch_name+'记录数统计')
                                                        ,(table_ch_name+'记录数统计')
                                                        ,'1'
                                                        ,None
                                                        ,(table_ch_name+'记录数统计')
                                                        ,'聚合表质量监测'
                                                        ,None
                                                        ,'7'
                                                        ,'7'
                                                        ,'2'
                                                        ,'530109'
                                                        ,'数据湖仓平台'
                                                        ,'AGL'
                                                        ,'1'
                                                        ,None
                                                        ,table_name
                                                        ,table_ch_name
                                                        ,'PT_DT'
                                                        ,'创建时间'
                                                        ,data_count_query
                                                        ,'xxxx'
                                                        ,'xxxx'
                                                        ,'大数据室'
                                                        ,'xxxx'
                                                        ,'科技服务部'
                                                        ]
            #优先使用数据字典中的主键信息，再使用基本信息中的主键信息
            pk_list_a = pk_list.split(',')
            #当主键列表不为空且为1，该表为单主键 
            if len(pk_list_a) == 1 and  pk_list_a != ['']:
                PK_CODE_CHECK = "CONCAT("  + f"'{pk_list_a[0]}'" + f",'01xb','PT_DT')"
                if template_flag == 'AGL-PKA':
                    pk_query = ("SELECT \n  CONCAT(" + f"CAST(COALESCE({pk_list_a[0]},'') AS STRING),'01xb'" + f",CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"  ,{PK_CODE_CHECK}\n" 
                                "   ,CONCAT(" + f"CAST(COALESCE({pk_list_a[0]},'') AS STRING),'01xb'" + f",CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,etl_create_dt \n   ,'999000'\n  ,PT_DT \n   FROM  AGL.{table_name} \n   WHERE " +  f"{pk_list_a[0]}" + f"||PT_DT in \n   ( \n" 
                                "   SELECT \n    " + f"{pk_list_a[0]}" + f"||PT_DT  \n   FROM AGL.{table_name}  \n   WHERE PT_DT = '${{process_date}}'  AND DEL_F ='0'\n" 
                                "       GROUP BY " + f"{pk_list_a[0]}" + ",PT_DT \n     HAVING COUNT(1) >1 \n ) LIMIT 100;"      
                    )
                    pk_null_query = (f"SELECT \n    '' \n"
                                    "    ,CONCAT(" + f"CAST('{pk_list_a[0]}' AS STRING)) \n"
                                    "    ,CAST(COUNT(1) AS STRING)  \n"
                                    f"    ,'${{process_date}}' \n" 
                                    "    ,'999000'\n"
                                    f"    ,'${{process_date}}'\n"
                                    f"FROM AGL.{table_name} \n"
                                    f"WHERE PT_DT = '${{process_date}}' AND DEL_F = '0' \n"
                                    "  AND CONCAT (\n"
                                    f"        CAST(COALESCE({pk_list_a[0]},'') AS STRING)\n"
                                    ") = '' LIMIT 100;"
                    )
                                
                else:
                    pk_query = ("SELECT \n  CONCAT(" + f"CAST(COALESCE({pk_list_a[0]},'') AS STRING),'01xb'" + f",CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"  ,{PK_CODE_CHECK}\n" 
                                "   ,CONCAT(" + f"CAST(COALESCE({pk_list_a[0]},'') AS STRING),'01xb'" + f",CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,PT_DT \n   ,'999000'\n  ,PT_DT \n   FROM  AGL.{table_name} \n   WHERE " +  f"{pk_list_a[0]}" + f"||PT_DT in \n   ( \n" 
                                "   SELECT \n    " + f"{pk_list_a[0]}" + f"||PT_DT  \n   FROM AGL.{table_name}  \n   WHERE PT_DT = '${{process_date}}' \n" 
                                "       GROUP BY " + f"{pk_list_a[0]}" + ",PT_DT \n     HAVING COUNT(1) >1 \n ) LIMIT 100;"      
                    )
                    pk_null_query = (f"SELECT \n    '' \n"
                                    "    ,CONCAT(" + f"CAST('{pk_list_a[0]}' AS STRING)) \n"
                                    "    ,CAST(COUNT(1) AS STRING)  \n"
                                    f"    ,'${{process_date}}' \n" 
                                    "    ,'999000'\n"
                                    f"    ,'${{process_date}}'\n"
                                    f"FROM AGL.{table_name} \n"
                                    f"WHERE PT_DT = '${{process_date}}'\n"
                                    "  AND CONCAT (\n"
                                    f"        CAST(COALESCE({pk_list_a[0]},'') AS STRING)\n"
                                    ") = ''LIMIT 100;"
                    ) 
            #多主键的情况
            elif len(pk_list_a) > 1:

                PK_CODE_CHECK = "CONCAT("  + ",".join([f"'{pk}','01xb'" for pk in pk_list_a[:-1]]) + f",'{pk_list_a[-1]}','01xb','PT_DT') \n" 

                if template_flag == 'AGL-PKA' :
                    pk_query = ("SELECT \n  CONCAT(" + ",".join([f"CAST(COALESCE({pk},'') AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,{PK_CODE_CHECK}" 
                                "    ,CONCAT(" + ",".join([f"CAST(COALESCE({pk},'') AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,etl_create_dt \n   ,'999000'\n  ,PT_DT \n   FROM  AGL.{table_name} \n   where " +  "||".join([f'{pk}' for pk in pk_list_a[:-1]]) + f"||{pk_list_a[-1]}||PT_DT IN \n   ( \n" 
                                "   SELECT \n" + "||".join([ f'{pk}' for pk in pk_list_a[:-1]]) + f"||{pk_list_a[-1]}||PT_DT  \n   FROM  AGL.{table_name}  \n   WHERE PT_DT = '${{process_date}}' and DEL_F ='0'\n" 
                                "       GROUP  BY " + ",".join([ f'{pk}'  for pk in pk_list_a[:-1]]) + f",{pk_list_a[-1]},PT_DT \n    HAVING COUNT(1) >1 \n ) LIMIT 100;"      
                    )
                    pk_null_query = (f"SELECT \n    '' \n"
                                    "    ,CONCAT(" + ",".join([f"CAST('{pk}' AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST('{pk_list_a[-1]}' AS STRING))\n"
                                    "    ,CAST(COUNT(1) AS STRING) \n"
                                    f"    ,'${{process_date}}' \n" 
                                    "     ,'999000'\n"
                                    f"    ,'${{process_date}}'\n"
                                    f"FROM AGL.{table_name} \n"
                                    f"WHERE PT_DT = '${{process_date}}' AND DEL_F = '0' \n"
                                    "  AND CONCAT (\n"
                                    "    " + ",".join([f"CAST(COALESCE({pk},'') AS STRING)" for pk in pk_list_a[:-1]])  + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING) \n"
                                    ") = '' LIMIT 100;"
                    )          
                else:
                    pk_query = ("SELECT \n  CONCAT(" + ",".join([f"CAST(COALESCE({pk},'') AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,{PK_CODE_CHECK}"
                                "    ,CONCAT(" + ",".join([f"CAST(COALESCE({pk},'') AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)) \n" 
                                f"   ,PT_DT \n   ,'999000'\n  ,PT_DT \n   FROM  AGL.{table_name} \n   where " +  "||".join([f'{pk}' for pk in pk_list_a[:-1]]) + f"||{pk_list_a[-1]}||PT_DT IN \n   ( \n" 
                                "   SELECT \n" + "||".join([ f'{pk}' for pk in pk_list_a[:-1]]) + f"||{pk_list_a[-1]}||PT_DT  \n   FROM  AGL.{table_name}  \n   WHERE PT_DT = '${{process_date}}' \n" 
                                "       GROUP  BY " + ",".join([ f'{pk}'  for pk in pk_list_a[:-1]]) + f",{pk_list_a[-1]},PT_DT \n    HAVING COUNT(1) >1 \n ) LIMIT 100;"      
                    )
                    pk_null_query = (f"SELECT \n    '' \n"
                                    "    ,CONCAT(" + ",".join([f"CAST('{pk}' AS STRING),'01xb'" for pk in pk_list_a[:-1]]) + f",CAST('{pk_list_a[-1]}' AS STRING))\n"
                                    "    ,CAST(COUNT(1) AS STRING) \n"
                                    f"    ,'${{process_date}}' \n" 
                                    "     ,'999000'\n"
                                    f"    ,'${{process_date}}'\n"
                                    f"FROM AGL.{table_name} \n"
                                    f"WHERE PT_DT = '${{process_date}}'\n"
                                    "  AND CONCAT (\n"
                                    "    " + ",".join([f"CAST(COALESCE({pk},'') AS STRING)" for pk in pk_list_a[:-1]])  + f",CAST(COALESCE({pk_list_a[-1]},'') AS STRING) \n"
                                    ") = '' LIMIT 100;"
                    )      
            #如果是1个元素且为空则主键为空，略过处理 
            elif len(pk_list_a) == 1 and  pk_list_a == ['']:
                PK_CODE_CHECK = 'PK'
                print(f"[ WARNING ]数据字典中<{table_name}>对应的主键为空,不生成主键检查语句\n.")
                error_list.append(f"[ WARNING ]数据字典中<{table_name}>对应的主键为空,不生成主键检查语句.\n")
                continue
            #插入目标表
            last_row = tgt_sheet.range('A3').expand('down').last_cell.row
            tgt_sheet.range(f'A{last_row + 1}').value=[(table_ch_name+'主键唯一')
                                                        ,(table_ch_name+'主键唯一')
                                                        ,'1'
                                                        ,None
                                                        ,(table_ch_name+'主键唯一')
                                                        ,'聚合表质量监测'
                                                        ,None
                                                        ,'1'
                                                        ,'7'
                                                        ,'2'
                                                        ,'530109'
                                                        ,'数据湖仓平台'
                                                        ,'AGL'
                                                        ,'1'
                                                        ,None
                                                        ,table_name
                                                        ,table_ch_name
                                                        ,pk_list if pk_list else ( info_pk_list if info_pk_list else "")
                                                        ,pk_list_cn if pk_list_cn else ""
                                                        ,pk_query
                                                        ,'xxxx'
                                                        ,'xxxx'
                                                        ,'大数据室'
                                                        ,'xxxx'
                                                        ,'科技服务部'
                                                        ]

            tgt_sheet.range(f'A{last_row + 2}').value=[(table_ch_name+'主键非空')
                                                        ,(table_ch_name+'主键非空')
                                                        ,'1'
                                                        ,None
                                                        ,(table_ch_name+'主键非空')
                                                        ,'聚合表质量监测'
                                                        ,None
                                                        ,'3'
                                                        ,'7'
                                                        ,'2'
                                                        ,'530109'
                                                        ,'数据湖仓平台'
                                                        ,'AGL'
                                                        ,'1'
                                                        ,None
                                                        ,table_name
                                                        ,table_ch_name
                                                        ,pk_list if pk_list else ( info_pk_list if info_pk_list else "")
                                                        ,pk_list_cn if pk_list_cn else ""
                                                        ,pk_null_query
                                                        ,'xxxx'
                                                        ,'xxxx'
                                                        ,'大数据室'
                                                        ,'xxxx'
                                                        ,'科技服务部'
                                                        ]
            #print(PK_CODE_CHECK)
            
            #检查代码映射
            #代码映射，由于码值特殊性需要对码值列设置为文本格式
            print(f"[ INFO ] 处理表 <{table_name}>的代码映射信息.\n")
            src_cm_data = src_cm_sheet.range('A1').expand('table').value
            if src_cm_data:
                src_cm_df = pd.DataFrame(src_cm_data[1:],columns=src_cm_data[1])
                #print(src_cm_df)
                filtered_src_cm_df = src_cm_df[src_cm_df['目标表英文名'] == table_name]
                if filtered_src_cm_df.empty:
                    code_columns_list = []
                    code_columns_cn_list = []
                else:
                    code_columns_list = filtered_src_cm_df['目标字段英文名'].drop_duplicates().tolist()
                    code_columns_cn_list = filtered_src_cm_df['目标字段中文名'].drop_duplicates().tolist()
            else:
                error_list.append(f"[ ERROR ] 代码映射sheet页无数据,可能是代码映射不存在或表名错误.\n")
            #获取当前最大行

            if code_columns_list:
                print(f"[ INFO ] 表 <{table_name}>的代码字段:[{', '.join(code_columns_list)}]. \n")
                for code_column,code_column_cn  in zip(code_columns_list,code_columns_cn_list):
                    last_row = tgt_sheet.range('A2').expand('down').last_cell.row
                    #print(code_column)
                    #print(template_flag)
                    if template_flag == 'AGL-PKA' :
                        #码值检查
                        code_query = (f"SELECT \n"
                                            f"  {PK_CODE_CHECK}\n"
                                            f"  ,CAST('{code_column}' AS STRING) \n"
                                            f"  ,{code_column}\n"
                                            f"  ,etl_create_dt \n"
                                            "   ,'999000' \n"
                                            "   ,PT_DT\n" 
                                            f"FROM  AGL.{table_name} \n"
                                            f"WHERE PT_DT = '${{process_date}}' \n"
                                            "   AND DEL_F = '0'\n"
                                            f"   AND {code_column} like '@%' LIMIT 100;"
                                            )
                    else:
                        code_query = (f"SELECT \n"
                                            f"  {PK_CODE_CHECK}\n"
                                            f"  ,CAST('{code_column}' AS STRING) \n"
                                            f"  ,{code_column}\n"
                                            f"  ,PT_DT \n"
                                            "   ,'999000' \n"
                                            "   ,PT_DT\n" 
                                            f"FROM  AGL.{table_name} \n"
                                            f"WHERE PT_DT = '${{process_date}}' \n"
                                            f"  AND {code_column} like '@%' LIMIT 100;"
                                            )
                    #插入目标表
                    #print(code_query)
                    tgt_sheet.range(f'A{last_row + 1}').value=[(table_ch_name+'码值检查')
                                                                ,(table_ch_name+'码值检查')
                                                                ,'1'
                                                                ,None
                                                                ,(table_ch_name+'码值检查')
                                                                ,'聚合表质量监测'
                                                                ,None
                                                                ,'7'
                                                                ,'7'
                                                                ,'2'
                                                                ,'530109'
                                                                ,'数据湖仓平台'
                                                                ,'AGL'
                                                                ,'1'
                                                                ,None
                                                                ,table_name
                                                                ,table_ch_name
                                                                ,code_column
                                                                ,code_column_cn
                                                                ,code_query
                                                                ,'xxxx'
                                                                ,'xxxx'
                                                                ,'大数据室'
                                                                ,'xxxx'
                                                                ,'科技服务部'
                                                                ]
            else:
                print(f"[ INFO ] 表 <{table_name}> 无代码映射,略过处理")
           
            processed_files += 1
            print(f"[ INFO ] 表 {table_name} 处理完成,剩余 < {total_files-processed_files} > 个,当前进度 <{int((processed_files)/total_files*100)}%> .\n")
            print("----------------------------------------------------------------\n")
        tgt_wb.save(template_file)
        tgt_wb.close()
    except Exception as e:
        error_list.append(f"{str(e)}\n")
        tgt_wb.save(template_file)
        tgt_wb.close()
        excel_app.quit()
    finally:
        excel_app.quit()


    return error_list,table_name_list,table_list
    excel_app.screen_updating = True

def info_display():
    print("\n当前版本：v1.2.1 \n"
          "最后更新日期：2025-02-08 \n"
          )


if __name__ == "__main__":
    if len(sys.argv) != 4:
            print("Usage: python gen_test_file.py <元数据文件> <模板文件> <生成人员/版本信息>")
            sys.exit(1)
    source_file = sys.argv[1]
    template_file = sys.argv[2]
    person_name = sys.argv[3] 
    info_display()
    new_file_name = copy_excel_file(template_file,person_name)
    errors,table_name_list,table_list = copy_sheets_and_metadata(source_file,new_file_name)
    print(f"\n#### 本次生成<{len(table_name_list)}>个检核信息. #### \n| {'| '.join(table_list)} " )
    if errors:
        print(f"#### 提示信息 ####:\n")
        for error in errors:
            print(f"|{error}")