# -*- coding: utf-8 -*-
import numpy
import pandas
from odps import DataFrame
from odps.tunnel import TableTunnel
# from ops.models import TableSchema, Column, Type
import sys,datetime,re
#from typing import List,Tuple,Optional,Dict

reload(sys)
sys.setdefaultencoding('utf8')

import logging

# 设置参数信息，仅测试
options.tunnel.use_instance_tunnel = True
options.tunnel.limit_instance_tunnel = False


def parse_sql_content(content, app_name, block_num, node_name, file_id):
    """
    解析SQL内容，提取表依赖关系
    args:
        content:脚本内容
        app_name:目标库名
        block_num:代码块编号
        node_name:节点名称
        file_id:文件ID
    returns:
        提出的表的依赖关系列表
    """
    results = []
    print("开始解析节点名称：{0},文件ID：{1}".format(node_name, file_id))

    # 正则表达式获取content中的数据信息
    patterns = [
        r'p_cz_[a-zA-Z0-9_]+_prd\.[a-zA-Z0-9_]+',  # p_cz_xxx_prd.yyy
        r'\{[a-zA-Z_]+\}\.\[a-zA-Z0-9_]+'  # {xxx}.yyy
    ]
    # 按分号切割SQL语句
    # 注意需要处理字符串中的分号注释等特殊情况
    sql_statements = []
    current_statement = []
    in_string = False
    string_char = None

    content = content.replace('\n', ' ')

    #简单的分割逻辑
    for char in content:
        if char in ('"', "'") and (not in_string or string_char == char):
            in_string = not in_string
            if in_string:
                string_char = char
            else:
                string_char = None
        elif char == ';' and not in_string:
            stmt = ''.join(current_statement).strip()
            if stmt:
                sql_statements.append(stmt)
            current_statement = []
        else:
            current_statement.append(char)

    #处理最后一个语句
    if current_statement:
        stmt = ''.join(current_statement).strip()
        if stmt:
            sql_statements.append(stmt)

    #print("111111111111111{0}".format(sql_statements))
    print("节点{0}，文件{1}共解析出{2}条SQL语句".format(node_name,file_id,len(sql_statements)))
    # 处理每一条SQL语句
    for statement_num, sql in enumerate(sql_statements,1):
        all_tables = []

        #匹配所有的表
        for pattern in patterns:
            matches = re.findall(pattern, sql)
            all_tables.extend(matches)
        print("已解析到{0}张初始表".format(len(all_tables)))

    #格式化表名（去除花括号）传参的话不能正确获取，可能需要获取传参后的数据匹配后进行处理，可以写一个映射表
    formatted_tables = []
    if len(all_tables) != 0:
        for table in all_tables:
            if table.startswith('{'):
                # {xxx}.yyy -> xxx.yyy
                clean_table = table.replace('{', '').replace('}', '')
                formatted_tables.append(clean_table)
            else:
                formatted_tables.append(table)
    #print(all_tables)
    print("在代码子区块-{0} 解析匹配到-{1}张表".format(statement_num, len(all_tables)))

    #识别是否是目标表
    target_tables_in_block = []
    source_tables_in_block = []
    #print(formatted_tables)

    target_tables_in_block.append(formatted_tables[0])
    source_tables_in_block = formatted_tables[1:]

    # 确定是目标表（取第一个）
    target_table_name = None
    target_db_name = None

    if target_tables_in_block:
        #如果有多个目标表，暂时取第一个
        first_target = target_tables_in_block[0]
        target_db_name,target_table_name = first_target.split('.',1)

        #把没匹配上的放回到source_block中
        source_tables_in_block.append(target_tables_in_block[1:])
    else:
        #如果没有取到任何目标库，那么默认以最后一个区块的第一个表作为目标库
        target_db_name,target_table_name = formatted_tables[-1].split('.',1)

    #print("111111111{0},2222222{1}".format(target_db_name,target_table_name))
    #print("目标表{0}".format(target_tables_in_block))
    #print("来源表{0}".format(source_tables_in_block))

    # 为每个来源表生成记录
    for source_table in source_tables_in_block:
        if '.' in source_table:
            source_db, source_table_name = source_table.split('.',1)
            results.append((
                node_name,
                file_id,
                block_num,
                statement_num,
                target_db_name,
                target_table_name,
                source_table,
                source_db,
                source_table_name
            ))

    print("节点{0} 完成解析，提取到{1}条依赖关系.".format(node_name,len(results)))

    return results


def create_table_if_not_exists(target_table):
    print("----------------1.1 检查并创建目标表----------------------------")
    print("---检查表是否存在---")
    if not o.exist_table(target_table):
        print("目标表{0}不存在,创建表...".format(target_table))
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {0} (
            node_name STRING COMMENT '节点名称',
            file_id INT COMMENT '文件编号',
            block_num INT COMMENT '代码编号',
            statement_num INT COMMENT '语句序号',
            target_db_name STRING COMMENT '目标库名',
            target_table_name STRING COMMENT '目标表名',
            source_table STRING COMMENT '来源表全名',
            source_db_name STRING COMMENT '来源库名',
            source_table_name STRING COMMENT '来源表名'
        ) COMMENT 'SQL依赖关系解析结果表'
        PARTITIONED BY (data_dt STRING COMMENT '日期分区')
        LIFECYCLE 30;
        """.format(target_table)
        #执行建表语句
        try:
            o.execute_sql(create_table_sql)
            print("---目标表已创建---")
        except Exception, e:
            print("---目标表创建失败！！！{0}---".format(str(e)))


def create_tmp_table(tmp_table_name):
    print("----------------1.2. 检查并创建临时表----------------------------")
    try:
        # 向目标表插入数据，根据数据量选择合适的插入语句
        # 插入临时表数据
        create_tmp_table_sql = """
            CREATE TABLE IF NOT EXISTS {0} (
                node_name STRING COMMENT '节点名称',
                file_id INT COMMENT '文件编号',
                block_num INT COMMENT '代码编号',
                statement_num INT COMMENT '语句序号',
                target_db_name STRING COMMENT '目标库名',
                target_table_name STRING COMMENT '目标表名',
                source_table STRING COMMENT '来源表全名',
                source_db_name STRING COMMENT '来源库名',
                source_table_name STRING COMMENT '来源表名'
            ) COMMENT 'SQL依赖关系解析结果临时表'
            ;
            """.format(tmp_table_name)
        try:
            o.execute_sql(create_tmp_table_sql)
            if o.exist_table(tmp_table_name):
                print("------临时表创建成功：{0}-------".format(tmp_table_name))
            else:
                print("------临时表创建失败！！！-------")
        except Exception, e:
            print("创建临时表时发生错误：{0}".format(str(e)))
            raise
    except Exception, e:
        print("创建临时表时发生错误：{0}".format(str(e)))
        raise


def main():
    print("----------------0.检查环境信息----------------------------")
    print("---获取来源表与目标表---")
    # 定义来源表
    source_table = 'ods_sunline.bak_dws_metadata_odps_jobs_info'
    # 定义目标表
    target_table = 'ods_sunline.all_rely_temp_wmj'
    #临时表
    tmp_table_name = 'ods_sunline.all_rely_temp_wmj_1'

    print("来源表：{0},目标表：{1}".format(source_table,target_table))
    create_table_if_not_exists(target_table)
    create_tmp_table(tmp_table_name)
    #查询SQL
    query_sql=r'''
    select node_name,
        file_id,
        lower(REGEXP_REPLACE(CONTENT,'--.*','')) as content,
        file_type,
        app_name,
        app_odps_project_name,
        output,
        data_dt
    from {0}
    where data_dt = '20251105'
        and lower(content) regexp '_zsrun_|_zspsa_|_zsaes_|_pics_|_ylcs_|_sgldb_|_aesdb_|_db2pics_|_db2sql_'
        and coalesce(content,'')!='' and file_type not in ('数据集成','dowhile内置节点','SHELL')
        and file_id = 359514
    '''.format(source_table)
    print("查询语句：{0}".format(query_sql))

    try:
        print("----------------1.2. 开始执行分析任务----------------------------")
        with o.execute_sql(query_sql).open_reader(tunnel=True) as reader:
            all_results = []

            for idx,record in enumerate(reader):
                try:
                    node_name = record['node_name']
                    file_id = record['file_id']
                    content = record['content']
                    file_type = record['file_type']
                    app_name = record['app_name']

                    print("处理第{0}条数据：{1}-{2}".format(idx + 1,node_name,file_id ))

                    #解析SQL内容
                    records = parse_sql_content(content,app_name,idx + 1,node_name,file_id)
                    all_results.extend(records)
                except Exception, e:
                    print("处理日志失败（索引{0}，文件ID：{1}）：{2} ".format(idx + 1,file_id, str(e)))
            print("在文件{0}-节点{1}，共解析{2}条依赖关系".format(file_id,node_name,len(all_results)))
        print("----------------1.3. 获取数据成功----------------------------")

        print("-------------------测试输出结果信息-------------------")
        for i,record in enumerate(all_results[:10]):
            print("{0}:{1}".format(i + 1,record))

        print("----------------2.插入表数据----------------------------")
        print("---上传数据到临时表，共{0}条数据---".format(len(all_results)))

        print("----------------2.1. 清空临时表数据----------------------------")
        truncate_sql = """
            truncate table {0};
            """.format(tmp_table_name)
        try:
            o.execute_sql(truncate_sql)
        except Exception, e:
            print("清空临时表数据时发生错误：{0}".format(str(e)))
            raise
        print("---清除临时表成功---")

        if all_results:
            try:
                insert_sql = "INSERT INTO {0} VALUES ".format(tmp_table_name)
                value_list = []
                for record in all_results:
                    node_name = "'{0}'".format(record[0].upper())
                    file_id = str(record[1])
                    block_num = str(record[2])
                    statement_num = str(record[3])
                    target_db_name = "'{0}'".format(record[4].upper().replace("'","''")) if record[4] else "NULL"
                    target_table_name = "'{0}'".format(record[5].upper().replace("'","''")) if record[5] else "NULL"
                    source_table = "'{0}'".format(record[6].upper().replace("'","''")) if record[6] else "NULL"
                    source_db_name = "'{0}'".format(record[7].upper().replace("'","''")) if record[7] else "NULL"
                    source_table_name = "'{0}'".format(record[8].upper().replace("'","''")) if record[8] else "NULL"
                    value_list.append("({0},{1},{2},{3},{4},{5},{6},{7},{8})".format(node_name ,file_id ,block_num ,statement_num ,target_db_name ,target_table_na
                ))
                batch_size = 100
                for i in range(0,len(value_list),batch_size):
                    batch_sql = insert_sql + ",".join(value_list[i:i+batch_size])
                    o.execute_sql(batch_sql)
                print("数据已完成上传临时表{0}".format(tmp_table_name))
            except Exception,e:
                print("使用Tunnel上传数据失败:{0}".format(str(e)))
        #将数据加载到目标表
        #根据数据量进行匹配
        print("----------------4. 插入目标表数据----------------------------")
        #t = o.get_table("target_table")
        #print(t)
        print("----------------4.1. 数据小于1000----------------------------")
        if len(all_results) <= 1000:
            #INSERT OVERWRITE
            try:
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                insert_sql = """
                    INSERT OVERWRITE TABLE {0} PARTITION(data_dt='{1}')
                    SELECT
                    upper(node_name) ,
                    upper(file_id) ,
                    upper(block_num) ,
                    upper(statement_num) ,
                    upper(target_db_name) ,
                    upper(target_table_name) ,
                    upper(source_table) ,
                    upper(source_db_name) ,
                    upper(source_table_name)
                    FROM {2}
                    """.format(target_table, today, tmp_table_name)
                print("数据量小于1000,使用INSERT OVERWRITE 插入目标表数据:{0}".format(insert_sql))
                o.execute_sql(insert_sql)
                print("数据量小于1000，已完成使用INSERT OVERWRITE 插入目标表数据")
            except Exception, e:
                print("数据量小于1000的插入失败：{0}".format(str(e)))
                raise
        else:
            print("----------------4.1. 数据大于1000----------------------------")
            from ops.df import DataFrame
            import pandas as pd
            df = pd.DataFrame(all_results,columns=['node_name','file_id','block_num','statement_num','target_db_name','target_table_name','source_table','source_db_name','source_table_name'])
            df = df.rename(columns={'source_db':'source_db_name'})
            #print(df.columns)
            #写入数据
            try:
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                DataFrame(df).persist(target_table,partition={'data_dt':today})
                print("大于1000的数据插入，使用pandas插入数据:{0};数据日期:{1}".format(target_table,today))
            except Exception, e:
                print("{0}数据插入失败:{1},{2}".format(target_table,file_id,str(e)))
    except Exception, e:
        print("---执行依赖分析任务失败---")
        print("错误日志如下：{0}".format(str(e)))
        raise
    finally:
        print("----------------5. 执行结束----------------------------")


if __name__ == '__main__':
    main()