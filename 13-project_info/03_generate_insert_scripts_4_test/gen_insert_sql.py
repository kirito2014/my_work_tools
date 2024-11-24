# -*- coding:utf-8 -*-
import pandas as pd 
import os,sys

def get_belong_theme(table_name):
    table_name_upper = table_name.upper()
    #print(file_name_upper[4:7])
    if table_name_upper[4:7] == "S01":
        return "AGL_CORP"
    elif table_name_upper[4:7] == "S02":
        return "AGL_RTLB"
    elif table_name_upper[4:7] == "S03":
        return "AGL_LOAN"
    elif table_name_upper[4:7] == "S04":
        return "AGL_ASSM"
    elif table_name_upper[4:7] == "S05":
        return "AGL_FINM"
    elif table_name_upper[4:7] == "S06":
        return "AGL_OPRS"
    elif table_name_upper[4:7] in ["S07","S08","S09","S10","S11"]:
        return "AGL_COMM"
    else:
        return "Unknown"

def read_excel_data(file_path):
    df = pd.read_excel(file_path,sheet_name='rem-数据字典')
    return df

#区分增全量的情况，拼接不同的查询技术字段
def generate_insert_sql(df):
    sql_scripts = []
    grouped_df = df.groupby('表英文名')
    for table_name, group_df in grouped_df:
        schema_name = get_belong_theme(table_name)
        columns_sql_PKA = ', '.join([f"{column}" for column in group_df['字段英文名'].str.split(',').explode().unique()]) + ', ETL_TIMESTAMP, ETL_CREATE_DT, ETL_LAST_ACG_DT, DEL_F'
        columns_sql_EVI = ', '.join([f"{column}" for column in group_df['字段英文名'].str.split(',').explode().unique()]) + ', ETL_TIMESTAMP'
        partition_sql = f"INSERT INTO {schema_name}.{table_name} PARTITION (PT_DT = '2023-12-25') "
        drop_sql = f"ALTER TABLE {schema_name}.{table_name} DROP IF EXISTS PARTITION (PT_DT = '2023-12-25'); \n" 
        #columns_sql = ', '.join([f"{column}" for column in group_df['字段英文名'].str.split(',').explode().unique()])
        if table_name.upper().endswith('_TF'):
            select_sql = f" SELECT {columns_sql_PKA} FROM {schema_name}.{table_name} WHERE PT_DT = '2023-09-30'; \n"
        elif  table_name.upper().endswith('_TA'):
            select_sql = f" SELECT {columns_sql_EVI} FROM {schema_name}.{table_name} WHERE PT_DT = '2023-09-30'; \n"
        #select_sql = f" SELECT {columns_sql} FROM {schema_name}.{table_name} WHERE PT_DT = '2023-09-30';"
        sql_script = drop_sql +  partition_sql + select_sql
        sql_scripts.append(sql_script)
    #print(sql_scripts)
    return sql_scripts

#生成查询数据量的语句    
def generate_query_sql(df):
    query_scripts = []
    grouped_df = df.groupby('表英文名')
    for table_name, group_df in grouped_df:
        schema_name = get_belong_theme(table_name)
        query_sql = f"SELECT '{schema_name}.{table_name}',pt_dt,count(1) FROM {schema_name}.{table_name}  group by 2 ;" 
        query_script = query_sql  
        query_scripts.append(query_script)
    #print(query_scripts)
    return query_scripts


#生成插入语句
def save_sql_scripts(sql_scripts,output_folder):
    output_file = os.path.join(output_folder,"generate_insert.sql")
    with open(output_file,'w',encoding='utf-8') as f:
        f.write('--===============数据回插脚本=================' + '\n' )
        for sql_script in sql_scripts:
            f.write(sql_script + '\n' )

def save_query_scripts(sql_scripts,output_folder):
    output_file = os.path.join(output_folder,"generate_query.sql")
    with open(output_file,'w',encoding='utf-8') as f:
        f.write('--===============数据查询脚本=================' + '\n' )
        for sql_script in sql_scripts:
            f.write(sql_script + '\n' )

def main(input_file,output_folder):
    df = read_excel_data(input_file)
    #print("lieming：",df.columns)
    sql_scripts = generate_insert_sql(df)
    query_scripts = generate_query_sql(df)
    save_sql_scripts(sql_scripts, output_folder)
    save_query_scripts(query_scripts, output_folder)

if __name__ == "__main__":
    if len(sys.argv) != 2:
            print("Usage: python gen_inser_sql.py <file_path> ")
            sys.exit(1)
    input_file = sys.argv[1]
    output_folder = "D:\sunline_etl_tool"
    main(input_file,output_folder)
        


