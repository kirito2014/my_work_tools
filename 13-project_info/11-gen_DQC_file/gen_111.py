import sys
import os
import logging
from typing import List, Tuple
import pandas as pd
import xlwings as xw
import shutil

# 常量配置
TEMPLATE_SHEET = '模板字段'
DATA_DICT_SHEET = 'rem-数据字典'
CODE_MAP_SHEET = 'rem-代码映射'
DEFAULT_PROJECT_CODE = '530102'
PLATFORM_NAME = '数据湖平台（旧）'
SYSTEM_CODE = 'AGL'

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    handlers=[logging.FileHandler('sdm_merge.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def copy_excel_template(target_file: str, person_name: str) -> str:
    """复制模板文件并返回新路径"""
    if not os.path.isfile(target_file):
        logger.error("模板文件不存在！！")
        return None
    new_file = f"通用规则EXCEL模板下载-{person_name}.xls"
    try:
        if os.path.isfile(new_file):
            logger.info(f"删除旧文件: {new_file}")
            os.remove(new_file)
        shutil.copyfile(target_file, new_file)
        logger.info(f"创建新文件: {new_file}")
        return new_file
    except Exception as e:
        logger.error(f"文件操作失败: {str(e)}")
        raise

def generate_pk_query(pk_list: List[str], table_name: str, template_flag: str) -> Tuple[str, str]:
    """生成主键检查SQL"""
    try:
        # 根据主键数量生成不同逻辑
        if len(pk_list) == 1:
            return _handle_single_pk(pk_list[0], table_name, template_flag)
        elif len(pk_list) > 1:
            return _handle_multi_pk(pk_list, table_name, template_flag)
    except Exception as e:
        logger.error(f"生成主键查询失败: {str(e)}")
        return "", ""

def _handle_single_pk(pk: str, table_name: str, template_flag: str) -> Tuple[str, str]:
    """处理单主键逻辑"""
    date_field = "etl_create_dt" if template_flag == 'AGL-PKA' else "PT_DT"
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    
    pk_query = f"""
    SELECT CONCAT(CAST(COALESCE({pk},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)),
           CONCAT('{pk}','01xb','PT_DT'),
           CONCAT(CAST(COALESCE({pk},'') AS STRING),'01xb',CAST(COALESCE(PT_DT,'') AS STRING)),
           {date_field},'999000',PT_DT
    FROM AGL.{table_name}
    WHERE {pk}||PT_DT IN (
        SELECT {pk}||PT_DT
        FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}' {del_condition}
        GROUP BY {pk},PT_DT HAVING COUNT(1) >1
    ) LIMIT 100;"""
    
    pk_null = f"""
    SELECT '',CONCAT(CAST('{pk}' AS STRING)),CAST(COUNT(1) AS STRING),
           '${{process_date}}','999000','${{process_date}}'
    FROM AGL.{table_name}
    WHERE PT_DT = '${{process_date}}' {del_condition}
      AND TRIM(COALESCE({pk},'')) = '' LIMIT 100;"""
    
    return pk_query, pk_null

def process_data_dictionary(src_wb: xw.Book, table_name: str) -> pd.DataFrame:
    """处理数据字典页逻辑"""
    try:
        sheet = src_wb.sheets[DATA_DICT_SHEET]
        sheet.api.AutoFilterMode = False  # 清除筛选器
        df = sheet.range('A1').expand('table').options(pd.DataFrame, header=1).value
        df.columns = df.columns.str.strip()
        return df[df['表英文名'] == table_name.strip()]
    except Exception as e:
        logger.error(f"处理数据字典失败: {str(e)}")
        return pd.DataFrame()

def write_to_template(sheet: xw.Sheet, data: list) -> None:
    """批量写入Excel数据"""
    try:
        last_row = sheet.range('A1').end('down').row
        sheet.range(f'A{last_row+1}').value = data
    except Exception as e:
        logger.error(f"写入Excel失败: {str(e)}")
        raise
        
def copy_sheets_and_metadata(source_file: str, target_file: str) -> Tuple[List[str], List[str]]:
    """主处理逻辑"""
    errors = []
    processed_tables = []
    
    try:
        with xw.App(visible=False) as app:
            app.screen_updating = False
            
            src_wb = app.books.open(source_file)
            tgt_wb = app.books.open(target_file)
            tgt_sheet = tgt_wb.sheets[TEMPLATE_SHEET]

            index_data = pd.read_excel(source_file, sheet_name='index', usecols="C,D,E,M,N,P")
            index_data = _clean_index_data(index_data)
            
            for _, row in index_data.iterrows():
                table_name = row['table_name']
                logger.info(f"正在处理表: {table_name}")
                
                # 数据字典处理
                dd_data = process_data_dictionary(src_wb, table_name)
                if dd_data.empty:
                    errors.append(f"[{table_name}] 数据字典缺失")
                    continue
                
                # 主键处理
                pk_list = dd_data[dd_data['是否主键'] == 'Y']['字段英文名'].tolist()
                qry, null_qry = generate_pk_query(pk_list, table_name, row['template_flag'])
                
                # 构建写入数据
                base_data = [
                    row['table_ch_name'] + '记录数统计',
                    row['table_ch_name'], '聚合表质量监测',
                    DEFAULT_PROJECT_CODE, PLATFORM_NAME, 
                    SYSTEM_CODE, table_name
                ]
                write_to_template(tgt_sheet, [base_data + [qry]])
                write_to_template(tgt_sheet, [base_data + [null_qry]])

                processed_tables.append(table_name)
                
            tgt_wb.save()
            return errors, processed_tables
            
    except Exception as e:
        logger.error(f"主流程异常: {str(e)}")
        errors.append(str(e))
        return errors, processed_tables

def _clean_index_data(df: pd.DataFrame) -> pd.DataFrame:
    """清洗index页数据"""
    df.columns = ['table_id','table_name','table_ch_name','enable_flag','template_flag','info_pk_list']
    df['table_id'] = df['table_id'].str.split("'").str[1]
    return df[df['enable_flag'] == 'Y']

if __name__ == "__main__":
    if len(sys.argv) != 4:
        logger.error("参数错误！用法: python script.py <源文件> <模板文件> <用户>")
        sys.exit(1)
    try:
        new_file = copy_excel_template(sys.argv[2], sys.argv[3])
        errors, tables = copy_sheets_and_metadata(sys.argv[1], new_file)
        
        logger.info(f"\n成功处理 {len(tables)} 张表:")
        logger.info("\n".join(tables))
        
        if errors:
            logger.warning("\n遇到以下问题:")
            logger.warning("\n".join(errors))
            
    except Exception as e:
        logger.error(f"程序崩溃: {str(e)}", exc_info=True)
        sys.exit(1)