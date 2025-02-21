import sys
import os
import logging
from typing import List, Tuple
import pandas as pd
import xlwings as xw
import shutil

# 常量配置
TEMPLATE_SHEET = '模板'
DATA_DICT_SHEET = 'rem-数据字典'
CODE_MAP_SHEET = 'rem-代码映射'
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
    new_file = f"生产验证文件-{person_name}.xls"
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

def clear_excel_data(file_path: str) -> None:
    """清空模板Excel的数据，确保进程释放"""
    app = None
    try:
        app = xw.App(visible=False)
        wb = app.books.open(file_path)
        
        # 处理工作表存在性检查
        try:
            sheet = wb.sheets[TEMPLATE_SHEET]
        except Exception as SheetNotFound:
            logger.error(f"[{file_path}] 缺少必要的工作表: {TEMPLATE_SHEET}")
            return

        # 计算实际数据范围
        start_cell = sheet.range('A2')
        last_row = start_cell.end('down').row
        last_col = start_cell.end('right').column
        
        # 仅当存在数据时执行清空
        if last_row > 2 or last_col > 1:
            sheet.range((2,1), (last_row, 15)).clear_contents()
        
        wb.save()
        app.quit()
        logger.info(f"清空数据完成: {file_path}")
    except Exception as e:
        logger.error(f"清空数据失败: {str(e)}")

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


def _clean_index_data(df: pd.DataFrame) -> pd.DataFrame:
    """清洗index页数据"""
    df.columns = ['table_id','table_name','table_cn_name','enable_flag','template_flag','info_pk_list']
    df['table_id'] = df['table_id'].str.split("'").str[1]
    return df[df['enable_flag'] == 'Y']

def generate_pk_query(pk_list: List[str], table_name: str, template_flag: str) -> Tuple[str, str]:
    """生成主键检查SQL"""
    try:
        # 根据主键数量生成不同逻辑
        if len(pk_list) >= 1:
            return _handle_pk(pk_list, table_name, template_flag)
        else:
            return "无主键,无需检查",""
    except Exception as e:
        logger.error(f"生成主键查询失败: {str(e)}")
        return "",""

def _handle_pk(pk: str, table_name: str, template_flag: str) -> Tuple[str, str]:
    """处理单主键逻辑"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    
    pk_query = f"""SELECT COUNT(1) FROM (
                        SELECT {pk},COUNT(1) 
                        FROM AGL.{table_name}
                        WHERE PT_DT = '${{process_date}}' 
                        {del_condition}
                        GROUP BY {pk} 
                        HAVING COUNT(1) >1
                        );"""
    count_query = f"""SELECT COUNT(1) FROM  AGL.{table_name}
                        WHERE PT_DT = '${{process_date}}' 
                        {del_condition}
                        ;"""
    #print(pk_query)
    return pk_query,count_query

def write_to_template(sheet, data):
    try:
        last_row = sheet.range('A1').expand('down').last_cell.row
        sheet.range(f'A{last_row + 1}').options(index=False, header=False).value =data
    except Exception as e:
        logging.error(f"写入Excel失败: {e}")
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

            print(index_data)

            for _, row in index_data.iterrows():
                table_name = row['table_name']
                table_cn_name = row['table_cn_name']
                logger.info(f"正在处理表: {table_name}")
                
                # 数据字典处理
                dd_data = process_data_dictionary(src_wb, table_name)
                #print(dd_data)
                if dd_data.empty:
                    errors.append(f"[{table_name}] 数据字典缺失")
                    continue
                
                # 主键处理
                pk_list = ','.join(dd_data[dd_data['是否主键'] == 'Y']['字段英文名'])
                pk_list_cn = ','.join(dd_data[dd_data['是否主键'] == 'Y']['字段中文名'])
                p_qry,c_qry = generate_pk_query(pk_list, table_name, row['template_flag'])

                #
                
                #构建写入数据
                #主键唯一性 检查
                base_data = ['A','01-直接映射','02-唯一性','0201-主键唯一性',
                    '检查主键是否重复',SYSTEM_CODE,table_cn_name,table_name,
                    pk_list,pk_list_cn,'2025-02-21',p_qry
                ]
                write_to_template(tgt_sheet, base_data)
                #实体唯一性 检查
                base_data = ['A','01-直接映射','02-唯一性','0202-实体唯一性',
                    '检查非空字段是否为空',SYSTEM_CODE,table_cn_name,table_name,
                    pk_list,pk_list_cn,'2025-02-21',c_qry
                ]

                write_to_template(tgt_sheet, base_data)


                processed_tables.append(table_name)
                
            tgt_wb.save()
            #app.quit()
            src_wb.close()
            return errors, processed_tables
            
    except Exception as e:
        logger.error(f"主流程异常: {str(e)}")
        errors.append(str(e))
        return errors, processed_tables


if __name__ == "__main__":
    source_file = sys.argv[1]
    target_file = sys.argv[2]
    #person_name = sys.argv[3]
    #new_file = copy_excel_template(target_file, person_name)
    #clear_excel_data(new_file)
    copy_sheets_and_metadata(source_file, target_file)
