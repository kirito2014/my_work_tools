"""
文件名(File Name)               :gen_chk_file.py
作者(Author)                    :wangmujun@Sunline
编写时间(CreateTime)            :2025-02-21
版本号(Version)                 :V1.0.0
使用方法(Usage)                 :python gen_chk_file.py <元数据文件> <模板文件> <生成人员/版本信息>
功能描述(Descriptions)          :
    本脚本主要用于实现以下功能:
    1、根据通用SDM模板生成校验规则下的excel文件
    2、
依赖库(Dependences):
    - pandas >= 2.2.3
    - xlwings >= 0.33.4
修改历史(Histories):
    v1.0.0 - 2025-02-21 - 初始版本
    
"""


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

def clear_filters(sheet):
    """清除指定工作表的筛选器"""
    if sheet.api.AutoFilter:
        sheet.api.AutoFilterMode = False

def check_vaild_excel(sheet: xw.Sheet) -> bool:
    """检查最大行是否一致如果一致则返回True，不一致则返回False"""
    max_row_a=sheet.range('A1').expand('down').last_cell.row
    max_row_i=sheet.range('I1').expand('down').last_cell.row
    if max_row_a and max_row_i and max_row_i != max_row_a:
        logger.error(f"{sheet.name} 首列非空校验不通过.")
        return False
    elif max_row_a and max_row_i and max_row_i == max_row_a:
        logger.info(f"{sheet.name} 首列非空校验通过.")
        if sheet.name == 'rem-代码映射':
            if sheet.range('A1').value != 'SRC_TAB_LIB_NAME' or sheet.range('A1').value == 'None':
                logger.error(f"{sheet.name} 首列内容校验不通过，确认是否错行缺失.")
                return False
        return True
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

        # 检查并标记包含"CUST_IN_CD"的字段
        df['is_cust_in_cd'] = df['字段英文名'].str.upper().str.contains('CUST_IN_CD')

        # 标记以"TELR_NO"结尾的字段
        df['is_teller_no'] = df['字段英文名'].str.upper().str.endswith('TELR_NO')

        # 标记以"ORG_NO"结尾的字段
        df['is_org_no'] = df['字段英文名'].str.upper().str.endswith('ORG_NO')

        # 标记以"_DT"结尾的字段
        df['is_dt'] = df['字段英文名'].str.upper().str.endswith('_DT')

        # 标记以 “TM_STAMP”结尾的字段
        df['is_tm_stamp'] = df['字段英文名'].str.upper().str.endswith('TM_STAMP')
        
        return df[df['表英文名'] == table_name.strip()]
    except Exception as e:
        logger.error(f"处理数据字典失败: {str(e)}")
        return pd.DataFrame()

def process_code_map(src_wb: xw.Book, table_name: str) -> pd.DataFrame:
    """处理代码映射页逻辑"""
    try:
        sheet = src_wb.sheets[CODE_MAP_SHEET]
        sheet.api.AutoFilterMode = False
        # 从第二行开始读取数据
        df = sheet.range('A2').expand('table').options(pd.DataFrame, header=1).value
        if df.empty:
            logger.error(f"[{table_name}] 代码映射表没有数据")
            return pd.DataFrame()
        df.columns = df.columns.str.strip()
        df['目标代码码值'] = df['目标代码码值'].fillna('').astype(str).str.strip()
        df['目标代码说明'] = df['目标代码说明'].fillna('').astype(str).str.strip()
        # 筛选出目标表英文名等于table_name的数据
        df = df[df['目标表英文名'] == table_name.strip()]
        if df.empty:
            return df
        # 组合代码值和说明
        df['code_info'] = df.apply(lambda row: f"{row['目标代码码值']}-{row['目标代码说明']}", axis=1)
        # 按目标字段中英文名分组，合并code_info
        grouped_df = df.groupby(['目标字段中文名', '目标字段英文名'])['code_info'].agg('\n'.join).reset_index()
        return grouped_df
    except Exception as e:
        logger.error(f"处理代码映射失败: {str(e)}")
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

def generate_field_validation_sql(table_name: str, field_name: str, template_flag: str) -> Tuple[str, str]:
    """生成字段非空校验和空值率统计SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        null_check_sql = f"""SELECT COUNT(1) FROM AGL.{table_name} 
                            WHERE PT_DT = '${{process_date}}' 
                            {del_condition}
                            AND ({field_name} IS NULL OR {field_name} = '');"""
        null_ratio_sql = f"""SELECT ROUND( CAST(SUM( CASE WHEN {field_name} IS NULL OR {field_name} = '' THEN 1 ELSE 0 END) AS FLOAT ) / NULLIF(COUNT(*),0) * 100, 2) AS null_ratio
                            FROM AGL.{table_name} 
                            WHERE PT_DT = '${{process_date}}'
                            {del_condition}
                            ;"""
        return null_check_sql, null_ratio_sql
    except Exception as e:
        logger.error(f"生成字段校验SQL失败: {str(e)}")
        return "", ""

def generate_cust_in_cd_validation_sql(table_name: str, field_name: str, template_flag: str) -> Tuple[str, str]:
    """生成客户内码字段的内容和长度校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 内容有效性校验（以81、82、83开头）
        content_valid_sql = f"""
        SELECT COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        AND SUBSTR({field_name}, 1,2) NOT  IN ('81', '82', '83'));
        """
        
        # 长度有效性校验（长度为11位）
        length_valid_sql = f"""
        SELECT COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        AND LENGTH({field_name}) != 11;
        """
        
        return content_valid_sql.strip(), length_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成客户内码校验SQL失败: {str(e)}")
        return "", ""

def generate_teller_no_validation_sql(table_name: str, field_name: str, template_flag: str) -> str:
    """生成柜员编号字段的长度有效性校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 长度有效性校验（长度为6位）
        length_valid_sql = f"""
        SELECT COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        AND LENGTH({field_name}) != 6;
        """
        return length_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成柜员编号校验SQL失败: {str(e)}")
        return ""
    
def generate_org_no_validation_sql(table_name: str, field_name: str, template_flag: str) -> str:
    """生成机构编号字段的长度有效性校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 长度有效性校验（长度为6位）
        length_valid_sql = f"""
        SELECT COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        AND LENGTH({field_name}) != 6;
        """
        return length_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成机构编号校验SQL失败: {str(e)}")
        return ""
    
def generate_dt_validation_sql(table_name: str, field_name: str, template_flag: str) -> str:
    """生成日期字段的长度有效性校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 长度有效性校验（长度为10位）
        length_valid_sql = f"""
        SELECT LENGTH({field_name}),COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        GROUP BY LENGTH({field_name});
        """
        return length_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成日期格式校验SQL失败: {str(e)}")
        return ""

def generate_tm_stamp_validation_sql(table_name: str, field_name: str, template_flag: str) -> str:
    """生成时间戳字段的长度有效性校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 长度有效性校验（长度为26位）
        length_valid_sql = f"""
        SELECT LENGTH({field_name}),COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        GROUP BY LENGTH({field_name});
        """
        return length_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成时间戳格式校验SQL失败: {str(e)}")
        return ""
    
def generate_code_validation_sql(table_name: str, field_name: str, template_flag: str) -> str:
    """码值映射校验SQL"""
    del_condition = "AND DEL_F = '0'" if template_flag == 'AGL-PKA' else ""
    try:
        # 码值映射校验
        code_valid_sql = f"""
        SELECT {field_name},COUNT(1) FROM AGL.{table_name}
        WHERE PT_DT = '${{process_date}}'
        {del_condition}
        GROUP BY  {field_name};
        """
        return code_valid_sql.strip()
    except Exception as e:
        logger.error(f"生成码值映射校验SQL失败: {str(e)}")
        return ""

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

            #检查首行

            src_data_sheet = src_wb.sheets[DATA_DICT_SHEET]
            src_code_sheet = src_wb.sheets[CODE_MAP_SHEET]

            if not check_vaild_excel(src_data_sheet) or not check_vaild_excel(src_code_sheet): 
                errors.append("首行校验失败")
                tgt_wb.close()
                src_wb.close()
                sys.exit()
            else:
                clear_filters(src_data_sheet)
                clear_filters(src_code_sheet)


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
                    pk_list_cn,pk_list,'2025-02-21',p_qry
                ]
                write_to_template(tgt_sheet, base_data)
                #实体唯一性 检查
                base_data = ['A','01-直接映射','02-唯一性','0202-实体唯一性',
                    '检查非空字段是否为空',SYSTEM_CODE,table_cn_name,table_name,
                    pk_list_cn,pk_list,'2025-02-21',c_qry
                ]

                write_to_template(tgt_sheet, base_data)

                # 处理代码映射
                cm_data = process_code_map(src_wb, table_name)
                if not cm_data.empty:
                    # 生成枚举值有效性校验规则
                    for _, cm_row in cm_data.iterrows():
                        field_cn = cm_row['目标字段中文名']
                        field_en = cm_row['目标字段英文名']
                        code_info = cm_row['code_info']
                        
                        code_check_sql = generate_code_validation_sql(table_name, field_en, row['template_flag'])
                        # 生成校验规则
                        code_rule_sql = [
                            'A', '01-直接映射', '03-有效性', '0304-枚举值有效性',
                            f'检查字段【{field_cn}】的代码值是否在代码表中', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn, field_en,
                            '2025-02-21', code_check_sql,"",code_info
                        ]
                        write_to_template(tgt_sheet, code_rule_sql)

                # 生成字段非空校验和空值率统计
                for _, field_row in dd_data.iterrows():
                    field_name = field_row['字段英文名']
                    field_cn_name = field_row['字段中文名']
                    
                    # 获取SQL语句
                    null_check_sql, null_ratio_sql = generate_field_validation_sql(table_name, field_name,row['template_flag'])
                    
                    # 非空校验 01-完整性	0101-非空完整性	统计字段非空值
                    base_data_null = ['A','01-直接映射','01-完整性','0101-非空完整性',
                        f'统计字段【{field_cn_name}】非空数量',SYSTEM_CODE,table_cn_name,table_name,
                        field_cn_name,field_name,'2025-02-21',null_check_sql]
                    write_to_template(tgt_sheet, base_data_null)
                    
                    # 空值率统计 02-关联处理	01-完整性	0404-参照一致性	主表与从表关联覆盖率大于0
                    base_data_ratio = ['A','02-关联处理','01-完整性','0404-参照一致性',
                        f'主表与从表关联覆盖率大于0',SYSTEM_CODE,table_cn_name,table_name,
                        field_cn_name,field_name,'2025-02-21',null_ratio_sql]
                    write_to_template(tgt_sheet, base_data_ratio)

                    # 客户内码特定校验
                    if field_row.get('is_cust_in_cd', False):
                        # 生成内容和长度校验SQL
                        content_valid_sql, length_valid_sql = generate_cust_in_cd_validation_sql(table_name, field_name,row['template_flag'])
                        
                        # 内容有效性校验
                        base_data_content = [
                            'A', '01-直接映射', '03-有效性', '0302-内容有效性',
                            f'验证客户内码【{field_cn_name}】以（81，82，83）开头', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', content_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_content)
                        
                        # 长度有效性校验
                        base_data_length = [
                            'A', '01-直接映射', '03-有效性', '0301-长度有效性',
                            f'统计客户内码【{field_cn_name}】长度（11位）', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', length_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_length)
                    
                    #柜员编号长度校验
                    if field_row.get('is_teller_no', False):
                        # 生成长度校验SQL
                        length_valid_sql = generate_teller_no_validation_sql(table_name, field_name,row['template_flag'])
                        
                        # 长度有效性校验
                        base_data_length = [
                            'A', '01-直接映射', '03-有效性','0301-长度有效性',
                            f'统计柜员编号【{field_cn_name}】字段长度（6位）', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', length_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_length)

                    #机构编号长度校验
                    if field_row.get('is_org_no', False):
                        # 生成长度校验SQL
                        length_valid_sql = generate_org_no_validation_sql(table_name, field_name,row['template_flag'])
                        
                        # 长度有效性校验
                        base_data_length = [
                            'A', '01-直接映射', '03-有效性','0301-长度有效性',
                            f'统计机构编号【{field_cn_name}】字段长度（6位）', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', length_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_length)

                    #日期长度校验
                    if field_row.get('is_dt', False):
                        # 生成长度校验SQL
                        length_valid_sql = generate_dt_validation_sql(table_name, field_name,row['template_flag'])
                        
                        # 长度有效性校验
                        base_data_length = [
                            'A', '01-直接映射', '03-有效性','0303-日期有效性',
                            f'检查日期字段【{field_cn_name}】是否为yyyy-mm-dd 10位格式', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', length_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_length)             

                   #时间戳长度校验
                    if field_row.get('is_dt', False):
                        # 生成长度校验SQL
                        length_valid_sql = generate_dt_validation_sql(table_name, field_name,row['template_flag'])
                        
                        # 长度有效性校验
                        base_data_length = [
                            'A', '01-直接映射', '03-有效性','0303-日期有效性',
                            f'检查时间戳字段【{field_cn_name}】是否为26位格式', SYSTEM_CODE,
                            table_cn_name, table_name, field_cn_name,field_name,
                            '2025-02-21', length_valid_sql
                        ]
                        write_to_template(tgt_sheet, base_data_length)                    

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
    if len(sys.argv) != 4:
        print("Usage: python main.py <source_file> <target_file> <person_name>")
        sys.exit(1)
    source_file = sys.argv[1]
    target_file = sys.argv[2]
    person_name = sys.argv[3]

    new_file = copy_excel_template(target_file, person_name)
    clear_excel_data(new_file)
    copy_sheets_and_metadata(source_file, new_file)
    logger.info("Excel文件处理完成！")
