# -*- coding: utf-8 -*-

"""
==============================================================================
Script Name  : gather_case_list.py
Description  : 映射测试案例数据统计自动化脚本
               
               本脚本用于自动化处理“SIT2阶段-明细进度”的测试结果统计。
               通过读取 config.ini 配置文件，脚本会自动遍历并预加载源数据目录
               下的测试案例文件（如 01stg_xxx.xlsx, 02dwd_xxx.xlsx），根据层级
               和表名精准提取测试用例状态，并计算不通过数、待分析数等关键指标，
               最终将统计结果和整体测试状态（通过/不通过）回写至目标结果表中。

Key Features :
    1. 全局预加载缓存机制，彻底规避高频磁盘 I/O，提升处理性能。
    2. 智能前缀模糊匹配（支持 01stg, 04dm 等变体前缀的动态识别）。
    3. 严格的防漏测机制：源文件缺失、Sheet 缺失或用例缺失均触发一票否决（不通过）。
    4. 自动双向日志记录系统，生成按时间戳命名的 log 文件便于追溯与排查。
    5. 安全无损的文件保存机制（基于 openpyxl，不破坏目标表格原生格式与宏）。

Dependencies : pip install pandas openpyxl
Usage        : python data_analyzer.py -s <源数据目录> -t <目标文件路径> [-c <配置文件>]
Example      : python data_analyzer.py -s ./source_data -t ./result.xlsx -c config.ini

Author       : [wangmujun / Sunline.Ltd]
Date         : 2026-04-10
Version      : 1.0.0
==============================================================================
"""

import os
import sys
import argparse
import logging
import datetime
import configparser
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, Border, Side, Alignment

# ==========================================
# 1. 初始化日志系统 (双向输出：控制台 + 文件)
# ==========================================
def setup_logger():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = datetime.datetime.now().strftime("logs_%Y%m%d%H%M%S.log")
    log_path = os.path.join(log_dir, log_filename)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__), log_path

logger, current_log_path = setup_logger()

# ==========================================
# 2. 全局配置与映射字典
# ==========================================
CHECK_COLUMN_MAPPING = {
    "作业跑通性检查": 9,      # I列: 跑通性检查
    "文件记录数检查": 10,     # J列: 文件与表记录检查
    "文件字段个数检查": 11,   # K列: 字段个数检查
    "新老记录数对比检查": 12, # L列: 新老记录数检查
    "上下游数据量对比检查": 13, # M列: 上下游记录数检查
    "表记录为空检查": 14,     # N列: 记录数非空检查
    "主键唯一性检查": 15,     # O列: 主键唯一性检查
    "主键非空检查": 16,       # P列: 主键非空检查
    "外键完整性检查": 17,     # Q列: 外键完整性检查
    "枚举值检查": 18,         # R列: 枚举值检查
    "全字段比对检查": 19,     # S列: 全字段映射检查
    "自定义": 20              # T列: 自定义
}

STATS_COLUMNS = {
    "用例总数": 21,         # U列
    "不通过问题总数": 22,   # V列
    "待分析条数": 23,       # W列
    "核心问题总数": 24,     # X列
    "平台问题总数": 25,     # Y列
    "测试状态": 26          # Z列
}

COL_TARGET_TABLE = "目标表名"
COL_TEST_RESULT = "测试结果"
COL_ISSUE_TYPE = "问题类型"
COL_ISSUE_DESC = "问题描述"


class DataAnalyzer:
    def __init__(self, source_dir, target_file, config_file='config.ini'):
        self.source_dir = source_dir
        self.target_file = target_file
        self.config_file = config_file
        self.config = {}
        self.source_data_cache = {}  
        self.level_mapping = {} 
        self.skipped_tasks = [] 
        
    def load_config(self):
        """读取 INI 配置文件并建立层级名称映射"""
        if not os.path.exists(self.config_file):
            logger.error(f"配置文件缺失: {self.config_file}")
            sys.exit(1)
            
        parser = configparser.ConfigParser()
        parser.read(self.config_file, encoding='utf-8')
        
        if 'SHEET_MAPPING' not in parser:
            logger.error("配置文件中缺少 [SHEET_MAPPING] 节点")
            sys.exit(1)
            
        for key in parser['SHEET_MAPPING']:
            clean_key = key.lower()
            self.config[clean_key] = [s.strip() for s in parser['SHEET_MAPPING'][key].split(',')]
            
            if len(clean_key) > 2:
                short_level = clean_key[2:] 
                self.level_mapping[short_level] = clean_key
                
        logger.info(f"配置文件加载成功，生成的层级映射为: {self.level_mapping}")

    def load_source_files(self):
        """遍历并缓存待处理的源文件数据"""
        target_prefixes = ["01stg", "02dwd", "03dws", "04dm"]
        
        if not os.path.isdir(self.source_dir):
            logger.error(f"待处理文件夹路径不存在: {self.source_dir}")
            sys.exit(1)

        files = os.listdir(self.source_dir)
        loaded_count = 0
        for f in files:
            if not f.endswith('.xlsx') or f.startswith('~'):
                continue
                
            f_lower = f.lower()
            matched_prefix = None
            
            # ==========================================
            # 【逻辑优化】：改用 startswith 动态匹配前缀
            # 不再局限于固定 5 位，完美兼容 "04dm" 这种 4 位的情况
            # ==========================================
            for tp in target_prefixes:
                if f_lower.startswith(tp):
                    matched_prefix = tp
                    break
            
            if matched_prefix:
                file_path = os.path.join(self.source_dir, f)
                logger.info(f"正在加载源文件到内存: {f} (识别前缀: {matched_prefix})")
                try:
                    sheets_dict = pd.read_excel(file_path, sheet_name=None, header=self.get_header_row_index())
                    self.source_data_cache[matched_prefix] = sheets_dict
                    loaded_count += 1
                except Exception as e:
                    logger.error(f"读取文件失败 {f}: {str(e)}")
                    sys.exit(1) 
                    
        if loaded_count == 0:
            logger.error("未在指定文件夹中找到任何匹配前缀 (01stg, 02dwd, 03dws, 04dm) 的源文件！")
            sys.exit(1)

    def get_header_row_index(self):
        return 0

    def process_data(self):
        """核心数据处理流程"""
        try:
            logger.info(f"正在加载目标结果文件: {self.target_file}")
            wb = load_workbook(self.target_file)
            if "SIT2阶段-明细进度" not in wb.sheetnames:
                logger.error("目标文件中未找到 sheet: SIT2阶段-明细进度")
                sys.exit(1)
                
            ws = wb["SIT2阶段-明细进度"]
            max_row = ws.max_row
            
            for row in range(2, max_row + 1):
                level_val = ws.cell(row=row, column=2).value 
                table_name_val = ws.cell(row=row, column=4).value 
                
                if not level_val or not table_name_val:
                    continue
                    
                level_short = str(level_val).strip().lower()
                table_name = str(table_name_val).strip().upper()
                
                logger.info(f"--------------------------------------------------")
                logger.info(f"当前处理 -> 目标表层级: {level_short}, 表名: {table_name}")

                target_prefix = self.level_mapping.get(level_short)
                
                if not target_prefix or target_prefix not in self.config or target_prefix not in self.source_data_cache:
                    logger.warning(f"  [异常] 无法处理层级 '{level_short}'。已全项标记为'无原数据'，状态置为'不通过'。")
                    self.skipped_tasks.append({
                        "row": row,
                        "level": level_short,
                        "table": table_name,
                        "reason": f"层级 '{level_short}' 缺少配置或对应源文件"
                    })
                    
                    for col_idx in CHECK_COLUMN_MAPPING.values():
                        ws.cell(row=row, column=col_idx).value = "无原数据"
                        
                    ws.cell(row=row, column=STATS_COLUMNS["用例总数"]).value = 0
                    ws.cell(row=row, column=STATS_COLUMNS["不通过问题总数"]).value = 0
                    ws.cell(row=row, column=STATS_COLUMNS["待分析条数"]).value = 0
                    ws.cell(row=row, column=STATS_COLUMNS["核心问题总数"]).value = 0
                    ws.cell(row=row, column=STATS_COLUMNS["平台问题总数"]).value = 0
                    
                    ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "不通过"
                    
                    continue 
                    
                required_sheets = self.config[target_prefix]
                file_data = self.source_data_cache[target_prefix]
                
                for check_name, col_idx in CHECK_COLUMN_MAPPING.items():
                    if check_name not in required_sheets:
                        ws.cell(row=row, column=col_idx).value = "/"
                
                stats = {
                    "total_cases": 0,
                    "fail_issues": 0,
                    "un_analyzed": 0,
                    "core_issues": 0,
                    "platform_issues": 0
                }
                
                has_missing_case = False
                
                for sheet_name in required_sheets:
                    col_idx = CHECK_COLUMN_MAPPING.get(sheet_name)
                    if not col_idx:
                        continue 

                    logger.info(f"  检查事项: {sheet_name}")

                    if sheet_name not in file_data:
                        logger.warning(f"  缺失 Sheet 页: {sheet_name}。标记为：检查来源数据是否存在")
                        ws.cell(row=row, column=col_idx).value = "检查来源数据是否存在"
                        has_missing_case = True  
                        continue

                    df = file_data[sheet_name]
                    
                    required_cols = [COL_TARGET_TABLE, COL_TEST_RESULT, COL_ISSUE_TYPE, COL_ISSUE_DESC]
                    missing_cols = [c for c in required_cols if c not in df.columns]
                    if missing_cols:
                        logger.error(f"  Sheet '{sheet_name}' 缺失必要字段: {missing_cols}")
                        ws.cell(row=row, column=col_idx).value = "/"
                        continue

                    df = df.fillna("")
                    df[COL_TARGET_TABLE] = df[COL_TARGET_TABLE].astype(str).str.strip().str.upper()
                    
                    target_data = df[df[COL_TARGET_TABLE] == table_name]
                    records_count = len(target_data)
                    stats["total_cases"] += records_count
                    
                    logger.info(f"  匹配条数: {records_count} 条记录")

                    if records_count == 0:
                        ws.cell(row=row, column=col_idx).value = "无对应检查案例"
                        has_missing_case = True  
                        continue
                        
                    check_result = "Y"
                    all_issue_types = []
                    
                    if records_count > 1:
                        for _, record in target_data.iterrows():
                            res = str(record[COL_TEST_RESULT]).strip()
                            itype = str(record[COL_ISSUE_TYPE]).strip()
                            
                            if res == "不通过":
                                check_result = "N"
                                stats["fail_issues"] += 1
                                if itype:
                                    all_issue_types.append(itype)
                            elif res == "":
                                check_result = "N" 
                                if not itype:
                                    stats["un_analyzed"] += 1
                                else:
                                    stats["fail_issues"] += 1
                                    all_issue_types.append(itype)
                            
                            if itype == "核心问题": stats["core_issues"] += 1
                            elif itype == "平台问题": stats["platform_issues"] += 1

                    elif records_count == 1:
                        record = target_data.iloc[0]
                        res = str(record[COL_TEST_RESULT]).strip()
                        itype = str(record[COL_ISSUE_TYPE]).strip()
                        
                        if res == "不通过":
                            check_result = "N"
                            stats["fail_issues"] += 1
                            if itype:
                                all_issue_types.append(itype)
                        elif res == "":
                            if not itype:
                                check_result = "未分析"
                                stats["un_analyzed"] += 1
                            else:
                                check_result = "N"
                                stats["fail_issues"] += 1
                                all_issue_types.append(itype)
                                
                        if itype == "核心问题": stats["core_issues"] += 1
                        elif itype == "平台问题": stats["platform_issues"] += 1
                    
                    ws.cell(row=row, column=col_idx).value = check_result

                ws.cell(row=row, column=STATS_COLUMNS["用例总数"]).value = stats["total_cases"]
                ws.cell(row=row, column=STATS_COLUMNS["不通过问题总数"]).value = stats["fail_issues"]
                ws.cell(row=row, column=STATS_COLUMNS["待分析条数"]).value = stats["un_analyzed"]
                ws.cell(row=row, column=STATS_COLUMNS["核心问题总数"]).value = stats["core_issues"]
                ws.cell(row=row, column=STATS_COLUMNS["平台问题总数"]).value = stats["platform_issues"]
                
                if has_missing_case:
                    ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "不通过"
                elif stats["total_cases"] > 0:
                    if stats["fail_issues"] == 0 and stats["un_analyzed"] == 0:
                        ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "通过"
                    elif stats["fail_issues"] > 0 or stats["un_analyzed"] > 0:
                        ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "不通过"
                    
            logger.info("==================================================")
            logger.info("所有数据处理完毕，正在保存目标结果文件...")
            wb.save(self.target_file)
            
            logger.info("==================================================")
            logger.info("√  处理完成总结报告：")
            logger.info(f"▶ 结果已成功保存至: {self.target_file}")
            logger.info(f"▶ 详细日志已保存至: {current_log_path}")
            
            if self.skipped_tasks:
                logger.warning(f"▶ 注意：共有 {len(self.skipped_tasks)} 条任务由于原数据缺失被标记为'不通过'，详情如下：")
                for task in self.skipped_tasks:
                    logger.warning(f"    - Excel第 {task['row']} 行 | 表名: {task['table']} | 层级: {task['level']} | 原因: {task['reason']}")
            else:
                logger.info("▶ 完美：所有目标表任务均有对应的配置和源文件，无缺失任务。")

        except SystemExit:
            pass
        except Exception as e:
            logger.error(f"处理过程中发生严重错误: {str(e)}", exc_info=True)


if __name__ == "__main__":
    logger.info("脚本开始运行...")
    # parser = argparse.ArgumentParser(description="映射测试案例数据统计脚本")
    # parser.add_argument("-s", "--source", required=True, help="待处理文件夹路径（内含多个XLSX文件）")
    # parser.add_argument("-t", "--target", required=True, help="目标结果XLSX文件路径")
    # parser.add_argument("-c", "--config", default="config.ini", help="INI配置文件路径 (默认: config.ini)")
    
    # args = parser.parse_args()
    
    # analyzer = DataAnalyzer(
    #     source_dir=args.source,
    #     target_file=args.target,
    #     config_file=args.config
    # )
    
    # analyzer.load_config()
    # analyzer.load_source_files()
    # analyzer.process_data()
    #手动指定输入输出文件用于测试

    source_dir = "e:/github/my_work_tools/14-gather_case_list/single_file"
    target_file = "e:/github/my_work_tools/14-gather_case_list/output/浙商银行数据中台配合新核心改造项目_SIT测试进度跟踪.xlsx"
    config_file = "config.ini"
    
    analyzer = DataAnalyzer(
        source_dir=source_dir,
        target_file=target_file,
        config_file=config_file
    )
    
    analyzer.load_config()
    analyzer.load_source_files()
    analyzer.process_data()
    