import os
import sys
import argparse
import logging
import configparser
import pandas as pd
from openpyxl import load_workbook

# ==========================================
# 1. 初始化日志系统
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

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
                
            prefix = f[:5].lower() 
            if prefix in target_prefixes:
                file_path = os.path.join(self.source_dir, f)
                logger.info(f"正在加载源文件到内存: {f}")
                try:
                    sheets_dict = pd.read_excel(file_path, sheet_name=None, header=self.get_header_row_index())
                    self.source_data_cache[prefix] = sheets_dict
                    loaded_count += 1
                except Exception as e:
                    logger.error(f"读取文件失败 {f}: {str(e)}")
                    sys.exit(1) 
                    
        if loaded_count == 0:
            logger.error("未在指定文件夹中找到任何匹配前缀 (01stg, 02dwd...) 的源文件！")
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
                    logger.error(f"【阻断错误】无法处理目标表层级 '{level_short}'！")
                    logger.error(f"原因：在源文件夹或配置文件中未找到对应的 '{target_prefix or '未知前缀'}' 文件或配置。")
                    logger.error("程序已强行中止，请检查输入文件及配置文件。")
                    sys.exit(1)
                    
                required_sheets = self.config[target_prefix]
                file_data = self.source_data_cache[target_prefix]
                
                # 遍历所有的已知检查项，如果该检查项不在当前层级的 ini 配置中，默认填入 "/"
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
                
                for sheet_name in required_sheets:
                    col_idx = CHECK_COLUMN_MAPPING.get(sheet_name)
                    if not col_idx:
                        continue 

                    logger.info(f"  检查事项: {sheet_name}")

                    if sheet_name not in file_data:
                        logger.warning(f"  缺失 Sheet 页: {sheet_name}。标记为：检查来源数据是否存在")
                        ws.cell(row=row, column=col_idx).value = "检查来源数据是否存在"
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
                        ws.cell(row=row, column=col_idx).value = "/"
                        continue
                        
                    check_result = "Y"
                    all_issue_types = []
                    
                    # ==========================================
                    # 【逻辑优化】：剥离未分析与不通过的判定关系
                    # ==========================================
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
                                check_result = "N" # 只要有异常，此列对应检查项即为N
                                if not itype:
                                    # 纯空值：只计入待分析，不计入问题总数
                                    stats["un_analyzed"] += 1
                                else:
                                    # 有问题类型但没填结果：按不通过兜底
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
                                # 纯空值：只计入待分析，不计入问题总数
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
                
                # ==========================================
                # 【逻辑优化】：Z列状态的严格判定
                # ==========================================
                if stats["total_cases"] > 0:
                    if stats["fail_issues"] == 0 and stats["un_analyzed"] == 0:
                        ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "通过"
                    elif stats["fail_issues"] > 0 or stats["un_analyzed"] > 0:
                        ws.cell(row=row, column=STATS_COLUMNS["测试状态"]).value = "不通过"
                    
            logger.info("==================================================")
            logger.info("所有数据处理完毕，正在保存目标结果文件...")
            wb.save(self.target_file)
            logger.info("处理完成报告：成功生成统计结果！")

        except SystemExit:
            pass
        except Exception as e:
            logger.error(f"处理过程中发生严重错误: {str(e)}", exc_info=True)


if __name__ == "__main__":
    # 命令行参数解析
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
    