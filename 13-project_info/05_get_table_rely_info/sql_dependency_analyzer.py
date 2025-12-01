#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用SQL依赖关系分析工具
支持多种SQL方言和项目结构
"""

import os
import sys
import re
import argparse
import yaml
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from typing import List, Dict, Tuple, Optional, Any
from pathlib import Path
import json
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('dependency_analysis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        default_config = self._get_default_config()
        
        if not os.path.exists(self.config_path):
            logger.warning(f"配置文件 {self.config_path} 不存在，使用默认配置")
            return default_config
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                user_config = yaml.safe_load(f)
                return self._merge_configs(default_config, user_config)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}，使用默认配置")
            return default_config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'projects': {
                'DEFAULT': {
                    'prefix': '',
                    'theme': '通用',
                    'description': '通用项目'
                }
            },
            'file_templates': {
                'default': {
                    'name': '默认模板',
                    'lines': {'table_name': 8, 'developer': 14},
                    'delimiter': ':',
                    'file_pattern': '.*\\.(hql|sql)$'
                }
            },
            'regex_patterns': {
                'table_reference': [
                    r'(?:FROM|JOIN)\s+(\w+\.\w+)\s+',
                    r'(?:INSERT\s+INTO|INSERT\s+OVERWRITE)\s+(\w+\.\w+)\b'
                ],
                'file_extension': r'\.(hql|sql)$'
            },
            'output': {
                'basic_columns': [
                    {'name': 'theme', 'title': '主题领域', 'width': 12},
                    {'name': 'target_table', 'title': '目标表名', 'width': 35},
                    {'name': 'table_cn_name', 'title': '中文表名', 'width': 30},
                    {'name': 'developer', 'title': '开发人员', 'width': 12},
                    {'name': 'source_table', 'title': '来源表名', 'width': 35},
                    {'name': 'source_schema', 'title': '来源库名', 'width': 15},
                    {'name': 'source_table_clean', 'title': '清理后表名', 'width': 35}
                ]
            }
        }
    
    def _merge_configs(self, default: Dict, user: Dict) -> Dict:
        """合并默认配置和用户配置"""
        merged = default.copy()
        
        def deep_update(d, u):
            for k, v in u.items():
                if isinstance(v, dict) and k in d and isinstance(d[k], dict):
                    deep_update(d[k], v)
                else:
                    d[k] = v
        
        if user:
            deep_update(merged, user)
        return merged
    
    def get_project_theme(self, file_name: str) -> str:
        """根据文件名获取项目主题"""
        file_name_upper = file_name.upper()
        for project, config in self.config['projects'].items():
            prefix = config.get('prefix', '')
            if prefix and file_name_upper.startswith(prefix):
                return config['theme']
        return self.config['projects']['DEFAULT']['theme']
    
    def get_file_template(self, file_path: str) -> Dict[str, Any]:
        """根据文件路径获取适用的模板"""
        file_name = os.path.basename(file_path).lower()
        
        for template_name, template_config in self.config['file_templates'].items():
            if template_name == 'default':
                continue
            
            file_pattern = template_config.get('file_pattern', '')
            if file_pattern and re.search(file_pattern, file_name, re.IGNORECASE):
                return template_config
        
        return self.config['file_templates']['default']


class SQLDependencyAnalyzer:
    """SQL依赖关系分析器"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.config
        self.compiled_patterns = self._compile_patterns()
    
    def _compile_patterns(self) -> List[re.Pattern]:
        """编译正则表达式模式"""
        patterns = []
        for pattern in self.config['regex_patterns']['table_reference']:
            try:
                compiled = re.compile(pattern, re.IGNORECASE)
                patterns.append(compiled)
            except re.error as e:
                logger.warning(f"正则表达式编译失败 {pattern}: {e}")
        return patterns
    
    def extract_table_references(self, sql_content: str) -> List[str]:
        """从SQL内容中提取表引用"""
        all_matches = []
        
        for pattern in self.compiled_patterns:
            matches = pattern.findall(sql_content)
            # 处理分组匹配
            for match in matches:
                if isinstance(match, tuple):
                    # 取第一个非空分组
                    table_name = next((m for m in match if m), None)
                    if table_name:
                        all_matches.append(table_name)
                else:
                    all_matches.append(match)
        
        # 去重和清理
        cleaned_matches = list(set(
            self.normalize_table_name(match) 
            for match in all_matches 
            if match and '.' in match
        ))
        
        return cleaned_matches
    
    def normalize_table_name(self, table_name: str) -> str:
        """标准化表名"""
        # 移除空格和特殊字符
        cleaned = re.sub(r'\s+', '', table_name)
        # 统一大写
        return cleaned.upper()
    
    def extract_file_metadata(self, file_path: str) -> Dict[str, str]:
        """提取文件的元数据（表中文名、开发人员）"""
        metadata = {
            'table_cn_name': '表名未获取',
            'developer': '开发人员未获取'
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            
            template_config = self.config_manager.get_file_template(file_path)
            lines_config = template_config['lines']
            delimiter = template_config['delimiter']
            
            # 提取表中文名
            table_line_idx = lines_config['table_name']
            if len(lines) > table_line_idx:
                table_name = self._extract_from_line(lines[table_line_idx], delimiter)
                if table_name:
                    metadata['table_cn_name'] = table_name
            
            # 提取开发人员
            dev_line_idx = lines_config['developer']
            if len(lines) > dev_line_idx:
                developer = self._extract_from_line(lines[dev_line_idx], delimiter)
                if developer:
                    metadata['developer'] = developer
                    
        except UnicodeDecodeError:
            logger.error(f"文件编码错误: {file_path}")
        except Exception as e:
            logger.error(f"处理文件 {file_path} 失败: {e}")
        
        return metadata
    
    def _extract_from_line(self, line: str, delimiter: str) -> Optional[str]:
        """从行中提取信息"""
        if delimiter in line:
            parts = line.split(delimiter, 1)
            if len(parts) > 1:
                return parts[1].strip()
        return None
    
    def get_base_table_name(self, file_name: str) -> str:
        """从文件名提取基础表名"""
        file_upper = file_name.upper()
        
        # 移除扩展名
        base_name = re.sub(self.config['regex_patterns']['file_extension'], '', file_upper, flags=re.IGNORECASE)
        
        # 移除常见前缀
        patterns = [
            r'^ETL_PROC_',
            r'^PROC_',
            r'^CREATE_',
            r'^SQL_'
        ]
        
        for pattern in patterns:
            base_name = re.sub(pattern, '', base_name)
        
        return base_name
    
    def intelligent_filter_tables(self, source_file: str, table_names: List[str]) -> List[str]:
        """智能过滤表名"""
        filtered_tables = []
        source_base = self.get_base_table_name(source_file)
        
        for table_name in table_names:
            if self._should_include_table(source_base, table_name):
                filtered_tables.append(table_name)
        
        return list(set(filtered_tables))
    
    def _should_include_table(self, source_base: str, table_name: str) -> bool:
        """判断是否应该包含该表"""
        table_clean = self._clean_table_name(table_name)
        
        # 排除自引用
        if (self.config['filters']['exclude_self_reference'] and 
            source_base in table_clean):
            return False
        
        # 排除同层引用
        if (self.config['filters']['exclude_same_layer'] and 
            self._is_same_layer(source_base, table_clean)):
            return False
        
        # 排除模式
        for pattern in self.config['filters']['exclude_patterns']:
            if re.search(pattern, table_clean, re.IGNORECASE):
                return False
        
        # 包含模式（如果有定义）
        include_patterns = self.config['filters']['include_patterns']
        if include_patterns:
            for pattern in include_patterns:
                if re.search(pattern, table_clean, re.IGNORECASE):
                    return True
            return False
        
        return True
    
    def _clean_table_name(self, table_name: str) -> str:
        """清理表名"""
        cleaned = table_name
        for pattern in self.config['regex_patterns']['table_cleanup']:
            cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
        return cleaned
    
    def _is_same_layer(self, source_base: str, table_name: str) -> bool:
        """判断是否为同层引用"""
        source_project = self.config_manager.get_project_theme(source_base)
        table_project = self.config_manager.get_project_theme(table_name)
        return source_project == table_project
    
    def process_folder(self, folder_path: str) -> List[Tuple]:
        """处理文件夹中的所有SQL文件"""
        data = []
        
        # 获取所有SQL文件
        sql_files = []
        for ext in ['hql', 'sql', 'HQL', 'SQL']:
            sql_files.extend(Path(folder_path).glob(f"**/*.{ext}"))
        
        total_files = len(sql_files)
        if total_files == 0:
            logger.warning(f"在文件夹 {folder_path} 中未找到SQL文件")
            return data
        
        logger.info(f"找到 {total_files} 个SQL文件，开始分析...")
        
        for i, file_path in enumerate(sql_files, 1):
            try:
                file_data = self.process_single_file(str(file_path))
                data.extend(file_data)
                
                # 更新进度
                self._update_progress(i, total_files, "分析文件")
                
            except Exception as e:
                logger.error(f"处理文件 {file_path} 时出错: {e}")
        
        print()  # 进度条换行
        logger.info(f"分析完成，共处理 {len(data)} 条依赖关系")
        return data
    
    def process_single_file(self, file_path: str) -> List[Tuple]:
        """处理单个SQL文件"""
        data = []
        file_name = os.path.basename(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            # 替换变量
            sql_content = self._replace_variables(sql_content)
            
            # 提取表引用
            table_names = self.extract_table_references(sql_content)
            filtered_tables = self.intelligent_filter_tables(file_name, table_names)
            
            # 提取元数据
            metadata = self.extract_file_metadata(file_path)
            belong_theme = self.config_manager.get_project_theme(file_name)
            
            # 构建数据
            for table_name in filtered_tables:
                source_schema, source_table = table_name.split('.', 1)
                source_table_clean = self._clean_table_name(source_table)
                
                data.append((
                    belong_theme,                           # 主题领域
                    self.get_base_table_name(file_name),    # 目标表名
                    metadata['table_cn_name'],              # 中文表名
                    metadata['developer'],                  # 开发人员
                    table_name,                             # 来源表名
                    source_schema,                          # 来源库名
                    source_table_clean                      # 清理后表名
                ))
                
        except Exception as e:
            logger.error(f"处理文件 {file_path} 失败: {e}")
        
        return data
    
    def _replace_variables(self, sql_content: str) -> str:
        """替换SQL中的变量"""
        # 常见的变量替换
        variables = {
            '${version_num}': '',
            '${bizdate}': '20240101',
            '${env}': 'prod'
        }
        
        for var, replacement in variables.items():
            sql_content = sql_content.replace(var, replacement)
        
        return sql_content
    
    def _update_progress(self, current: int, total: int, prefix: str = "处理"):
        """更新进度条"""
        bar_length = self.config['progress']['bar_length']
        progress = current / total
        block = int(bar_length * progress)
        percentage = progress * 100
        
        bar = f"[{'#' * block}{'-' * (bar_length - block)}]"
        text = f"\r{prefix} | {bar} | {percentage:.1f}% ({current}/{total})"
        sys.stdout.write(text)
        sys.stdout.flush()


class ResultExporter:
    """结果导出器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    def export(self, data: List[Tuple], output_file: str, format: str = 'excel', 
               include_dependency: bool = False, dependency_file: str = None):
        """导出结果到文件"""
        format = format.lower()
        
        if format == 'excel':
            self.export_to_excel(data, output_file, include_dependency, dependency_file)
        elif format == 'csv':
            self.export_to_csv(data, output_file)
        elif format == 'json':
            self.export_to_json(data, output_file)
        elif format == 'html':
            self.export_to_html(data, output_file)
        else:
            raise ValueError(f"不支持的格式: {format}")
    
    def export_to_excel(self, data: List[Tuple], output_file: str, 
                       include_dependency: bool = False, dependency_file: str = None):
        """导出到Excel"""
        logger.info(f"正在导出结果到Excel: {output_file}")
        
        # 创建DataFrame
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        # 如果需要处理依赖关系
        if include_dependency and dependency_file:
            df = self._add_dependency_info(df, dependency_file)
        
        # 使用openpyxl引擎以支持样式
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='依赖关系', index=False)
            worksheet = writer.sheets['依赖关系']
            
            # 应用样式
            self._apply_excel_styles(worksheet, df)
        
        logger.info(f"Excel文件已保存: {output_file}")
    
    def _add_dependency_info(self, df: pd.DataFrame, dependency_file: str) -> pd.DataFrame:
        """添加依赖信息"""
        try:
            dependency_df = pd.read_excel(dependency_file, sheet_name=None)
            
            # 合并依赖信息
            merged_dfs = []
            for sheet_name, sheet_df in dependency_df.items():
                sheet_df['source_sheet'] = sheet_name
                merged_dfs.append(sheet_df)
            
            all_dependencies = pd.concat(merged_dfs, ignore_index=True)
            
            # 合并到主数据
            result_df = pd.merge(
                df, all_dependencies, 
                how='left', 
                left_on='source_table_clean', 
                right_on='etl_job',
                suffixes=('', '_dep')
            )
            
            # 添加作业信息
            result_df['etl_job_no'] = result_df['etl_system'].fillna('无对应作业编号')
            result_df['etl_job_name'] = 'IMP:' + result_df['etl_system'].fillna('') + '_' + result_df['etl_job'].fillna('')
            
            return result_df
            
        except Exception as e:
            logger.error(f"处理依赖文件失败: {e}")
            return df
    
    def _apply_excel_styles(self, worksheet, df):
        """应用Excel样式"""
        # 设置列宽
        for idx, col_config in enumerate(self.config['output']['basic_columns'], 1):
            col_letter = openpyxl.utils.get_column_letter(idx)
            worksheet.column_dimensions[col_letter].width = col_config['width']
        
        # 设置标题样式
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True)
        
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
    
    def export_to_csv(self, data: List[Tuple], output_file: str):
        """导出到CSV"""
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"CSV文件已保存: {output_file}")
    
    def export_to_json(self, data: List[Tuple], output_file: str):
        """导出到JSON"""
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        # 转换为字典格式
        records = df.to_dict('records')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON文件已保存: {output_file}")
    
    def export_to_html(self, data: List[Tuple], output_file: str):
        """导出到HTML"""
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>SQL依赖关系分析报告</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #366092; color: white; }
                tr:nth-child(even) { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h1>SQL依赖关系分析报告</h1>
        """ + df.to_html(index=False, escape=False) + """
        </body>
        </html>
        """
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML文件已保存: {output_file}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='通用SQL依赖关系分析工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 基础用法：生成来源表列表
  python sql_dependency_analyzer.py /path/to/sql/scripts
  
  # 生成完整依赖信息
  python sql_dependency_analyzer.py /path/to/sql/scripts -d dependency.xlsx
  
  # 指定输出格式和配置文件
  python sql_dependency_analyzer.py /path/to/sql/scripts -o output.csv -f csv -c my_config.yaml
        """
    )
    
    parser.add_argument('folder_path', help='SQL脚本文件夹路径')
    parser.add_argument('-o', '--output', default='dependency_analysis.xlsx', 
                       help='输出文件路径 (默认: dependency_analysis.xlsx)')
    parser.add_argument('-c', '--config', default='config.yaml',
                       help='配置文件路径 (默认: config.yaml)')
    parser.add_argument('-f', '--format', choices=['excel', 'csv', 'json', 'html'],
                       default='excel', help='输出格式 (默认: excel)')
    parser.add_argument('-d', '--dependency-file', 
                       help='依赖清单文件路径（用于生成完整依赖信息）')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='显示详细日志信息')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 检查文件夹是否存在
    if not os.path.exists(args.folder_path):
        logger.error(f"文件夹不存在: {args.folder_path}")
        sys.exit(1)
    
    try:
        # 初始化分析器
        analyzer = SQLDependencyAnalyzer(args.config)
        
        # 处理文件夹
        data = analyzer.process_folder(args.folder_path)
        
        if not data:
            logger.warning("未找到任何依赖关系数据")
            return
        
        # 导出结果
        exporter = ResultExporter(analyzer.config)
        include_dependency = bool(args.dependency_file)
        
        exporter.export(
            data, 
            args.output, 
            args.format, 
            include_dependency, 
            args.dependency_file
        )
        
        logger.info(f"分析完成！结果已保存到: {args.output}")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()