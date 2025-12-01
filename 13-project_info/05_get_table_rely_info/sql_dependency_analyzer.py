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
from dominate import document
from dominate.tags import *

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
        """从文件名提取基础表名，根据配置决定是否去除后缀"""
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
        
        # 根据配置决定是否去除后缀
        remove_suffix = self.config.get('processing', {}).get('remove_suffix', 'Y') == 'Y'
        if remove_suffix:
            suffix_identifier = self.config.get('processing', {}).get('suffix_identifier', '_PC')
            # 去除后缀标识符
            base_name = re.sub(f'{re.escape(suffix_identifier)}$', '', base_name, flags=re.IGNORECASE)
        
        return base_name

    def intelligent_filter_tables(self, source_file: str, table_names: List[str]) -> List[str]:
        """智能过滤表名，根据配置筛选掉指定的来源库"""
        filtered_tables = []
        source_base = self.get_base_table_name(source_file)
        
        # 获取配置的筛选值
        filter_schema = self.config.get('processing', {}).get('filter_schema', 'AGL')
        
        for table_name in table_names:
            if self._should_include_table(source_base, table_name, filter_schema):
                filtered_tables.append(table_name)
        
        return list(set(filtered_tables))

    def _should_include_table(self, source_base: str, table_name: str, filter_schema: str) -> bool:
        """判断是否应该包含该表，根据配置筛选掉指定的来源库"""
        table_clean = self._clean_table_name(table_name)
        
        # 排除自引用
        if (self.config.get('filters', {}).get('exclude_self_reference', True) and 
            source_base in table_clean):
            return False
        
        # 排除同层引用
        if (self.config.get('filters', {}).get('exclude_same_layer', True) and 
            self._is_same_layer(source_base, table_clean)):
            return False
        
        # 根据配置筛选掉指定的来源库
        if filter_schema and table_name.startswith(f"{filter_schema}."):
            return False
        
        # 排除模式
        exclude_patterns = self.config.get('filters', {}).get('exclude_patterns', [])
        for pattern in exclude_patterns:
            if re.search(pattern, table_clean, re.IGNORECASE):
                return False
        
        # 包含模式（如果有定义）
        include_patterns = self.config.get('filters', {}).get('include_patterns', [])
        if include_patterns:
            for pattern in include_patterns:
                if re.search(pattern, table_clean, re.IGNORECASE):
                    return True
            return False
        
        return True
    
    def _clean_table_name(self, table_name: str) -> str:
        """清理表名"""
        cleaned = table_name
        cleanup_patterns = self.config['regex_patterns'].get('table_cleanup', [])
        for pattern in cleanup_patterns:
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
        
        # 修复：使用更精确的文件搜索方法，避免重复
        sql_files = self._find_sql_files(folder_path)
        
        total_files = len(sql_files)
        if total_files == 0:
            logger.warning(f"在文件夹 {folder_path} 中未找到SQL文件")
            return data
        
        logger.info(f"找到 {total_files} 个SQL文件，开始分析...")
        
        for i, file_path in enumerate(sql_files, 1):
            try:
                file_data = self.process_single_file(file_path)
                data.extend(file_data)
                
                # 更新进度
                self._update_progress(i, total_files, "分析文件")
                
            except Exception as e:
                logger.error(f"处理文件 {file_path} 时出错: {e}")
        
        print()  # 进度条换行
        logger.info(f"分析完成，共处理 {len(data)} 条依赖关系")
        return data

    def _find_sql_files(self, folder_path: str) -> List[str]:
        """
        查找SQL文件，避免重复
        修复：使用不区分大小写的搜索，但去重
        """
        sql_files = set()  # 使用集合避免重复
        folder = Path(folder_path)
        
        # 首先尝试使用不区分大小写的搜索
        pattern = re.compile(r'.*\.(hql|sql)$', re.IGNORECASE)
        
        for file_path in folder.rglob('*'):
            if file_path.is_file() and pattern.match(file_path.name):
                # 使用规范化的路径来避免重复
                normalized_path = str(file_path.resolve())
                sql_files.add(normalized_path)
        
        # 转换为列表并排序
        result = sorted(list(sql_files))
        
        # 调试信息
        logger.debug(f"找到的SQL文件列表:")
        for i, file_path in enumerate(result, 1):
            logger.debug(f"  {i:3d}. {os.path.basename(file_path)}")
        
        return result
    
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
                    source_table_clean,                     # 清理后表名
                    file_name                               # 新增：文件名
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
        bar_length = self.config.get('progress', {}).get('bar_length', 30)
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
        
        # 确保输出文件扩展名与格式匹配
        output_file = self._ensure_correct_extension(output_file, format)
        
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
    
    def _ensure_correct_extension(self, output_file: str, format: str) -> str:
        """确保输出文件扩展名与格式匹配"""
        base_name = os.path.splitext(output_file)[0]
        
        extension_map = {
            'excel': '.xlsx',
            'csv': '.csv',
            'json': '.json',
            'html': '.html'
        }
        
        correct_extension = extension_map.get(format, '.xlsx')
        
        # 如果当前扩展名不正确，则修正
        current_extension = os.path.splitext(output_file)[1].lower()
        if current_extension != correct_extension:
            new_output_file = base_name + correct_extension
            logger.info(f"修正输出文件扩展名: {output_file} -> {new_output_file}")
            return new_output_file
        
        return output_file
    
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
            # 检查依赖文件是否存在
            if not os.path.exists(dependency_file):
                logger.warning(f"依赖文件不存在: {dependency_file}")
                return df
            
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
            
            # 重新排列列顺序，将新增列放在最后
            original_columns = [col['name'] for col in self.config['output']['basic_columns']]
            new_columns = original_columns + ['etl_job_no', 'etl_job_name']
            result_df = result_df[new_columns]
            
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
        logger.info(f"正在导出结果到CSV: {output_file}")
        
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        # CSV特定设置
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=',')
        logger.info(f"CSV文件已保存: {output_file}")
    
    def export_to_json(self, data: List[Tuple], output_file: str):
        """导出到JSON"""
        logger.info(f"正在导出结果到JSON: {output_file}")
        
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        # 转换为字典格式，处理特殊类型
        records = []
        for _, row in df.iterrows():
            record = {}
            for col in columns:
                value = row[col]
                # 处理NaN值
                if pd.isna(value):
                    record[col] = None
                else:
                    record[col] = str(value) if not isinstance(value, (str, int, float, bool)) else value
            records.append(record)
        
        # 写入JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        logger.info(f"JSON文件已保存: {output_file}")
    


    def export_to_html(self, data: List[Tuple], output_file: str):
        """导出到HTML - 使用Tailwind CSS美化"""
        logger.info(f"正在导出结果到HTML: {output_file}")
        
        columns = [col['name'] for col in self.config['output']['basic_columns']]
        df = pd.DataFrame(data, columns=columns)
        
        # 获取列标题映射
        column_titles = {col['name']: col['title'] for col in self.config['output']['basic_columns']}
        
        # 创建HTML文档
        doc = document(title='SQL依赖关系分析报告')
        
        with doc.head:
            meta(charset='UTF-8')
            meta(name='viewport', content='width=device-width, initial-scale=1.0')
            # 引入Tailwind CSS
            link(rel='stylesheet', href='https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css')
            style("""
                body {
                    font-family: 'Microsoft YaHei', Arial, sans-serif;
                }
                .table-container {
                    max-height: 70vh;
                    overflow: auto;
                }
                table th {
                    position: sticky;
                    top: 0;
                    z-index: 10;
                }
            """)
        
        generate_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        record_count = len(df)
        table_count = df['target_table'].nunique()
        
        with doc:
            # 主容器
            with div(cls='min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100 py-8 px-4'):
                # 内容卡片
                with div(cls='max-w-7xl mx-auto'):
                    # 标题区域
                    with div(cls='text-center mb-8'):
                        with div(cls='bg-white rounded-2xl shadow-lg p-8 mb-6'):
                            h1('📊 SQL依赖关系分析报告', 
                            cls='text-3xl font-bold text-gray-800 mb-4')
                            p('基于SQL/HQL脚本的自动化依赖关系分析', 
                            cls='text-lg text-gray-600 mb-6')
                            
                            # 统计信息卡片
                            with div(cls='grid grid-cols-1 md:grid-cols-3 gap-6'):
                                with div(cls='bg-blue-50 rounded-xl p-6 text-center border border-blue-200'):
                                    with div(cls='text-blue-600 mb-2'):
                                        span('📅', cls='text-2xl')
                                    h3(cls='text-sm font-semibold text-gray-600 mb-1')('生成时间')
                                    p(generate_time, cls='text-lg font-bold text-gray-800')
                                
                                with div(cls='bg-green-50 rounded-xl p-6 text-center border border-green-200'):
                                    with div(cls='text-green-600 mb-2'):
                                        span('📈', cls='text-2xl')
                                    h3(cls='text-sm font-semibold text-gray-600 mb-1')('总记录数')
                                    p(f'{record_count} 条', cls='text-lg font-bold text-gray-800')
                                
                                with div(cls='bg-purple-50 rounded-xl p-6 text-center border border-purple-200'):
                                    with div(cls='text-purple-600 mb-2'):
                                        span('🗂️', cls='text-2xl')
                                    h3(cls='text-sm font-semibold text-gray-600 mb-1')('数据表数量')
                                    p(f'{table_count} 个', cls='text-lg font-bold text-gray-800')
                    
                    # 数据表格区域
                    with div(cls='bg-white rounded-2xl shadow-lg overflow-hidden'):
                        # 表格标题栏
                        with div(cls='bg-gradient-to-r from-blue-600 to-indigo-700 px-6 py-4'):
                            with div(cls='flex justify-between items-center'):
                                h2('依赖关系明细', 
                                cls='text-xl font-bold text-white')
                                with div(cls='text-blue-100'):
                                    span(f'共 {len(df)} 条记录', 
                                        cls='text-sm bg-blue-500 bg-opacity-20 px-3 py-1 rounded-full')
                        
                        # 表格容器
                        with div(cls='p-6'):
                            with div(cls='table-container border border-gray-200 rounded-lg'):
                                # 创建表格
                                with table(cls='min-w-full divide-y divide-gray-200'):
                                    # 表头
                                    with thead(cls='bg-gray-50'):
                                        with tr():
                                            for col_config in self.config['output']['basic_columns']:
                                                th(col_config['title'], 
                                                cls='px-6 py-4 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider sticky top-0 bg-gray-50')
                                    
                                    # 表格数据
                                    with tbody(cls='bg-white divide-y divide-gray-200'):
                                        for i, (_, row) in enumerate(df.iterrows()):
                                            # 交替行颜色
                                            row_class = 'bg-white' if i % 2 == 0 else 'bg-gray-50'
                                            with tr(cls=f'{row_class} hover:bg-blue-50 transition-colors duration-150'):
                                                for col in columns:
                                                    value = row[col]
                                                    cell_class = 'px-6 py-4 whitespace-nowrap text-sm'
                                                    if pd.isna(value):
                                                        td('', cls=f'{cell_class} text-gray-400')
                                                    else:
                                                        # 对特定列添加特殊样式
                                                        if col == 'developer':
                                                            td(str(value), 
                                                            cls=f'{cell_class} text-purple-600 font-medium')
                                                        elif col == 'theme':
                                                            td(str(value), 
                                                            cls=f'{cell_class} text-blue-600 font-semibold')
                                                        else:
                                                            td(str(value), 
                                                            cls=f'{cell_class} text-gray-700')
                    
                    # 页脚
                    with div(cls='mt-8 text-center'):
                        with div(cls='bg-white rounded-xl shadow-sm p-6'):
                            with div(cls='flex flex-col md:flex-row justify-between items-center text-sm text-gray-600'):
                                with div(cls='mb-4 md:mb-0'):
                                    span('🔧 SQL依赖关系分析工具', 
                                        cls='font-semibold text-gray-700')
                                    span(' v2.0', cls='text-blue-600')
                                with div(cls='flex items-center space-x-6'):
                                    with div(cls='flex items-center space-x-2'):
                                        span('🕒', cls='text-lg')
                                        span(f'生成时间: {generate_time}')
                                    with div(cls='flex items-center space-x-2'):
                                        span('⚡', cls='text-lg')
                                        span('Powered by Python & Tailwind CSS')
            
            # 添加一些交互效果
            script("""
                // 添加表格行点击效果
                document.addEventListener('DOMContentLoaded', function() {
                    const rows = document.querySelectorAll('tbody tr');
                    rows.forEach(row => {
                        row.addEventListener('click', function() {
                            this.classList.toggle('bg-yellow-50');
                        });
                    });
                    
                    // 添加打印按钮功能
                    const printButton = document.createElement('button');
                    printButton.innerHTML = '🖨️ 打印报告';
                    printButton.className = 'fixed bottom-6 right-6 bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-full shadow-lg transition-all duration-200 transform hover:scale-105';
                    printButton.onclick = () => window.print();
                    document.body.appendChild(printButton);
                });
            """)
        
        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(doc.render())
        
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
  
  # 显示详细日志（包含文件列表）
  python sql_dependency_analyzer.py /path/to/sql/scripts -v
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
                       help='显示详细日志信息（包含找到的文件列表）')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("启用详细日志模式")
    
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