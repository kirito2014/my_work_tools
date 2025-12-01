# SQL依赖关系分析工具 GUI版

## 功能特点

1. **用户友好界面**: 使用tkinter和ttkthemes创建的现代化界面
2. **配置管理**: 支持加载、编辑和保存配置文件
3. **多种输出格式**: 支持Excel、CSV、JSON和HTML格式
4. **实时进度显示**: 显示分析进度和状态
5. **详细日志**: 完整的日志记录和查看功能
6. **依赖关联**: 支持关联ETL作业信息

## 安装要求

### Python环境要求
- Python 3.7+
- 必要的Python包 (使用requirements.txt安装)

### 安装步骤

1. 安装Python依赖:
```bash
pip install -r requirements.txt
```
2. 手动打包:

```bash
# 打包应用程序
pyinstaller --name="SQL依赖关系分析工具" --windowed --onefile --clean --add-data "sql_dependency_analyzer.py;." --hidden-import yaml --hidden-import pandas --hidden-import openpyxl --hidden-import dominate --hidden-import tkinter --hidden-import ttkthemes sql_dependency_analyzer_gui.py
```

3.程序打包:
```bash
python build_exe.py
```
#### 配置文件说明

```
# 项目配置
projects:
  DEFAULT:
    prefix: ''
    theme: '通用'
    description: '通用项目'

# 文件模板
file_templates:
  default:
    name: '默认模板'
    lines:
      table_name: 8
      developer: 14
    delimiter: ':'
    file_pattern: '.*\\.(hql|sql)$'

# 正则表达式模式
regex_patterns:
  table_reference:
    - '(?:FROM|JOIN)\s+(\w+\.\w+)\s+'
    - '(?:INSERT\s+INTO|INSERT\s+OVERWRITE)\s+(\w+\.\w+)\b'
  file_extension: '\\.(hql|sql)$'

# 输出配置
output:
  basic_columns:
    - name: 'theme'
      title: '主题领域'
      width: 12
    - name: 'target_table'
      title: '目标表名'
      width: 35
```

#### 命令行版本命令
``` bash
# 生成基础依赖关系
python sql_dependency_analyzer.py /path/to/sql/scripts

# 生成完整依赖关系（包含作业编号）
python sql_dependency_analyzer.py /path/to/sql/scripts -d dependency_list.xlsx

# 生成CSV格式
python sql_dependency_analyzer.py /path/to/sql/scripts -f csv -o output.csv

# 使用自定义配置
python sql_dependency_analyzer.py /path/to/sql/scripts -c my_config.yaml
```
