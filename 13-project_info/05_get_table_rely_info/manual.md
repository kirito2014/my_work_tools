# 生成基础依赖关系
python sql_dependency_analyzer.py /path/to/sql/scripts

# 生成完整依赖关系（包含作业编号）
python sql_dependency_analyzer.py /path/to/sql/scripts -d dependency_list.xlsx

# 生成CSV格式
python sql_dependency_analyzer.py /path/to/sql/scripts -f csv -o output.csv

# 使用自定义配置
python sql_dependency_analyzer.py /path/to/sql/scripts -c my_config.yaml