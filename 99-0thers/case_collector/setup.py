from distutils.core import setup
import py2exe
import os
import sys

sys.argv.append('py2exe')

# 获取res文件夹中的所有文件
data_files = []
for folder, subfolders, files in os.walk('res'):
    for file in files:
        data_files.append((folder, [os.path.join(folder, file)]))

setup(
    windows=['case_collector.py'],  # 如果是窗口程序则改为 windows=['your_script.py']
    options={
        'py2exe': {
            'packages': ['PIL', 'queue', 'tkinter', 'openpyxl', 'pandas'],  # 指定依赖库
            'includes': ['os', 'sys', 're', 'glob', 'threading','logging'],
        }
    },
    data_files=data_files  # 打包资源文件
)
