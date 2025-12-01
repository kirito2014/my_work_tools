# build_exe.py
"""
打包SQL依赖关系分析工具为exe
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def build_exe():
    """使用PyInstaller打包应用程序"""
    
    print("开始打包SQL依赖关系分析工具...")
    
    # 检查必要的文件
    required_files = [
        "sql_dependency_analyzer_gui.py",
        "sql_dependency_analyzer.py",
        "requirements.txt"
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            print(f"错误: 找不到文件 {file}")
            return False
    
    # 创建打包目录
    build_dir = "build"
    dist_dir = "dist"
    
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir)
    
    if os.path.exists(dist_dir):
        shutil.rmtree(dist_dir)
    
    # 确保有必要的图标文件
    icon_path = "icon.ico"
    if not os.path.exists(icon_path):
        print("警告: 未找到图标文件 icon.ico，将使用默认图标")
        icon_path = None
    
    # 构建PyInstaller命令
    cmd = [
        "pyinstaller",
        "--name=SQL依赖关系分析工具",
        "--windowed",
        "--onefile",
        "--clean",
        "--add-data", "sql_dependency_analyzer.py;.",
        "--hidden-import", "yaml",
        "--hidden-import", "pandas",
        "--hidden-import", "openpyxl",
        "--hidden-import", "dominate",
        "--hidden-import", "tkinter",
        "--hidden-import", "ttkthemes",
    ]
    
    if icon_path:
        cmd.extend(["--icon", icon_path])
    
    cmd.append("sql_dependency_analyzer_gui.py")
    
    print(f"执行命令: {' '.join(cmd)}")
    
    try:
        # 执行打包命令
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("打包成功！")
            print(f"可执行文件位于: {dist_dir}/")
            
            # 复制配置文件示例
            print("复制配置文件示例...")
            if os.path.exists("config.yaml"):
                shutil.copy("config.yaml", f"{dist_dir}/config.yaml.example")
            
            # 复制使用说明
            with open(f"{dist_dir}/使用说明.txt", "w", encoding="utf-8") as f:
                f.write("""SQL依赖关系分析工具 使用说明

1. 首次运行
   - 双击运行 "SQL依赖关系分析工具.exe"
   - 程序会在当前目录生成默认配置文件 config.yaml

2. 基本使用流程
   a. 在"配置"标签页加载或修改配置文件
   b. 在"生成"标签页选择SQL文件夹
   c. 配置输出选项
   d. 点击"开始分析"按钮

3. 配置文件说明
   - 可以编辑配置文件来自定义正则表达式、模板等
   - 支持动态加载和保存配置

4. 输出格式
   - Excel (.xlsx): 默认格式，支持样式
   - CSV (.csv): 纯文本格式
   - JSON (.json): 结构化数据
   - HTML (.html): 网页报告，美观易读

5. 依赖清单
   - 如果需要关联ETL作业信息，可以指定依赖清单文件
   - 支持Excel格式的依赖清单

6. 日志查看
   - 所有操作都会记录在日志中
   - 可以在"日志"标签页查看和保存日志

注意事项:
- 确保SQL文件使用UTF-8编码
- 对于大文件夹，分析可能需要一些时间
- 可以随时查看进度条和日志了解分析状态

如有问题，请检查日志文件或联系开发者。
""")
            
            print("打包完成！")
            return True
        else:
            print(f"打包失败: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("错误: 未找到PyInstaller，请先安装: pip install pyinstaller")
        return False
    except Exception as e:
        print(f"打包过程中发生错误: {e}")
        return False


if __name__ == "__main__":
    # 检查是否安装了PyInstaller
    try:
        import pyinstaller
        build_exe()
    except ImportError:
        print("错误: 请先安装PyInstaller")
        print("运行: pip install pyinstaller")
        sys.exit(1)