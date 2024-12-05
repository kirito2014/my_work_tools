# 成绩单趋势生成工具

## 功能
- 支持通过图形界面生成成绩趋势折线图。
- 平滑曲线、带最大值/最小值标注。
- 生成结果保存为 HTML 文件。

## 使用方法
1. 启动工具，选择 Excel 文件并选择 Sheet 页。
2. 点击“执行生成”，工具将生成图表。



## 1. 安装 Python（如果尚未安装）
Python 是运行此工具的必要条件。
确认你已安装 Python。可以在命令行中运行以下命令检查：

 
```bash
python --version
```
如果没有安装，请访问 Python 官网 下载并安装。

## 2. 安装 virtualenv（如果尚未安装）
虽然 Python 3.3 及以上版本内置了 venv 模块，但你也可以选择安装 virtualenv 来创建虚拟环境：

```bash
pip install virtualenv
```
## 3. 创建虚拟环境
在你希望存放虚拟环境的文件夹内，运行以下命令创建一个新的虚拟环境（env 是虚拟环境的名字，可以根据需要修改）：

```bash
python -m venv env
```
或者使用 virtualenv：

```bash
virtualenv env
```
## 4. 激活虚拟环境
创建完虚拟环境后，运行以下命令激活它：

```bash
.\env\Scripts\activate
```
激活后，命令行提示符会变成 (env)，表示你当前处于虚拟环境中。

## 5. 安装项目依赖
在虚拟环境中，使用以下命令安装依赖（例如，你的项目有一个 requirements.txt 文件）：

```bash
pip install -r requirements.txt
```
## 6. 运行项目
虚拟环境激活后，你可以运行你的 Python 脚本。例如：

```bash
python trend_chart_tool.py
```
## 7. 退出虚拟环境
工作完成后，你可以使用以下命令退出虚拟环境：

```bash
deactivate
```
## 8. 打包项目（可选）
如果你希望将项目打包为 EXE 文件，可以使用 pyinstaller。首先安装 pyinstaller：

```bash
pip install pyinstaller
```
然后运行以下命令打包：

```bash
pyinstaller --noconfirm --onefile --windowed   --collect-all pyecharts --upx-dir "path\to\upx"  --icon="tools.ico"   .\gen_grade_ui.py
```
