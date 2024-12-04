from pyecharts.charts import Line
from pyecharts import options as opts
from pyecharts.render import make_snapshot
import pandas as pd
import os
from datetime import datetime
from openpyxl import load_workbook
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM
import cairosvg

# 1. 加载 Excel 数据
excel_path = "AAAA.xlsx"  # 替换为您的文件路径
sheet_name = "成绩单"  # 替换为您的 Sheet 名称
wb = load_workbook(excel_path)
ws = wb[sheet_name]

# 获取数据
data = []
for row in ws.iter_rows(values_only=True):
    data.append(row)

# 转换为 DataFrame
header = data[0]  # 第一行为标题
content = data[1:]  # 其余行为内容
df = pd.DataFrame(content, columns=header)

# 2. 配置生成图片的文件夹
# 保存为 SVG 文件后转为 PNG
output_folder = "趋势图"
os.makedirs(output_folder, exist_ok=True)

for index, row in df.iterrows():
    name = row["姓名"]
    scores = row[1:].tolist()

    max_score = max(scores)
    min_score = min(scores)
    max_idx = scores.index(max_score)
    min_idx = scores.index(min_score)

    line = (
        Line()
        .add_xaxis(x_labels.tolist())
        .add_yaxis("分数", scores)
        .set_global_opts(
            title_opts=opts.TitleOpts(title=f"{name} 成绩趋势"),
            yaxis_opts=opts.AxisOpts(max_=100, min_=0),
        )
    )
    svg_path = os.path.join(output_folder, f"{name}_成绩单_趋势_{today}.svg")
    png_path = os.path.join(output_folder, f"{name}_成绩单_趋势_{today}.png")
    line.render(svg_path)

    # SVG 转 PNG
    cairosvg.svg2png(url=svg_path, write_to=png_path)

print(f"SVG 和 PNG 图已保存到 {output_folder}")