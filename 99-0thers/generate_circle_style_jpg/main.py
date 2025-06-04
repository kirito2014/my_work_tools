from PIL import Image, ImageDraw
import numpy as np
import random

def add_color_variation(color, variation=20):
    """添加随机色差效果，使颜色在原色的基础上有所偏移"""
    r, g, b = color
    r = min(255, max(0, r + random.randint(-variation, variation)))
    g = min(255, max(0, g + random.randint(-variation, variation)))
    b = min(255, max(0, b + random.randint(-variation, variation)))
    return (r, g, b)

def add_grain(image, grain_strength=10):
    """添加颗粒效果：对每个像素进行小范围随机扰动"""
    np_image = np.array(image)
    noise = np.random.randint(-grain_strength, grain_strength, np_image.shape, dtype=np.int16)
    np_image = np.clip(np_image + noise, 0, 255)
    return Image.fromarray(np_image.astype(np.uint8))

def circle_pixelate(image_path, circle_size=10, circle_spacing=15, color_variation=20, grain_strength=10):
    # 打开图片
    img = Image.open(image_path)
    img = img.convert("RGB")
    
    # 获取图片宽度和高度
    width, height = img.size
    
    # 创建新的空白图片来保存结果
    result_img = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(result_img)
    
    # 遍历整个图片，以圆形区域进行填充
    for y in range(0, height, circle_spacing):
        for x in range(0, width, circle_spacing):
            # 获取当前区域的颜色平均值
            box = (x, y, x + circle_size, y + circle_size)
            region = img.crop(box)
            avg_color = np.array(region).mean(axis=(0, 1)).astype(int)
            
            # 为颜色添加色差
            avg_color = add_color_variation(avg_color, color_variation)
            
            # 计算圆心
            center_x, center_y = x + circle_size // 2, y + circle_size // 2
            # 画圆形
            draw.ellipse(
                [x, y, x + circle_size, y + circle_size], 
                fill=tuple(avg_color), 
                outline=None
            )
    
    # 添加颗粒效果
    result_img = add_grain(result_img, grain_strength)
    
    return result_img

# 使用示例
image_path = "th.jfif"  # 替换为你的图片路径
circle_size = 30  # 圆形像素的大小
circle_spacing = 1  # 圆形之间的间距
color_variation = 10  # 色差大小
grain_strength = 15  # 颗粒强度

# 生成圆形像素化效果的图片
pixelated_img = circle_pixelate(image_path, circle_size, circle_spacing, color_variation, grain_strength)

# 显示或保存结果
pixelated_img.show()  # 显示图片
pixelated_img.save("pixelated_with_grain_and_color_variation.jpg")  # 保存图片
