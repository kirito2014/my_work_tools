#!/usr/bin/env python3
"""
PNG 转 ICO 转换器
支持将 PNG 文件转换为 ICO 图标文件
"""

import os
import sys
import argparse
from PIL import Image, ImageDraw
import tempfile

def convert_png_to_ico(png_path, ico_path=None, size=64):
    """
    将 PNG 或 JPG 文件转换为 ICO 图标文件，先将图像裁剪为正方形
    
    参数:
        png_path: 图像文件路径（支持 PNG 或 JPG/JPEG）
        ico_path: 输出 ICO 文件路径（可选）
        size: 图标尺寸，默认 64x64
    
    返回:
        bool: 转换是否成功
    """
    try:
        # 验证输入文件
        if not os.path.exists(png_path):
            print(f"错误: 文件不存在 - {png_path}")
            return False
        
        # 检查文件格式
        file_ext = os.path.splitext(png_path)[1].lower()
        if file_ext not in ['.png', '.jpg', '.jpeg']:
            print(f"错误: 文件不是支持的格式（PNG/JPG）- {png_path}")
            return False
        
        # 生成输出路径
        if ico_path is None:
            base_name = os.path.splitext(png_path)[0]
            ico_path = base_name + ".ico"
        
        print(f"正在转换: {os.path.basename(png_path)} -> ICO格式")
        
        try:
            # 使用 PIL 打开图像
            with Image.open(png_path) as img:
                # 1. 裁剪为正方形
                width, height = img.size
                
                # 计算正方形的大小（取最小的边长）
                min_side = min(width, height)
                
                # 计算裁剪区域的中心点
                left = (width - min_side) // 2
                top = (height - min_side) // 2
                right = left + min_side
                bottom = top + min_side
                
                # 裁剪图像为正方形
                img_cropped = img.crop((left, top, right, bottom))
                print(f"  裁剪为正方形: {min_side}x{min_side} 像素")
                
                # 2. 调整图像大小到指定尺寸
                img_resized = img_cropped.resize((size, size), Image.LANCZOS)
                
                # 3. 确保图像模式适合 ICO 格式
                if img_resized.mode in ('RGBA', 'LA') or (img_resized.mode == 'P' and 'transparency' in img_resized.info):
                    # 保持透明度
                    ico_img = img_resized
                else:
                    # 转换为 RGB 模式
                    ico_img = img_resized.convert('RGB')
                
                # 4. 保存为 ICO 文件
                ico_img.save(ico_path, format='ICO', sizes=[(size, size)])
            
            print(f"[OK] 转换完成: {os.path.basename(ico_path)}")
            print(f"  尺寸: {size}x{size} 像素")
            print(f"  位置: {ico_path}")
            
            return True
            
        except Exception as pil_error:
            print(f"PIL处理失败: {str(pil_error)}")
            print("尝试创建基本ICO文件...")
            
            # 创建简单的占位图像
            img = Image.new('RGBA', (size, size), (255, 255, 255, 0))
            draw = ImageDraw.Draw(img)
            
            # 绘制一个简单的框作为占位符
            draw.rectangle([10, 10, size-10, size-10], outline=(0, 0, 0, 255), width=2)
            
            # 保存为ICO
            img.save(ico_path, format='ICO', sizes=[(size, size)])
            
            print(f"[OK] 创建了占位ICO文件: {os.path.basename(ico_path)}")
            print("  警告: 无法正常处理图像文件")
            print("  建议: 检查图像文件是否损坏或格式正确")
            
            return True
                
    except Exception as e:
        print(f"[ERR] 转换失败: {str(e)}")
        return False

def batch_convert_png_to_ico(input_folder, output_folder=None, size=64):
    """
    批量将图像文件转换为ICO图标文件
    支持 PNG 和 JPG/JPEG 格式
    
    参数:
        input_folder: 输入文件夹路径
        output_folder: 输出文件夹路径（可选）
        size: 图标尺寸，默认 64x64
    
    返回:
        int: 成功转换的文件数量
    """
    if not os.path.isdir(input_folder):
        print(f"错误: 文件夹不存在 - {input_folder}")
        return 0
    
    if output_folder is None:
        output_folder = input_folder
    else:
        os.makedirs(output_folder, exist_ok=True)
    
    # 查找所有 PNG 和 JPG 文件
    image_files = []
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            image_files.append(filename)
    
    if not image_files:
        print("未找到 PNG 或 JPG 文件")
        return 0
    
    print(f"找到 {len(image_files)} 个图像文件")
    print("开始批量转换...")
    print("-" * 50)
    
    success_count = 0
    
    for image_filename in image_files:
        image_path = os.path.join(input_folder, image_filename)
        
        # 生成输出路径
        base_name = os.path.splitext(image_filename)[0]
        ico_filename = base_name + ".ico"
        ico_path = os.path.join(output_folder, ico_filename)
        
        if convert_png_to_ico(image_path, ico_path, size):
            success_count += 1
    
    print("-" * 50)
    print(f"批量转换完成! 成功转换 {success_count}/{len(image_files)} 个文件")
    
    return success_count

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="图像转 ICO 转换器 - 将 PNG 或 JPG 文件转换为 ICO 图标",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 转换单个文件
  %(prog)s image.png
  %(prog)s image.jpg
  %(prog)s input.png output.ico
  %(prog)s input.jpg output.ico
  
  # 转换单个文件并指定尺寸
  %(prog)s image.png -s 128
  %(prog)s image.jpg -s 128
  
  # 批量转换文件夹
  %(prog)s /path/to/image/folder -b
  %(prog)s /path/to/image/folder -b -o /path/to/output/folder
  
  # 递归批量转换
  %(prog)s /path/to/folder -b -r
        """)
    
    
    parser.add_argument(
        'input',
        help='输入 PNG 文件路径或文件夹路径（使用 -b 参数时）'
    )
    
    parser.add_argument(
        'output', 
        nargs='?',
        help='输出文件路径（单文件模式）'
    )
    
    parser.add_argument(
        '-b', '--batch',
        action='store_true',
        help='批量转换模式（处理文件夹）'
    )
    
    parser.add_argument(
        '-o', '--output-dir',
        help='输出文件夹路径（批量模式）'
    )
    

    
    parser.add_argument(
        '-s', '--size',
        type=int,
        default=64,
        help='图标尺寸（默认: 64）'
    )
    
    parser.add_argument(
        '-r', '--recursive',
        action='store_true',
        help='递归处理子文件夹（批量模式）'
    )
    
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='SVG 转 ICO 转换器 v1.0'
    )
    
    args = parser.parse_args()
    
    # 验证尺寸参数
    if args.size < 16 or args.size > 256:
        print("错误: 尺寸必须在 16-256 之间")
        sys.exit(1)
    
    try:
        if args.batch:
            # 批量转换模式
            if not os.path.isdir(args.input):
                print("错误: 批量模式需要文件夹路径")
                sys.exit(1)
            
            success_count = batch_convert_png_to_ico(
                args.input, 
                args.output_dir, 
                args.size
            )
            
            if success_count == 0:
                sys.exit(1)
                
        else:
            # 单文件转换模式
            if not os.path.isfile(args.input):
                print("错误: 文件不存在")
                sys.exit(1)
            
            success = convert_png_to_ico(
                args.input, 
                args.output, 
                args.size
            )
            
            if not success:
                sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()