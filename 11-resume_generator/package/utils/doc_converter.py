#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
文档格式转换工具：将doc格式转换为docx格式
"""
import os
import sys
import comtypes.client
import time
from typing import Optional


def convert_doc_to_docx(doc_path: str, output_dir: Optional[str] = None) -> str:
    """
    将doc文件转换为docx文件
    :param doc_path: doc文件路径
    :param output_dir: 输出目录，默认与原文件同目录
    :return: 转换后的docx文件路径
    """
    # 检查文件是否存在
    if not os.path.exists(doc_path):
        raise FileNotFoundError(f"文件不存在: {doc_path}")
    
    # 检查文件是否为doc格式
    if not doc_path.lower().endswith('.doc'):
        raise ValueError(f"文件不是doc格式: {doc_path}")
    
    # 提取文件名和目录
    file_dir = os.path.dirname(doc_path)
    file_name = os.path.basename(doc_path)
    base_name = os.path.splitext(file_name)[0]
    
    # 确定输出目录
    if output_dir is None:
        output_dir = file_dir
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建输出文件路径（添加"_已转换"后缀）
    docx_file_name = f"{base_name}_已转换.docx"
    docx_path = os.path.join(output_dir, docx_file_name)
    
    try:
        # 启动Word应用程序
        word = comtypes.client.CreateObject('Word.Application')
        word.Visible = False  # 不显示Word窗口
        
        # 打开doc文件
        doc = word.Documents.Open(os.path.abspath(doc_path))
        
        # 另存为docx格式
        doc.SaveAs(os.path.abspath(docx_path), FileFormat=16)  # 16 = wdFormatXMLDocument
        
        # 关闭文档和Word应用程序
        doc.Close()
        word.Quit()
        
        # 等待一下确保文件保存完成
        time.sleep(1)
        
        print(f"✅ 成功将 {file_name} 转换为 {docx_file_name}")
        return docx_path
        
    except Exception as e:
        # 确保关闭Word应用程序
        try:
            word.Quit()
        except:
            pass
        
        raise Exception(f"转换doc到docx时出错: {str(e)}")


if __name__ == "__main__":
    """
    命令行使用示例：
    python doc_to_docx_converter.py input.doc [output_dir]
    """
    if len(sys.argv) < 2:
        print("用法: python doc_to_docx_converter.py <doc文件路径> [输出目录]")
        sys.exit(1)
    
    doc_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        docx_path = convert_doc_to_docx(doc_path, output_dir)
        print(f"转换完成，输出文件: {docx_path}")
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)