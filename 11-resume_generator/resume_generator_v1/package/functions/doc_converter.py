#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
文档格式转换工具：将doc格式转换为docx格式
"""
import os
import sys
import comtypes.client
import time
from typing import Optional, List, Tuple

# 处理PyInstaller打包后的路径问题
if getattr(sys, 'frozen', False):
    # 打包后的环境
    base_dir = os.path.dirname(sys.executable)
    # 确保工作目录设置为当前目录（exe所在目录）
    os.chdir(base_dir)
else:
    # 开发环境
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def convert_doc_to_docx(doc_path: str, output_dir: Optional[str] = None) -> str:
    """
    将单个doc文件转换为docx文件
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
        
        # 减少延迟时间
        time.sleep(0.1)
        
        print(f"[OK] 成功将 {file_name} 转换为 {docx_file_name}")
        return docx_path
        
    except Exception as e:
        # 确保关闭Word应用程序
        try:
            word.Quit()
        except:
            pass
        
        raise Exception(f"转换doc到docx时出错: {str(e)}")


def batch_convert_docs_to_docx(doc_files: List[str], output_dir: Optional[str] = None) -> List[Tuple[str, str, bool]]:
    """
    批量转换多个doc文件为docx文件，只启动一次Word实例，显著提高转换速度
    :param doc_files: doc文件路径列表
    :param output_dir: 输出目录，默认与原文件同目录
    :return: 转换结果列表，每个元素为(原文件路径, 转换后文件路径, 是否成功)
    """
    results = []
    word = None
    
    try:
        # 只启动一次Word应用程序
        word = comtypes.client.CreateObject('Word.Application')
        word.Visible = False
        word.DisplayAlerts = False  # 禁用所有警告对话框
        
        for doc_path in doc_files:
            try:
                # 检查文件是否存在
                if not os.path.exists(doc_path):
                    results.append((doc_path, None, False))
                    continue
                
                # 检查文件是否为doc格式
                if not doc_path.lower().endswith('.doc'):
                    results.append((doc_path, None, False))
                    continue
                
                # 提取文件名和目录
                file_dir = os.path.dirname(doc_path)
                file_name = os.path.basename(doc_path)
                base_name = os.path.splitext(file_name)[0]
                
                # 确定输出目录
                current_output_dir = output_dir if output_dir else file_dir
                
                # 确保输出目录存在
                os.makedirs(current_output_dir, exist_ok=True)
                
                # 构建输出文件路径
                docx_file_name = f"{base_name}_已转换.docx"
                docx_path = os.path.join(current_output_dir, docx_file_name)
                
                # 打开并转换文件
                doc = word.Documents.Open(os.path.abspath(doc_path))
                doc.SaveAs(os.path.abspath(docx_path), FileFormat=16)  # 16 = wdFormatXMLDocument
                doc.Close(SaveChanges=False)
                
                # 极小的延迟确保文件保存完成
                time.sleep(0.05)
                
                results.append((doc_path, docx_path, True))
                print(f"[OK] 成功将 {file_name} 转换为 {docx_file_name}")
                
            except Exception as e:
                print(f"[ERROR] 转换 {os.path.basename(doc_path)} 时出错: {str(e)}")
                results.append((doc_path, None, False))
                
    except Exception as e:
        print(f"[ERROR] 批处理转换时出错: {str(e)}")
    finally:
        # 确保关闭Word应用程序
        if word is not None:
            try:
                word.Quit(SaveChanges=False)
            except:
                pass
    
    return results


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