#!/usr/bin/env python3
"""
Word文档合并工具
支持保留表格格式的批量文档合并
"""

import os
import glob
import logging
import gc
import datetime
from typing import List, Dict, Tuple
import psutil

try:
    import win32com.client as win32
    WIN32_AVAILABLE = True
except ImportError:
    WIN32_AVAILABLE = False

try:
    from docxcompose.composer import Composer
    from docx import Document
    DOCXCOMPOSE_AVAILABLE = True
except ImportError:
    DOCXCOMPOSE_AVAILABLE = False

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


class DocumentMerger:
    """文档合并器"""
    
    def __init__(self, log_level=logging.INFO):
        """初始化合并器"""
        self.setup_logging(log_level)
        self.win32_available = self.check_win32_availability()
        self.process = psutil.Process()
        
    def setup_logging(self, log_level):
        """设置日志"""
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
    def check_win32_availability(self) -> bool:
        """检查win32com和Word是否可用"""
        if not WIN32_AVAILABLE:
            self.logger.warning("win32com不可用，将使用docxcompose")
            return False
            
        try:
            word = win32.gencache.EnsureDispatch('Word.Application')
            word.Quit()
            self.logger.info("检测到Word程序，将使用win32com进行合并")
            return True
        except Exception as e:
            self.logger.warning(f"Word程序不可用: {str(e)}，将使用docxcompose")
            return False
    
    def get_memory_usage(self) -> float:
        """获取内存使用情况(MB)"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def parse_filename(self, filename: str) -> Tuple[str, str, str]:
        """
        解析文件名格式: XX银行人员简历_人员名称_20251120.docx
        返回: (银行名称, 人员名称, 日期)
        """
        try:
            basename = os.path.splitext(filename)[0]
            parts = basename.split('_')
            
            if len(parts) >= 3:
                bank_name = parts[0]
                person_name = parts[1]
                date_str = parts[2]
                return bank_name, person_name, date_str
            else:
                return "", "", ""
        except Exception:
            return "", "", ""
    
    def classify_files(self, folder_path: str, target_date: str) -> Dict[str, List[str]]:
        """
        分类文件并按银行名称分组
        返回: {银行名称: [文件路径列表]}
        """
        self.logger.info(f"开始扫描文件夹: {folder_path}")
        
        # 获取所有docx文件
        pattern = os.path.join(folder_path, "*.docx")
        all_files = glob.glob(pattern)
        
        classified_files = {}
        valid_count = 0
        
        for file_path in all_files:
            filename = os.path.basename(file_path)
            bank_name, person_name, date_str = self.parse_filename(filename)
            
            # 检查日期是否符合
            if date_str == target_date and bank_name and person_name:
                if bank_name not in classified_files:
                    classified_files[bank_name] = []
                classified_files[bank_name].append(file_path)
                valid_count += 1
                self.logger.debug(f"找到合格文件: {filename}")
            else:
                self.logger.debug(f"跳过文件(日期或格式不匹配): {filename}")
        
        self.logger.info(f"扫描完成: 找到 {valid_count} 个符合条件的文件")
        for bank_name, files in classified_files.items():
            self.logger.info(f"  {bank_name}: {len(files)} 个文件")
        
        return classified_files
    
    def create_output_folder(self, input_folder: str, bank_name: str) -> str:
        """创建输出文件夹"""
        output_folder_name = f"{bank_name}合并简历"
        output_folder_path = os.path.join(input_folder, output_folder_name)
        
        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)
            self.logger.info(f"创建输出文件夹: {output_folder_path}")
        
        return output_folder_path
    
    def get_output_filename(self, bank_name: str, target_date: str) -> str:
        """生成输出文件名"""
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        return f"{bank_name}人员简历_合并_{today_str}.docx"
    
    def cleanup_existing_file(self, file_path: str):
        """清理已存在的文件"""
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                self.logger.info(f"删除已存在的文件: {file_path}")
            except Exception as e:
                self.logger.error(f"删除文件失败: {file_path}, 错误: {str(e)}")
    
    def merge_with_win32com(self, file_list: List[str], output_path: str) -> bool:
        """使用win32com合并文档"""
        self.logger.info("使用win32com进行合并...")
        
        try:
            word = win32.gencache.EnsureDispatch('Word.Application')
            word.Visible = False
            word.DisplayAlerts = False
            
            # 创建新文档
            merged_doc = word.Documents.Add()
            
            # 设置进度条
            if TQDM_AVAILABLE:
                file_iter = tqdm(file_list, desc="win32com合并")
            else:
                file_iter = file_list
                self.logger.info(f"开始合并 {len(file_list)} 个文件")
            
            success_count = 0
            for i, file_path in enumerate(file_iter):
                try:
                    selection = word.Selection
                    
                    # 插入文件（保留所有格式）
                    selection.InsertFile(file_path)
                    
                    # 在文档间添加分节符（最后一个不添加）
                    if i < len(file_list) - 1:
                        selection.InsertBreak(2)  # 2 = 分节符下一页
                    
                    success_count += 1
                    
                    # 每处理10个文件清理一次内存
                    if success_count % 10 == 0:
                        gc.collect()
                        
                except Exception as e:
                    self.logger.error(f"处理文件失败: {os.path.basename(file_path)}, 错误: {str(e)}")
                    continue
            
            # 保存文档
            merged_doc.SaveAs(output_path)
            merged_doc.Close()
            word.Quit()
            
            self.logger.info(f"win32com合并完成: 成功 {success_count}/{len(file_list)} 个文件")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"win32com合并失败: {str(e)}")
            return False
    
    def merge_with_docxcompose(self, file_list: List[str], output_path: str) -> bool:
        """使用docxcompose合并文档"""
        self.logger.info("使用docxcompose进行合并...")
        
        if not DOCXCOMPOSE_AVAILABLE:
            self.logger.error("docxcompose不可用")
            return False
        
        try:
            if not file_list:
                self.logger.error("文件列表为空")
                return False
            
            # 以第一个文档作为主文档
            master = Document(file_list[0])
            composer = Composer(master)
            
            # 设置进度条
            if TQDM_AVAILABLE:
                remaining_files = tqdm(file_list[1:], desc="docxcompose合并")
            else:
                remaining_files = file_list[1:]
                self.logger.info(f"开始合并 {len(file_list)} 个文件")
            
            success_count = 1  # 第一个文件已加载
            for i, file_path in enumerate(remaining_files, 1):
                try:
                    doc = Document(file_path)
                    
                    # 在追加文档之前添加分页符
                    from docx.enum.text import WD_BREAK
                    composer.doc.add_page_break()
                    
                    composer.append(doc)
                    success_count += 1
                    
                    # 内存管理
                    if i % 10 == 0:
                        gc.collect()
                        self.logger.debug(f"已处理 {i} 个文件，当前内存: {self.get_memory_usage():.2f} MB")
                        
                except Exception as e:
                    self.logger.error(f"处理文件失败: {os.path.basename(file_path)}, 错误: {str(e)}")
                    continue
            
            composer.save(output_path)
            self.logger.info(f"docxcompose合并完成: 成功 {success_count}/{len(file_list)} 个文件")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"docxcompose合并失败: {str(e)}")
            return False
    
    def merge_files(self, input_folder: str, target_date: str, 
                   use_win32com: bool = None) -> Dict[str, Dict]:
        """
        主合并函数
        
        Args:
            input_folder: 输入文件夹路径
            target_date: 目标日期 (格式: yyyymmdd)
            use_win32com: 强制使用win32com或docxcompose，None为自动选择
            
        Returns:
            Dict: 合并结果统计
        """
        self.logger.info(f"开始合并任务 - 文件夹: {input_folder}, 目标日期: {target_date}")
        self.logger.info(f"初始内存使用: {self.get_memory_usage():.2f} MB")
        
        # 分类文件
        classified_files = self.classify_files(input_folder, target_date)
        
        if not classified_files:
            self.logger.warning("没有找到符合条件的文件")
            return {"status": "no_files", "details": {}}
        
        # 确定合并方法
        if use_win32com is None:
            use_win32com = self.win32_available
        else:
            use_win32com = use_win32com and self.win32_available
        
        merge_method = "win32com" if use_win32com else "docxcompose"
        self.logger.info(f"使用合并方法: {merge_method}")
        
        results = {}
        
        # 按银行分组处理
        for bank_name, file_list in classified_files.items():
            self.logger.info(f"处理银行: {bank_name}, 文件数: {len(file_list)}")
            
            # 创建输出文件夹
            output_folder = self.create_output_folder(input_folder, bank_name)
            
            # 生成输出文件名
            output_filename = self.get_output_filename(bank_name, target_date)
            output_path = os.path.join(output_folder, output_filename)
            
            # 清理已存在的文件
            self.cleanup_existing_file(output_path)
            
            # 按文件名排序
            file_list.sort()
            
            # 执行合并
            if use_win32com:
                success = self.merge_with_win32com(file_list, output_path)
                # 如果win32com失败，尝试使用docxcompose
                if not success:
                    self.logger.info("win32com合并失败，尝试使用docxcompose")
                    success = self.merge_with_docxcompose(file_list, output_path)
            else:
                success = self.merge_with_docxcompose(file_list, output_path)
            
            # 记录结果
            results[bank_name] = {
                "input_files": len(file_list),
                "output_path": output_path,
                "success": success,
                "method": merge_method
            }
            
            if success:
                self.logger.info(f"成功合并: {bank_name} -> {output_path}")
            else:
                self.logger.error(f"合并失败: {bank_name}")
            
            # 强制垃圾回收
            gc.collect()
        
        self.logger.info(f"合并任务完成 - 最终内存使用: {self.get_memory_usage():.2f} MB")
        
        return {
            "status": "completed",
            "method": merge_method,
            "details": results
        }


# 使用示例和测试代码
if __name__ == "__main__":
    def main():
        """主函数示例"""
        import argparse
        
        parser = argparse.ArgumentParser(description="合并Word文档")
        parser.add_argument("input_folder", help="输入文件夹路径")
        parser.add_argument("target_date", help="目标日期 (格式: yyyymmdd)")
        parser.add_argument("--use-win32com", action="store_true", 
                          help="强制使用win32com")
        parser.add_argument("--use-docxcompose", action="store_true", 
                          help="强制使用docxcompose")
        parser.add_argument("--log-level", default="INFO", 
                          choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                          help="日志级别")
        
        args = parser.parse_args()
        
        # 确定合并方法
        use_win32com = None
        if args.use_win32com:
            use_win32com = True
        elif args.use_docxcompose:
            use_win32com = False
        
        # 执行合并
        merger = DocumentMerger(log_level=getattr(logging, args.log_level))
        result = merger.merge_files(args.input_folder, args.target_date, use_win32com)
        
        # 打印结果摘要
        print("\n" + "="*50)
        print("合并结果摘要:")
        print("="*50)
        for bank_name, detail in result.get("details", {}).items():
            status = "成功" if detail["success"] else "失败"
            print(f"{bank_name}: {status} ({detail['input_files']} 个文件)")
            print(f"  输出: {detail['output_path']}")
    
    main()