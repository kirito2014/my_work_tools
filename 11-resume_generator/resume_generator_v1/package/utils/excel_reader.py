import os
import pandas as pd
import xlrd
from openpyxl import load_workbook

class ExcelReader:
    def __init__(self, file_path):
        """
        初始化ExcelReader对象。

        :param file_path: Excel文件路径。
        """
        self.file_path = file_path
        self.file_type = self._get_file_type()

    def _get_file_type(self):
        """
        获取文件类型（.xls、.xlsx、.xlsm）。

        :return: 文件类型（"xls" 或 "xlsx"）。
        """
        file_extension = os.path.splitext(self.file_path)[1].lower()
        if file_extension == ".xls":
            return "xls"
        elif file_extension in (".xlsx", ".xlsm"):
            return "xlsx"
        else:
            raise ValueError(f"不支持的文件类型: {file_extension}")

    def read_excel(self, sheet_name="数据来源"):
        """
        读取Excel文件中的数据。

        :param sheet_name: 工作表名称，默认为"数据来源"。
        :return: 包含标题行和数据行的字典。
        """
        if self.file_type == "xls":
            return self._read_xls(sheet_name)
        elif self.file_type == "xlsx":
            return self._read_xlsx(sheet_name)
        else:
            raise ValueError(f"不支持的文件类型: {self.file_type}")

    def _read_xls(self, sheet_name):
        """
        读取.xls文件中的数据。

        :param sheet_name: 工作表名称。
        :return: 包含标题行和数据行的字典。
        """
        try:
            # 使用xlrd读取.xls文件
            workbook = xlrd.open_workbook(self.file_path)
            sheet = workbook.sheet_by_name(sheet_name)

            # 获取标题行
            headers = sheet.row_values(0)

            # 获取数据行
            data = []
            for row_idx in range(1, sheet.nrows):
                row_data = sheet.row_values(row_idx)
                data.append(row_data)

            return {"headers": headers, "data": data}

        except Exception as e:
            raise ValueError(f"读取.xls文件时出错: {e}")

    def _read_xlsx(self, sheet_name):
        """
        读取.xlsx或.xlsm文件中的数据。

        :param sheet_name: 工作表名称。
        :return: 包含标题行和数据行的字典。
        """
        try:
            # 使用pandas读取.xlsx或.xlsm文件
            df = pd.read_excel(self.file_path, sheet_name=sheet_name)

            # 获取标题行
            headers = df.columns.tolist()

            # 获取数据行
            data = df.values.tolist()

            return {"headers": headers, "data": data}

        except Exception as e:
            raise ValueError(f"读取.xlsx文件时出错: {e}")

# 示例调用
if __name__ == "__main__":
    # 示例文件路径
    file_path = r"D:\github\11-resume_generator\template\人员简历最新1.31.xlsm"

    try:
        # 创建ExcelReader对象
        reader = ExcelReader(file_path)

        # 读取数据
        result = reader.read_excel(sheet_name="数据来源")

        # 打印标题行和数据行
        print("标题行:", result["headers"])
        print("数据行:", result["data"])

    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
