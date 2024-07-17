# import necessary libraries
import requests
from bs4 import BeautifulSoup
import openpyxl
from openpyxl.utils import get_column_letter
# Use the "dataframe_to_rows" function from the "openpyxl.utils.dataframe" module to write the data frame to the target sheet
from openpyxl.utils.dataframe import dataframe_to_rows

# Import the necessary component from the qcc_components.js library
from qcc_components import qcc_search

workbook = openpyxl.load_workbook(r'D:\10-售前2023\2023杂项内容\案例映射\9.长亮数据案例统一视图-v1.0-内部版-0423.xlsx')
target_sheet = workbook['客户清单']
# define the search query using the customer name from cell A2
search_query = '百度百科 ' + '自贡商行'

# use the component to perform a search query
first_result = qcc_search(search_query)

# write the result to cell H2
#H2.value = first_result
print(first_result)