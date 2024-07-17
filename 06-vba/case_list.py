import os
import win32com.client as win32
import openpyxl

def confirm_execution():
    # 弹出确认框，确认后执行代码
    if win32.MessageBox(None, "确认执行操作吗？", "确认执行", 4) != 6:  # 6 表示用户点击了"是"
        return
    
    # 获取基本配置信息
    mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    
    # 获取源工作簿名称和目标工作簿名称
    sourceWorkbookName = mappingSheet.Range("C3").Value
    targetWorkbookName = mappingSheet.Range("D3").Value
    sourceWorkSheetName = mappingSheet.Range("C4").Value
    
    # 检查源工作簿是否存在
    sourceWorkbookPath = os.path.join(os.getcwd(), sourceWorkbookName)
    if not os.path.exists(sourceWorkbookPath):
        win32.MessageBox(None, "源工作簿不存在！", "错误", 48)  # 48 表示显示警告图标
        return
    
    # 打开源工作簿并复制指定的工作表
    sourceWorkbook = win32.gencache.EnsureDispatch("Excel.Application").Workbooks.Open(sourceWorkbookPath)
    sourceWorksheet = None
    try:
        sourceWorksheet = sourceWorkbook.Sheets(sourceWorkSheetName)
    except:
        pass
    if sourceWorksheet is None:
        win32.MessageBox(None, "指定的工作表不存在！", "错误", 48)
        sourceWorkbook.Close(False)
        return
    
    # 检查是否已存在以“2014年以来的案例”为名称的工作表
    targetWorksheet = None
    try:
        targetWorksheet = ThisWorkbook.Sheets("2014年以来的案例")
    except:
        pass
    
    # 如果存在，则删除该工作表
    if targetWorksheet is not None:
        targetWorksheet.Delete()
    
    # 复制源工作表到当前工作簿
    sourceWorksheet.Copy(After=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count))
    targetWorksheet = ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count)
    targetWorksheet.Name = "2014年以来的案例"
    
    # 执行各个子过程
    cust_list()
    case_list()
    general_supply_list()
    Datapp_supply_list()
    
    # 删除工作表"2014年以来的案例"
    ThisWorkbook.Sheets("2014年以来的案例").Delete()
    
    ThisWorkbook.Sheets("文档目录").Activate()
    
    # 提示处理完成
    win32.MessageBox(None, "处理完成！", "提示", 64)  # 64 表示显示信息图标
    
    # 关闭源工作簿
    sourceWorkbook.Close(False)
def cust_list():
    # 优化代码，屏幕不更新
    Application.ScreenUpdating = False

    # 设置源工作表和目标工作表
    wsSource = ThisWorkbook.Sheets("2014年以来的案例")
    wsTarget = ThisWorkbook.Sheets("客户清单")
    mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    
    # 获取源工作表的最大行数
    lastRowSource = wsSource.Cells(wsSource.Rows.Count, "B").End(xlUp).Row
    
    # 获取目标工作表的最大行数
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).Row
    
    # 清空目标工作表的数据
    if lastRowTarget > 1:
        wsTarget.Range("A2:E" + str(lastRowTarget)).ClearContents
        lastRowTarget = 1
    
    # 遍历源工作表的数据
    for i in range(2, lastRowSource + 1):
        customerName = wsSource.Cells(i, "B").Value
        customerType = wsSource.Cells(i, "D").Value
        
        # 写入处理后的数据到目标工作表
        lastRowTarget += 1
        wsTarget.Cells(lastRowTarget, "A").Value = customerName
        wsTarget.Cells(lastRowTarget, "B").Value = get_abbreviation(customerName)
        
        if customerType == "城市商业银行":
            wsTarget.Cells(lastRowTarget, "C").Value = "城商行"
        else:
            wsTarget.Cells(lastRowTarget, "C").Value = customerType
    
    # 进行去重操作
    wsTarget.Range("A1:I" + str(lastRowTarget)).RemoveDuplicates(Columns=[1, 2, 3], Header=xlYes)
    wsTarget.Range("A1:I" + str(lastRowTarget)).Font.Name = "思源黑体"
    wsTarget.Range("A1:I" + str(lastRowTarget)).VerticalAlignment = xlVAlignCenter
    wsTarget.Range("A1:I" + str(lastRowTarget)).Font.Size = 10
    wsTarget.Range("A1:I" + str(lastRowTarget)).Borders.LineStyle = xlContinuous
    wsTarget.Range("A1:I" + str(lastRowTarget)).Borders.Weight = xlThin
    
    # 移除"B"列中的特定字样
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "B").End(xlUp).Row
    wsTarget.Range("B2:B" + str(lastRowTarget)).Replace("股份有限公司", "", LookAt=xlPart)
    wsTarget.Range("B2:B" + str(lastRowTarget)).Replace("有限公司", "", LookAt=xlPart)
    wsTarget.Range("B2:B" + str(lastRowTarget)).Replace("有限责任公司", "", LookAt=xlPart)
    
    # 获取去重后A列的最大行数
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).Row
    
    # 获取D2和E2单元格中的公式
    formulaD = mappingSheet.Range("H2").Formula
    formulaE = mappingSheet.Range("H3").Formula
    
    wsTarget.Range("D2").Value = formulaD
    wsTarget.Range("E2").Value = formulaE
    
    # 填充公式到DE列的每一行
    wsTarget.Range("D2:E" + str(lastRowTarget)).Formula = wsTarget.Range("D2:E2").FormulaR1C1
    wsTarget.Range("D2:E" + str(lastRowTarget)).FillDown()

    print("客户清单处理完成")

    # 优化代码，屏幕更新
    Application.ScreenUpdating = True

def get_abbreviation(customer_name):
    # 根据需要实现获取客户简称的逻辑
    # 这里仅作示例，直接返回输入的客户名称
    return customer_name
def general_supply_list():
    # 通用补充映射
    targetSheet = workbook.sheets["通用补充信息"]
    sourceSheet = workbook.sheets["2014年以来的案例"]
    mappingSheet = workbook.sheets["映射配置信息"]
    
    # 获取源数据工作表的最大行数
    lastRowSource = sourceSheet.cells(sourceSheet.rows.count, "A").end("up").row
    
    # 获取目标工作表的最大行数判断是否有存量数据
    lastRowTarget = targetSheet.cells(targetSheet.rows.count, "A").end("up").row
    
    # 清空目标工作表的第二行开始的数据
    if lastRowTarget >= 4:
        targetSheet.range("A4:V" + str(lastRowTarget)).clear_contents()
        lastRowTarget = 3  # 初始化第一行为标题栏
    
    for i in range(2, lastRowSource + 1):
        # ************************初始化映射公式部分***************************
        # CustomerCategory = mappingSheet.cells(12, "C").value
    
        # ************************数据映射部分****************************
        # 直取单元格
        contractName = sourceSheet.cells(i, "A").value
        DataBaseVersion = sourceSheet.cells(i, "X").value
        NodeCount = sourceSheet.cells(i, "R").value
        BITools = sourceSheet.cells(i, "BB").value
        
        lastRowTarget += 1
        
        targetSheet.cells(lastRowTarget, "A").value = contractName
        targetSheet.cells(lastRowTarget, "C").value = DataBaseVersion
        targetSheet.cells(lastRowTarget, "D").value = NodeCount
        targetSheet.cells(lastRowTarget, "F").value = BITools
        
        # 映射加工
        # 调度平台 AU:ETL调度监控平台
        # 补充特殊处理 BD:调度功能，有“长亮”或“JCM"即填“调度平台“
        ConfigValue = mappingSheet.cells(53, "C").value
        SchdPlatform = get_prod_list2(ConfigValue, lastRowTarget)
        
        # 判断调度平台是否存在
        if "长亮" in sourceSheet.cells(i, "BD").value or "JCM" in sourceSheet.cells(i, "BD").value:
            SchdPlatform += ",调度平台"
        
        targetSheet.cells(lastRowTarget, "G").value = SchdPlatform
        
        # 开发平台产品
        ConfigValue = mappingSheet.cells(54, "C").value
        DevelopPlatform = get_prod_list2(ConfigValue, lastRowTarget)
        
        # 判断调度平台是否存在
        if "长亮" in sourceSheet.cells(i, "BD").value or "JCM" in sourceSheet.cells(i, "BD").value:
            DevelopPlatform += ",调度平台"
        
        targetSheet.cells(lastRowTarget, "H").value = DevelopPlatform
        
        # 数据交换平台
        ConfigValue = mappingSheet.cells(55, "C").value
        DataExchange = get_prod_list2(ConfigValue, lastRowTarget)
        targetSheet.cells(lastRowTarget, "I").value = DataExchange
        
        # 数据资产管理平台判断CO列是否存在，不为空则为长亮科技数据资产管理平台
        DataAssetManage = sourceSheet.cells(i, "CO").value
        
        if DataAssetManage != "":
            DataAssetManage = "长亮科技数据资产管理平台"
        
        targetSheet.cells(lastRowTarget, "J").value = DataAssetManage
    
    # 添加单元格边框、字体和垂直居中
    targetRange = targetSheet.range("A4:V" + str(lastRowTarget))
    targetRange.borders.linestyle = 1
    targetRange.borders.weight = 2
    targetRange.font.name = "思源黑体"
    targetRange.font.size = 10
    targetRange.verticalalignment = -4108
    
    print("通用补充处理完成")

