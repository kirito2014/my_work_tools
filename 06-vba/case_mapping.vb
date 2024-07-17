    '--------------------------------------------------------------------
    ' 用途说明:
    ' 代码用途: 此代码用于对解决方案部案例清单，客户清单进行处理和优化，包括去重、简称提取和数据填充
    ' 使用方法：点击主页目录logo，点选确认执行，通过映射配置信息进行配置管理
    ' 版权所有 @2023 Sunline。保留所有权利。
    '--------------------------------------------------------------------

    
    ' 更新日期: [2023/05/11]
    ' 更新人员：[wangmj]
    ' 版本编号：[1.0.1]
    ' 修改内容: [
    '- 优化了注释和代码的格式，提高可读性。
    '- 统一使用`.Value`来获取单元格的值，增加代码的一致性。
    '- 修正了一处错误，将`ConfigValue`的赋值语句放在了循环内部。
    '- 对单元格范围添加边框、设置字体和垂直居中的格式，使用了链式调用来简化代码。
    '- 添加了一条操作完成的提示消息框。
    '- 添加了屏幕更新优化，将`Application.ScreenUpdating`设为`False`，在代码执行完毕后再设为`True`，减少屏幕刷新的次数，提高执行速度。
    ']
    '请注意，在使用代码时，请确保工作簿中存在名为"2014年以来的案例"、"通用补充信息"和"映射配置信息"的工作表，并且保证这些工作表中的单元格地址和映射配置的内容是正确的。



Sub ConfirmExecution()
    ' 弹出确认框，确认后执行代码
    If MsgBox("确认执行操作吗？", vbQuestion + vbYesNo, "确认执行") = vbNo Then Exit Sub
    
    ' 获取基本配置信息
    Dim mappingSheet As Worksheet
    Set mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    
    ' 获取源工作簿名称和目标工作簿名称
    Dim sourceWorkbookName As String
    Dim targetWorkbookName As String
    Dim sourceWorkSheetName As String
    sourceWorkbookName = mappingSheet.Range("C3").value
    targetWorkbookName = mappingSheet.Range("D3").value
    sourceWorkSheetName = mappingSheet.Range("C4").value
    findWorkbookPath = mappingSheet.Range("C2").value & "\" & sourceWorkbookName
    
    ' 检查源工作簿是否存在
    Dim sourceWorkbookPath As String
    sourceWorkbookPath = ThisWorkbook.Path & "\" & sourceWorkbookName
    If Dir(sourceWorkbookPath) = "" Then
        
        If Dir(findWorkbookPath) <> "" Then
            sourceWorkbookPath = findWorkbookPath
        Else
            MsgBox "源工作簿不存在！", vbExclamation, "错误"
            Exit Sub
        End If
        
        
    End If
    
    ' 打开源工作簿并复制指定的工作表
    Dim sourceWorkbook As Workbook
    Dim sourceWorksheet As Worksheet
    Set sourceWorkbook = Workbooks.Open(sourceWorkbookPath)
    On Error Resume Next
    Set sourceWorksheet = sourceWorkbook.Sheets(sourceWorkSheetName)
    On Error GoTo 0
    If sourceWorksheet Is Nothing Then
        MsgBox "指定的工作表不存在！", vbExclamation, "错误"
        sourceWorkbook.Close SaveChanges:=False
        Exit Sub
    End If
    
    ' 检查是否已存在以“2014年以来的案例”为名称的工作表
    Dim targetWorksheet As Worksheet
    On Error Resume Next
    Set targetWorksheet = ThisWorkbook.Sheets("2014年以来的案例")
    On Error GoTo 0
    
    ' 如果存在，则删除该工作表
    If Not targetWorksheet Is Nothing Then
        Application.DisplayAlerts = False
        targetWorksheet.Delete
        Application.DisplayAlerts = True
    End If
    
    ' 复制源工作表到当前工作簿
    sourceWorksheet.Copy After:=ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count)
    Set targetWorksheet = ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count)
    targetWorksheet.Name = "2014年以来的案例"
    
    ' 执行各个子过程
    cust_list
    case_list
    general_supply_list
    Datapp_supply_list
    
    ' 删除工作表"2014年以来的案例"

    Application.DisplayAlerts = False
    targetWorksheet.Delete
    Application.DisplayAlerts = True
    
    ThisWorkbook.Sheets("文档目录").Activate
    
    ' 提示处理完成
    MsgBox "处理完成！", vbInformation, "提示"
    
    ' 关闭源工作簿
    sourceWorkbook.Close SaveChanges:=False
End Sub



Private Sub cust_list()
    Application.ScreenUpdating = False '优化代码，屏幕不更新
    
    Dim wsSource As Worksheet
    Dim wsTarget As Worksheet
    Dim lastRowSource As Long
    Dim lastRowTarget As Long
    Dim i As Long
    
    ' 设置源工作表和目标工作表
    Set wsSource = ThisWorkbook.Sheets("2014年以来的案例") ' 第一个工作表
    Set wsTarget = ThisWorkbook.Sheets("客户清单") ' 名为"客户清单"的工作表
    Set mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    
    ' 获取源工作表的最大行数
    lastRowSource = wsSource.Cells(wsSource.Rows.Count, "B").End(xlUp).Row
    
    ' 获取目标工作表的最大行数
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).Row
    
    ' 清空目标工作表的数据
    If lastRowTarget > 1 Then
        wsTarget.Range("A2:E" & lastRowTarget).ClearContents
        lastRowTarget = 1
    End If
    
    ' 遍历源工作表的数据
    For i = 2 To lastRowSource
        Dim customerName As String
        Dim customerType As String
        
        customerName = wsSource.Cells(i, "B").value
        customerType = wsSource.Cells(i, "D").value
        
        ' 写入处理后的数据到目标工作表
        lastRowTarget = lastRowTarget + 1 ' 下一行
        wsTarget.Cells(lastRowTarget, "A").value = customerName
        wsTarget.Cells(lastRowTarget, "B").value = GetAbbreviation(customerName) ' 获取客户简称
        ' Modify City Commercial Banks
        If customerType = "城市商业银行" Then
            wsTarget.Cells(lastRowTarget, "C").value = "城商行"
        Else
            wsTarget.Cells(lastRowTarget, "C").value = customerType ' 获取客户细类
        End If
    Next i
    
    ' 进行去重操作
    With wsTarget.Range("A1:I" & lastRowTarget)
        .RemoveDuplicates Columns:=Array(1, 2, 3), Header:=xlYes
        .Font.Name = "思源黑体"
        .VerticalAlignment = xlVAlignCenter
        .Font.Size = 10
        .Borders.LineStyle = xlContinuous
        .Borders.Weight = xlThin
    End With
    
    ' 移除"B"列中的特定字样
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "B").End(xlUp).Row
    wsTarget.Range("B2:B" & lastRowTarget).Replace "股份有限公司", "", LookAt:=xlPart
    wsTarget.Range("B2:B" & lastRowTarget).Replace "有限公司", "", LookAt:=xlPart
    wsTarget.Range("B2:B" & lastRowTarget).Replace "有限责任公司", "", LookAt:=xlPart
    
    ' 获取去重后A列的最大行数
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).Row
       ' 获取D2和E2单元格中的公式
    Dim formulaD As String
    Dim formulaE As String
    formulaD = mappingSheet.Range("H2").formula
    formulaE = mappingSheet.Range("H3").formula
    
    wsTarget.Range("D2").value = formulaD
    wsTarget.Range("E2").value = formulaE
    ' 填充公式到DE列的每一行
    wsTarget.Range("D2:E" & lastRowTarget).formula = wsTarget.Range("D2:E" & lastRowTarget).FormulaR1C1
    wsTarget.Range("D2:E" & lastRowTarget).FillDown
    

    
    Debug.Print "客户清单处理完成"
    
    Application.ScreenUpdating = True '优化代码，屏幕更新
End Sub


Private Function GetOfficialName(ByVal customerName As String) As String
    Dim regex As Object
    Set regex = CreateObject("VBScript.RegExp")
    
    regex.pattern = ".*股份有限公司$"
    regex.Global = False
    
    If regex.test(customerName) Then
        GetOfficialName = regex.Replace(customerName, "")
    Else
        GetOfficialName = customerName
    End If
End Function

Private Function GetAbbreviation(ByVal customerName As String) As String
    ' 根据需要实现获取客户简称的逻辑
    ' 这里仅作示例，直接返回输入的客户名称
    GetAbbreviation = customerName
End Function

Private Sub general_supply_list()
    ' 通用补充映射
    Application.ScreenUpdating = False ' 优化代码，屏幕不更新
    
    Dim sourceSheet As Worksheet
    Dim targetSheet As Worksheet
    Dim lastRowSource As Long
    Dim lastRowTarget As Long
    Dim ConfigValue As String
    Dim i As Long
    
    ' 设置源数据工作表和目标工作表
    Set sourceSheet = ThisWorkbook.Sheets("2014年以来的案例")
    Set targetSheet = ThisWorkbook.Sheets("通用补充信息")
    Set mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    
    ' 获取源数据工作表的最大行数
    lastRowSource = sourceSheet.Cells(sourceSheet.Rows.Count, "A").End(xlUp).Row
    
    ' 获取目标工作表的最大行数判断是否有存量数据
    lastRowTarget = targetSheet.Cells(targetSheet.Rows.Count, "A").End(xlUp).Row
    
    ' 清空目标工作表的第二行开始的数据
    If lastRowTarget >= 4 Then
        targetSheet.Range("A4:V" & lastRowTarget).ClearContents
        lastRowTarget = 3 ' 初始化第一行为标题栏
    End If
    
    For i = 2 To lastRowSource
        ' ************************初始化映射公式部分***************************
        ' CustomerCategory = mappingSheet.Cells(12, "C").value
    
        ' ************************数据映射部分****************************
        ' 直取单元格
        Dim contractName As String
        Dim DataBaseVersion As String
        Dim NodeCount As String
        Dim BITools As String
        
        contractName = sourceSheet.Cells(i, "A").value
        DataBaseVersion = sourceSheet.Cells(i, "X").value
        NodeCount = sourceSheet.Cells(i, "R").value
        BITools = sourceSheet.Cells(i, "BB").value
        
        lastRowTarget = lastRowTarget + 1
        
        targetSheet.Cells(lastRowTarget, "A").value = contractName
        targetSheet.Cells(lastRowTarget, "C").value = DataBaseVersion
        targetSheet.Cells(lastRowTarget, "D").value = NodeCount
        targetSheet.Cells(lastRowTarget, "F").value = BITools
        
        ' 映射加工
        ' 调度平台 AU:ETL调度监控平台
        ' 补充特殊处理 BD:调度功能，有“长亮”或“JCM"即填“调度平台“
        Dim SchdPlatform As String
        
        ConfigValue = mappingSheet.Cells(53, "C").value
        SchdPlatform = get_prod_list2(ConfigValue, i)
        
        ' 判断调度平台是否存在
        If InStr(sourceSheet.Cells(i, "BD").value, "长亮") > 0 Or InStr(sourceSheet.Cells(i, "BD").value, "JCM") > 0 Then
            SchdPlatform = SchdPlatform & ",调度平台"
        End If
        
        targetSheet.Cells(lastRowTarget, "G").value = SchdPlatform
        
        ' 开发平台产品
        Dim DevelopPlatform As String
        
        ConfigValue = mappingSheet.Cells(54, "C").value
        DevelopPlatform = get_prod_list2(ConfigValue, i)
        
            ' 判断调度平台是否存在
    If InStr(sourceSheet.Cells(i, "BD").value, "长亮") > 0 Or InStr(sourceSheet.Cells(i, "BD").value, "JCM") > 0 Then
        DevelopPlatform = DevelopPlatform & ",调度平台"
    End If
    
    targetSheet.Cells(lastRowTarget, "H").value = DevelopPlatform
    
    ' 数据交换平台
    Dim DataExchange As String
    
    ConfigValue = mappingSheet.Cells(55, "C").value
    DataExchange = get_prod_list2(ConfigValue, i)
    targetSheet.Cells(lastRowTarget, "I").value = DataExchange
    
    ' 数据资产管理平台判断CO列是否存在，不为空则为长亮科技数据资产管理平台
    Dim DataAssetManage As String
    
    DataAssetManage = sourceSheet.Cells(i, "CO").value
    
    If DataAssetManage <> "" Then
        DataAssetManage = "长亮科技数据资产管理平台"
    End If
    
    targetSheet.Cells(lastRowTarget, "J").value = DataAssetManage
    
Next i

' 添加单元格边框、字体和垂直居中
With targetSheet.Range("A4:V" & lastRowTarget)
    .Borders.LineStyle = xlContinuous
    .Borders.Weight = xlThin
    .Font.Name = "思源黑体"
    .Font.Size = 10
    .VerticalAlignment = xlVAlignCenter
End With

Debug.Print "通用补充处理完成"

Application.ScreenUpdating = True ' 优化代码，屏幕更新

End Sub


Private Sub Datapp_supply_list()
'通用补充映射
    Application.ScreenUpdating = False '优化代码，屏幕不更新
    Dim sourceSheet As Worksheet
    Dim targetSheet As Worksheet
    Dim lastRowSource As Long
    Dim lastRowTarget As Long
    Dim ConfigValue As String
    Dim i As Long


    ' 设置源数据工作表和目标工作表
    Set sourceSheet = ThisWorkbook.Sheets("2014年以来的案例")
    Set targetSheet = ThisWorkbook.Sheets("数据应用类补充信息")
    Set mappingSheet = ThisWorkbook.Sheets("映射配置信息")


    ' 获取源数据工作表的最大行数
    lastRowSource = sourceSheet.Cells(sourceSheet.Rows.Count, "A").End(xlUp).Row
    
    ' 获取目标工作表的最大行数判断是否有存量数据
    lastRowTarget = targetSheet.Cells(targetSheet.Rows.Count, "A").End(xlUp).Row
    
     ' 清空目标工作表的第二行开始的数据
    If lastRowTarget >= 4 Then
        targetSheet.Range("A4:V" & lastRowTarget).ClearContents
        lastRowTarget = 3 '初始化第一行为标题栏
    End If

    For i = 2 To lastRowSource
    
    '************************初始化映射公式部分***************************
        'CustomerCategory = mappingSheet.Cells(12, "C").value
    
     
     
     
     '************************数据映射部分****************************
     '直取单元格
        Dim contractName As String
        Dim BITools As String
        
        contractName = sourceSheet.Cells(i, "A").value
        BITools = sourceSheet.Cells(i, "BB").value
        
        lastRowTarget = lastRowTarget + 1
        targetSheet.Cells(lastRowTarget, "A").value = contractName
        targetSheet.Cells(lastRowTarget, "B").value = BITools

    
    Next i
    
    
    '添加单元格边框,设置垂直居中，思源黑体字体10号
    With targetSheet.Range("A4" & ":H" & lastRowTarget)
    .Borders.LineStyle = xlContinuous
    .Borders.Weight = xlThin
    .Font.Name = "思源黑体"
    .VerticalAlignment = xlVAlignCenter
    .Font.Size = 10
    .Font.Color = black
    End With

    Debug.Print "案例补充处理完成"

Application.ScreenUpdating = True '优化代码，屏幕更新

End Sub

Sub test()
Call case_list

End Sub

Private Sub case_list()

    Application.ScreenUpdating = False '优化代码，屏幕不更新
    
    Dim sourceSheet As Worksheet
    Dim targetSheet As Worksheet
    Dim lastRowSource As Long
    Dim lastRowTarget As Long
    '定义客户细类，客户类型，客户大类，客户资产规模
    Dim CustomerCategory, customerType, CustomerClass, CustomerAssetSize, SolutionCategorySummary As Variant
    '定义解决方案,数据平台,配套产品实施等用语定义
    Dim ConsultPlanType, DataPlatformType, SupplyProdImple, PlatformImple, PlatformConsult As Variant
    Dim ConfigValue As String
    Dim GeneralSupInfor, DataAppSupInfor As String
    Dim i As Long
    
    ' 设置源数据工作表和目标工作表
    Set sourceSheet = ThisWorkbook.Sheets("2014年以来的案例")
    Set targetSheet = ThisWorkbook.Sheets("案例清单")
    Set mappingSheet = ThisWorkbook.Sheets("映射配置信息")
    

    
    ' 获取源数据工作表的最大行数
    lastRowSource = sourceSheet.Cells(sourceSheet.Rows.Count, "A").End(xlUp).Row
    
    ' 获取目标工作表的最大行数判断是否有存量数据
    lastRowTarget = targetSheet.Cells(targetSheet.Rows.Count, "A").End(xlUp).Row
    
     ' 清空目标工作表的第二行开始的数据
    If lastRowTarget >= 4 Then
        targetSheet.Range("A4:AK" & lastRowTarget).ClearContents
        lastRowTarget = 3 '初始化第一行为标题栏，保留案例方便测试
    End If
    
    
    ' 遍历源数据表格的数据
    For i = 2 To lastRowSource
    
       '************************初始化映射公式***************************
        CustomerCategory = mappingSheet.Cells(12, "C").value
        customerType = mappingSheet.Cells(13, "C").value
        CustomerClass = mappingSheet.Cells(14, "C").value
        CustomerAssetSize = mappingSheet.Cells(15, "C").value
        SolutionCategorySummary = mappingSheet.Cells(19, "C").value
        GeneralSupInfor = mappingSheet.Cells(37, "C").value
        DataAppSupInfor = mappingSheet.Cells(38, "C").value
        
        '**********************合同基本信息处理***************************
        ' 获取合同名称、项目规模、合同年份和是否人力外包的值
        Dim contractName As String
        Dim projectSize As String
        Dim contractYear As String
        Dim isOutsourced As String
        
        contractName = sourceSheet.Cells(i, "A").value
        projectSize = process_amount(sourceSheet.Cells(i, "K").value)
        contractYear = sourceSheet.Cells(i, "I").value
        isOutsourced = is_yes(sourceSheet.Cells(i, "CV").value)
        
        ' 将映射的数据写入目标工作表的合同基本信息列（A:D）
        lastRowTarget = lastRowTarget + 1
        targetSheet.Cells(lastRowTarget, "A").value = contractName
        targetSheet.Cells(lastRowTarget, "B").value = projectSize
        targetSheet.Cells(lastRowTarget, "C").value = contractYear
        targetSheet.Cells(lastRowTarget, "D").value = isOutsourced
        
        '***********************客户基本信息处理**************************
        '客户简称与客户清单保持一致
        
        customerName = remove_company_suffix(sourceSheet.Cells(i, "B").value)
        
        '替换公式中的E4为实际行数
        
        CustomerCategory = Replace(CustomerCategory, "E4", "E" & CStr(lastRowTarget))
        customerType = Replace(customerType, "E4", "E" & CStr(lastRowTarget))
        CustomerClass = Replace(CustomerClass, "E4", "E" & CStr(lastRowTarget))
        CustomerAssetSize = Replace(CustomerAssetSize, "E4", "E" & CStr(lastRowTarget))
        'Debug.Print CustomerClass
        
        targetSheet.Cells(lastRowTarget, "E").value = customerName
        targetSheet.Cells(lastRowTarget, "F").value = CustomerCategory
        targetSheet.Cells(lastRowTarget, "G").value = customerType
        targetSheet.Cells(lastRowTarget, "H").value = CustomerClass
        targetSheet.Cells(lastRowTarget, "I").value = CustomerAssetSize
        
        '**********************归属信息处理*******************************
        '主管系统部 分管系统部 案例信息反馈人 暂无映射对应

        '**********************解决方案分类处理***************************
        SolutionCategorySummary = Replace(SolutionCategorySummary, "var", CStr(lastRowTarget))
        
        targetSheet.Cells(lastRowTarget, "M").value = SolutionCategorySummary
        
        '咨询规划类 CP:数据架构咨询规划|BH:数据挖掘模型
        '使用配置函数
        ConfigValue = mappingSheet.Cells(20, "C").value
        ConsultPlanType = get_prod_list2(ConfigValue, i)
        targetSheet.Cells(lastRowTarget, "N").value = ConsultPlanType
        
        'ConfigValue = ""
        
        '数据平台类 S:数据迁移|T:ODS|U:数据仓库|AC:历史数据平台|Y:数据中台
        ConfigValue = mappingSheet.Cells(21, "C").value
        DataPlatformType = get_prod_list2(ConfigValue, i)
        targetSheet.Cells(lastRowTarget, "O").value = DataPlatformType
        
        '配套产品实施 AR:数据交换平台|AS:外部数据管理平台|AU:ETL调度监控平台|AW:数据补录平台|BI:数据开发平台
        '补充特殊处理 BD:调度功能，有“长亮”或“JCM"即填“调度平台“
        ConfigValue = mappingSheet.Cells(22, "C").value
        SupplyProdImple = get_prod_list2(ConfigValue, i)
        
        '判断调度平台是否存在
        If InStr(sourceSheet.Cells(i, "BD").value, "长亮") > 0 Then
            SupplyProdImple = SupplyProdImple & ",调度平台"
        ElseIf InStr(sourceSheet.Cells(i, "BD").value, "JCM") > 0 Then
            SupplyProdImple = SupplyProdImple & ",调度平台"
        Else
            SupplyProdImple = SupplyProdImple
        End If

        targetSheet.Cells(lastRowTarget, "P").value = SupplyProdImple
        '平台实施类
        '数据标准:数据标准管理模块|元数据:元数据管理模块|数据质量:数据质量管理模块|数据资产:数据资产管理模块|数据安全:数据安全管理模块|主数据:主数据管理模块|模型:数据模型管理模块|需求:数据需求管理模块|生命周期:数据生命周期管理模块
        
        ConfigValue = mappingSheet.Cells(23, "C").value
        PlatformImple = get_prod_list1(ConfigValue, "CN", i)
        targetSheet.Cells(lastRowTarget, "Q").value = PlatformImple
        
        '平台咨询类
        
        ConfigValue = mappingSheet.Cells(24, "C").value
        PlatformConsult = get_prod_list1(ConfigValue, "CO", i)
        targetSheet.Cells(lastRowTarget, "R").value = PlatformConsult
        
        '数据分析类
        '条线集市 S列 AF:零售集市|AG:信用卡集市|AH:对公集市|AI:同业集市|AJ:财务集市|AK:风险集市|AL:报表集市|AN:监管集市|AO:分行集市
        Dim LineMarket As String
        Dim OtherLineMarket As String
        Dim IndicLabel As String
        Dim DataService As String
        'Dim OtherConfigValue As String
        Dim mergedLineMarket As String

        ConfigValue = mappingSheet.Cells(25, "C").value
        LineMarket = get_prod_list2(ConfigValue, i)

        ' 还需要统计AP列其他集市的内容
        ConfigValue = mappingSheet.Cells(25, "E").value
        OtherLineMarket = get_prod_list1(ConfigValue, "AP", i)
        '使用函数去重拼接结果集
        mergedLineMarket = merge_markets(LineMarket, OtherLineMarket)
        'Debug.Print mergedLineMarket
        
        targetSheet.Cells(lastRowTarget, "S").value = mergedLineMarket
         
        '指标标签 T列
        ConfigValue = mappingSheet.Cells(26, "C").value
        IndicLabel = get_prod_list2(ConfigValue, i)
        targetSheet.Cells(lastRowTarget, "T").value = IndicLabel
        
        '数据服务 U列
        ConfigValue = mappingSheet.Cells(27, "C").value
        DataService = get_prod_list1(ConfigValue, "BJ", i)
        targetSheet.Cells(lastRowTarget, "U").value = DataService
        
        '数据应用类
        Dim OperateAnalzy As String
        Dim CustManage As String
        Dim RiskManage As String
        Dim RegReport As String
        
        '经营分析
        ConfigValue = mappingSheet.Cells(28, "C").value
        OperateAnalzy = get_prod_list2(ConfigValue, i)
        
        '经营分析
        ConfigValue = mappingSheet.Cells(29, "C").value
        CustManage = get_prod_list2(ConfigValue, i)
        
        '经营分析
        ConfigValue = mappingSheet.Cells(30, "C").value
        RiskManage = get_prod_list2(ConfigValue, i)
        
         '经营分析
        ConfigValue = mappingSheet.Cells(31, "C").value
        RegReport = get_prod_list2(ConfigValue, i)
        
        targetSheet.Cells(lastRowTarget, "V").value = OperateAnalzy
        targetSheet.Cells(lastRowTarget, "W").value = CustManage
        targetSheet.Cells(lastRowTarget, "X").value = RiskManage
        targetSheet.Cells(lastRowTarget, "Y").value = RegReport
        
        '代销暂无处理
        '项目补充信息
        Dim ProjectContent As String
        Dim ProjectCycyle As String
        Dim ProjectStatus As String
        
        ProjectContent = sourceSheet.Cells(i, "M").value
        ProjectCycyle = sourceSheet.Cells(i, "N").value
        ProjectStatus = sourceSheet.Cells(i, "P").value
        
        targetSheet.Cells(lastRowTarget, "AB").value = ProjectContent
        targetSheet.Cells(lastRowTarget, "AC").value = ProjectCycyle
        targetSheet.Cells(lastRowTarget, "AD").value = ProjectStatus
        
        '其他补充信息，公式带入
        
        GeneralSupInfor = Replace(GeneralSupInfor, "var", CStr(lastRowTarget))
        DataAppSupInfor = Replace(DataAppSupInfor, "var", CStr(lastRowTarget))
        targetSheet.Cells(lastRowTarget, "AE").value = GeneralSupInfor
        targetSheet.Cells(lastRowTarget, "AF").value = DataAppSupInfor
        
        '销售信息
        '销售片区、销售区域、客户联系人、备注
        Dim SaleArea, SaleRegion, CustContact, Memo As String
        SaleArea = sourceSheet.Cells(i, "G").value
        SaleRegion = sourceSheet.Cells(i, "H").value
        CustContact = sourceSheet.Cells(i, "O").value
        Memo = sourceSheet.Cells(i, "Q").value
        
        targetSheet.Cells(lastRowTarget, "AG").value = SaleArea
        targetSheet.Cells(lastRowTarget, "AH").value = SaleRegion
        targetSheet.Cells(lastRowTarget, "AI").value = CustContact
        targetSheet.Cells(lastRowTarget, "AJ").value = Memo
        

        
    Next i
    
    
        
    '添加单元格边框,设置垂直居中，思源黑体字体10号
    With targetSheet.Range("A4" & ":AK" & lastRowTarget)
    .Borders.LineStyle = xlContinuous
    .Borders.Weight = xlThin
    .Font.Name = "思源黑体"
    .VerticalAlignment = xlVAlignCenter
    .Font.Size = 10
    '.Font.Color = black
    End With
    
    Debug.Print "案例清单处理完成"
    
    
    
    Application.ScreenUpdating = True '优化代码，屏幕不更新
    
End Sub



Sub test1()


Dim prodList As String
Dim test_value As String
test_value = ThisWorkbook.Sheets("映射配置信息").Cells(25, "C").value
prodList = get_prod_list2(test_value, 470) '167 389 445
Debug.Print prodList

End Sub


Private Function get_prod_list2(value As String, n As Long) As String
    Dim prodDict As Object
    Set prodDict = CreateObject("Scripting.Dictionary")
    
    Set sourceSheet = ThisWorkbook.Sheets("2014年以来的案例")
    Set targetSheet = ThisWorkbook.Sheets("案例清单")
    
    '定义一个字典存放数据集合，数据用|进行分隔
    Dim items() As String
    items = Split(value, "|") '分隔后的AA:1 BB:2
    
    Dim i As Long
    For i = 0 To UBound(items)
        '定义字典项 item作为子集合 用:进行分隔
        Dim item() As String
        item = Split(items(i), ":") '分隔后的AA 1 BB 2
        
        '定义列名
        Dim colName As String
        colName = item(0) 'AA
        
        '定义产品名
        Dim prodName As String
        prodName = item(1) '1
        
        
        '定义单元格内容
        Dim colValue As String
        colValue = sourceSheet.Cells(n, colName).value '第4行第AA列的内容
        
        '判断单元格内容
        If colValue = "√" Or colValue <> "" Then
            If Not prodDict.exists(prodName) Then
                prodDict.Add prodName, 1 '如果为√把对应配置好的产品名添加到列表中
            Else
                prodDict(prodName) = prodDict(prodName) + 1
            End If
        End If
    Next i
    
    
    '处理加工后的产品名称多个用逗号隔开
    Dim result As String
    For Each Key In prodDict.Keys
        If result = "" Then
            result = Key '& ": " & prodDict(Key)
        Else
            result = result & "," & Key '& ": " & prodDict(Key)
        End If
    Next Key
    
    ' 清空字典
    prodDict.RemoveAll
    
    get_prod_list2 = result
End Function


Private Function merge_markets(LineMarket As String, OtherLineMarket As String) As String
    Dim mergedMarket As String
    Dim lineMarkets() As String
    Dim otherMarkets() As String
    Dim market As Variant
    Dim dict As Object
    
    ' 初始化合并后的市场
    mergedMarket = LineMarket
    
    ' 分割 LineMarket 和 OtherLineMarket
    lineMarkets = Split(LineMarket, ",")
    otherMarkets = Split(OtherLineMarket, ",")
    
    ' 创建字典并将 LineMarket 中的元素添加到字典中
    Set dict = CreateObject("Scripting.Dictionary")
    For Each market In lineMarkets
        dict(Trim(market)) = True
    Next market
    
    ' 遍历 OtherLineMarket 中的元素，检查是否已存在于 LineMarket 中
    For Each market In otherMarkets
        If Not dict.exists(Trim(market)) Then
            ' 不存在则添加到合并后的市场
            mergedMarket = mergedMarket & "," & Trim(market)
        End If
    Next market
    
    ' 去除重复的逗号和空格
    mergedMarket = Trim(Replace(mergedMarket, ",,", ","))
    
    ' 返回合并后的市场
    merge_markets = mergedMarket
End Function




Private Function merge_line_markets(ByRef LineMarket As String, ByRef OtherLineMarket As String) As String
    Dim mergedLineMarket As String
    mergedLineMarket = LineMarket ' 初始化合并后的 LineMarket
    
    ' 将 LineMarket 和 OtherLineMarket 拼接为一个字符串
    Dim mergedMarket As String
    mergedMarket = LineMarket & "," & OtherLineMarket
    
    ' 去除重复的元素
    Dim uniqueMarkets() As String
    uniqueMarkets = Split(mergedMarket, ",")
    mergedLineMarket = Join(Application.Index(uniqueMarkets, 0, Evaluate("TRANSPOSE(IF(ISNUMBER(SEARCH("",""&"",""&" & Join(uniqueMarkets, ","" & ", "" & "")) & "", "" & "", """)),1,0)))"), ",")
    
    ' 返回合并后的 LineMarket
    merge_line_markets = mergedLineMarket
End Function







'处理CN和CO的特殊情况
Private Function get_prod_list1(value As String, m As String, n As Long) As String
    Dim prodDict As Object
    Set prodDict = CreateObject("Scripting.Dictionary")
    
    Set sourceSheet = ThisWorkbook.Sheets("2014年以来的案例")
    Set targetSheet = ThisWorkbook.Sheets("案例清单")
    
    '定义一个字典存放数据集合，数据用|进行分隔
    Dim items() As String
    items = Split(value, "|") '分隔后的AA:1 BB:2
    
    '定义单元格内容
    Dim colValue As String
    colValue = sourceSheet.Cells(n, m).value '第n行第m列的内容
    
    Dim i As Long
    For i = 0 To UBound(items)
        '定义字典项 item作为子集合 用:进行分隔
        Dim item() As String
        item = Split(items(i), ":") '分隔后的AA 1 BB 2
        
        '定义列名
        Dim colName As String
        StrName = item(0) 'AA
        
        '定义产品名
        Dim prodName As String
        prodName = item(1) '1
        
        

        
        '判断单元格内容
        If InStr(colValue, StrName) > 0 Then
            If Not prodDict.exists(prodName) Then
                prodDict.Add prodName, 1 '如果为√把对应配置好的产品名添加到列表中
            Else
                prodDict(prodName) = prodDict(prodName) + 1
            End If
        End If
    Next i
    
    
    '处理加工后的产品名称多个用逗号隔开
    Dim result As String
    For Each Key In prodDict.Keys
        If result = "" Then
            result = Key '& ": " & prodDict(Key)
        Else
            result = result & "," & Key '& ": " & prodDict(Key)
        End If
    Next Key
    
    ' 清空字典
    prodDict.RemoveAll
    
    '处理特殊情况包含分级分类的
    '判断源表格是否包含分级分类
    If InStr(colValue, "分级分类") > 0 Then
        '判断已有结果中是否有数据安全相关的内容，有则忽略，没有则拼接到最后
        If InStr(result, "数据安全管理模块") > 0 Then
        
        result = result
        
        ElseIf InStr(result, "数据安全管理模块") = 0 Then
        
        result = result & ",数据安全管理模块"
        
        End If
    
     ElseIf InStr(colValue, "分级分类") = 0 Then
     
     result = result
     
     End If
    
    
    get_prod_list1 = result
    
End Function





'判断是否函数
Private Function is_yes(cellValue As Variant) As String
    If cellValue = "√" Then
        is_yes = "是"
    Else
        is_yes = "否"
    End If
End Function
'处理金额字段
Private Function process_amount(cellValue As Variant) As String
    If IsEmpty(cellValue) Then
        process_amount = "未知"
        Exit Function
    End If
    
    Dim amount As Double
    amount = CDbl(ProcessData(cellValue))
    
    Select Case amount
        Case Is < 100
            process_amount = "<100"
        Case 100 To 200
            process_amount = "100-200"
        Case 200 To 300
            process_amount = "200-300"
        Case 300 To 400
            process_amount = "300-400"
        Case 400 To 500
            process_amount = "400-500"
        Case 500 To 600
            process_amount = "500-600"
        Case 600 To 700
            process_amount = "600-700"
        Case 700 To 800
            process_amount = "700-800"
        Case 800 To 900
            process_amount = "800-900"
        Case 900 To 1000
            process_amount = "900-1000"
        Case Is >= 1000
            process_amount = ">1000"
        Case Else
            process_amount = "未知"
    End Select
End Function


'处理名称简称

Private Function remove_company_suffix(cellValue As Variant) As String
    Dim newValue As String
    newValue = cellValue
    
    newValue = Replace(newValue, "有限责任公司", "")
    newValue = Replace(newValue, "有限公司", "")
    newValue = Replace(newValue, "股份有限公司", "")
    
    remove_company_suffix = Trim(newValue)
End Function

'公式替换变量
Private Function replace_number(formula As String, number As Integer) As String
    replace_number = Replace(formula, "var", CStr(number))
End Function
' 处理单元格的金额
Function ProcessData(ByVal data As Variant) As String
    Dim processedData As String
    Dim delimiter As String
    Dim segments() As String
    
    ' 去除中文字符、冒号和换行
    processedData = Trim(RegExpReplace(CStr(data), "[\u4e00-\u9fa5:：\n]", ""))
    
    ' 以 "-" 分隔数字
    segments = Split(processedData, "-")
    
    ' 取最后一个分隔符后的数字
    If UBound(segments) > 0 Then
        processedData = segments(UBound(segments))
    Else
        ' 直接返回去除中文字符后的数字
        processedData = Trim(RegExpReplace(processedData, "[^\d]+", ""))
    End If
    
    ProcessData = processedData
End Function

' 正则表达式替换函数
Function RegExpReplace(ByVal str As String, ByVal pattern As String, ByVal replaceWith As String) As String
    Dim regex As Object
    
    Set regex = CreateObject("VBScript.RegExp")
    regex.pattern = pattern
    regex.Global = True
    RegExpReplace = regex.Replace(str, replaceWith)
    
    Set regex = Nothing
End Function



