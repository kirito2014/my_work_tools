
    '***************************************************************************
    '版权所有 Sunline.ltd.co
    '功能：处理文件夹中的文件，将数据复制到目标表格中
    '简介：选择一个文件夹，遍历文件夹中的所有文件，将数据复制到目标表格中
    '使用方法：运行此宏
    '注意事项：
    '1. 需要引用Microsoft Scripting Runtime库
    '2. 必须将目标表格放在同一工作簿中，分别命名为"案例清单"、"通用补充信息"、"数据应用类补充信息"、"客户清单"
    '更新日期：2023/11/1
    '更新人员：战略创新部/王穆军
    '更新内容：
        '添加了Option Explicit来强制声明变量，避免拼写错误。
        '使用Application.ScreenUpdating将屏幕更新暂时关闭，以提高运行速度。
        '添加了注释来解释代码的功能和步骤。
        '使用With语句块来对目标表进行格式化，避免多次重复引用目标表。
        '对文件操作进行了优化，包括打开和关闭文件的位置。
        '使用Do While循环代替For Each循环，以提高运行速度。
		'添加了文档校验功能、名称去除换行
    '***************************************************************************



Option Explicit

Sub 主程序()
    Application.ScreenUpdating = False '优化代码，屏幕不更新
    
    Dim folderPath As String
    Dim fileDialog As fileDialog
    Dim selectedFolder As Variant
    Dim i As Long
    
    ' 选择要处理的文件夹
    Set fileDialog = Application.fileDialog(msoFileDialogFolderPicker)
    fileDialog.Title = "选择要处理的文件夹"
    
    If fileDialog.Show = -1 Then
        selectedFolder = fileDialog.SelectedItems(1)
    Else
        Exit Sub
    End If
    
    ' 检查文件夹中是否存在文件
    If Len(Dir(selectedFolder & "\*.xlsx")) = 0 Then
        MsgBox "没有要处理的文件，请确认文件是否存在！", vbInformation
        Exit Sub
    End If
    
    ' 设置目标文件夹路径和计数器初始值
    folderPath = selectedFolder
    i = 4
    
    ' 清空目标表的存量数据
    Dim wsTarget As Worksheet
    Dim lastRowTarget As Long
    Dim lastRowTarget_cust As Long
    
    Set wsTarget = ThisWorkbook.Sheets("案例清单")
    Dim wsTarget_sup As Worksheet
    Set wsTarget_sup = ThisWorkbook.Sheets("通用补充信息")
    Dim wsTarget_data As Worksheet
    Set wsTarget_data = ThisWorkbook.Sheets("数据应用类补充信息")
    Dim wsTarget_cust  As Worksheet
    Set wsTarget_cust = ThisWorkbook.Sheets("客户清单")
    
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).row
    lastRowTarget_cust = wsTarget_cust.Cells(wsTarget_cust.Rows.Count, "B").End(xlUp).row
    
    ' 清空目标工作表的数据
    If lastRowTarget > 4 Then
        wsTarget.Range("A4:AK" & lastRowTarget).ClearContents
        wsTarget_sup.Range("A4:V" & lastRowTarget).ClearContents
        wsTarget_data.Range("A4:H" & lastRowTarget).ClearContents
    End If
    '删除客户清单中的存量数据
    wsTarget_cust.Range("A2:B" & lastRowTarget_cust).ClearContents
    
    ' 遍历并打开文件夹中的所有文件
    Dim fileName As String
    fileName = Dir(folderPath & "\*.xlsx")
    
    Do While fileName <> ""
        ' 打开文件
        Dim wb As Workbook
        Set wb = Workbooks.Open(folderPath & "\" & fileName)
        
        Dim wsSource As Worksheet
        Set wsSource = wb.Sheets("案例调研")
        
        ' 将源表中的值复制到目标表中
        With wsTarget
        ' 检查A4单元格内容是否为空，单独一个检查，哪些没有进行标准填写检查todo
            If Not IsEmpty(wsSource.Range("A4").value) Then
                MsgBox "表格填写错误，请检查是否删除示例行。文件名：" & fileName, vbExclamation
                wb.Close SaveChanges:=False
                Exit Sub
            End If
            If IsEmpty(wsSource.Range("E3").value) Then
                Debug.Print "客户简称未填写。文件名：" & fileName
                'wb.Close SaveChanges:=False
                'Exit Sub
            End If

        
            .Range("A" & i).value = Replace(wsSource.Range("A3").value, vbLf, "")
            .Range("B" & i).value = wsSource.Range("B3").value
            .Range("C" & i).value = wsSource.Range("C3").value
            .Range("D" & i).value = wsSource.Range("D3").value
            ' 客户信息
            ' E列为调研模板获取其余为公式填充，后处理需要获取最大行填充，手动刷的话忽略处理
            .Range("E" & i).value = wsSource.Range("E3").value
            ' 归属信息
            ' 主管系统部
            .Range("J" & i).value = wsSource.Range("H3").value
            ' 分管系统部
            .Range("K" & i).value = wsSource.Range("I3").value
            ' 项目经理
            .Range("L" & i).value = wsSource.Range("J3").value
            ' 解决方案分类
            ' 数据架构咨询规划，数据挖掘模型
            .Range("N" & i).value = wsSource.Range("H17").value
            ' 数据平台类
            .Range("O" & i).value = wsSource.Range("H18").value
            ' 配套产品实施
            .Range("P" & i).value = wsSource.Range("H19").value
            ' 数据管理类
            ' 平台实施
            .Range("Q" & i).value = wsSource.Range("H20").value
            ' 咨询
            .Range("R" & i).value = wsSource.Range("H21").value
            ' 数据分析类
            ' 条线集市
            .Range("S" & i).value = wsSource.Range("H22").value
            ' 指标/标签
            .Range("T" & i).value = wsSource.Range("H23").value
            ' 数据服务
            .Range("U" & i).value = wsSource.Range("H24").value
            ' 数据应用类
            ' 经营分析类
            .Range("V" & i).value = wsSource.Range("H25").value
            ' 客户管理
            .Range("W" & i).value = wsSource.Range("H26").value
            ' 风险管理
            .Range("X" & i).value = wsSource.Range("H27").value
            ' 监管报送
            .Range("Y" & i).value = wsSource.Range("H28").value
            ' 业务系统
            .Range("Z" & i).value = wsSource.Range("H29").value
            ' 代销
            .Range("AA" & i).value = wsSource.Range("H30").value
            ' 项目补充信息
            ' 项目实施内容
            .Range("AB" & i).value = wsSource.Range("A6").value
            ' 项目实施周期
            .Range("AC" & i).value = wsSource.Range("L3").value
            ' 项目实施状态
            .Range("AD" & i).value = wsSource.Range("M3").value
            ' 其他补充信息
            ' 通用补充信息（填充公式）
            Dim formula_general As String
            Dim formula_supply As String
            Dim formula_total As String
            formula_general = Replace("=IFERROR(HYPERLINK(""#'通用补充信息'!A""&MATCH(Avar, 通用补充信息!A:A, 0), "">>>通用补充信息<<<""), "">>>暂无补充<<<"")", "var", i)
            formula_supply = Replace("=IFERROR(HYPERLINK(""#'数据应用类补充信息'!A""&MATCH(Avar, 数据应用类补充信息!A:A, 0), "">>>数据应用类补充信息<<<""), "">>>暂无补充<<<"")", "var", i)
            'formula_total = Replace("=Nvar&IF(ISBLANK(Nvar),"",", ")&Ovar&IF(ISBLANK(Ovar),"",", ")&Pvar&IF(ISBLANK(Pvar),"",", ")&Qvar&IF(ISBLANK(Qvar),"",", ")&Rvar&IF(ISBLANK(Rvar),"",", ")&Svar&IF(ISBLANK(Svar),"",", ")&Tvar&IF(ISBLANK(Tvar),"",", ")&Uvar&IF(ISBLANK(Uvar),"",", ")&Vvar&IF(ISBLANK(Vvar),"",", ")&Wvar&IF(ISBLANK(Wvar),"",", ")&Xvar&IF(ISBLANK(Xvar),"",", ")&Yvar&IF(ISBLANK(Yvar),"",", ")&Zvar&IF(ISBLANK(Zvar),"",", ")&AAvar", "var", i)
            .Range("AE" & i).formula = formula_general
            .Range("AF" & i).formula = formula_supply
            '.Range("M" & i).formula = formula_total
            ' 销售信息
            ' 销售片区 TODO
            ' 销售区域
            .Range("AH" & i).value = wsSource.Range("K3").value
            ' 客户联系人
            .Range("AI" & i).value = wsSource.Range("F3").value
            ' 备注 无映射 TODO
            'Debug.Print "案例清单更新完成"
        End With

        ' 合同名称
        With wsTarget_sup
            .Range("A" & i).value = wsSource.Range("A3").value
            ' 数据库类型
            .Range("B" & i).value = wsSource.Range("A35").value
            ' 数据库版本号
            .Range("C" & i).value = wsSource.Range("B35").value
            ' 节点数
            .Range("D" & i).value = wsSource.Range("C35").value
            ' 代理厂商
            .Range("E" & i).value = wsSource.Range("D35").value
            .Range("F" & i).value = wsSource.Range("E35").value
            .Range("G" & i).value = wsSource.Range("F35").value
            .Range("H" & i).value = wsSource.Range("G35").value
            .Range("I" & i).value = wsSource.Range("H35").value
            .Range("J" & i).value = wsSource.Range("I35").value
            .Range("K" & i).value = wsSource.Range("J35").value
            .Range("L" & i).value = wsSource.Range("K35").value
            .Range("M" & i).value = wsSource.Range("L35").value
            .Range("N" & i).value = wsSource.Range("M35").value
            ' 上下游信息
            .Range("P" & i).value = wsSource.Range("A41").value
            .Range("Q" & i).value = wsSource.Range("B41").value
            .Range("R" & i).value = wsSource.Range("D41").value
            ' 数据库迁移
            .Range("S" & i).value = wsSource.Range("E41").value
            .Range("T" & i).value = wsSource.Range("F41").value
            .Range("U" & i).value = wsSource.Range("G41").value
            'Debug.Print "通用补充信息"
        End With

        ' 数据应用类补充信息
        With wsTarget_data
            ' 合同名称
            .Range("A" & i).value = wsSource.Range("A3").value
            ' 硬件、软件
            .Range("B" & i).value = wsSource.Range("H41").value
            .Range("C" & i).value = wsSource.Range("I41").value
            .Range("D" & i).value = wsSource.Range("J41").value
            .Range("E" & i).value = wsSource.Range("K41").value
            ' 数据源
            .Range("F" & i).value = wsSource.Range("C41").value
            .Range("G" & i).value = wsSource.Range("L41").value
            'Debug.Print "数据应用类补充信息"
        End With
        
        
        '客户清单
        '使用具有描述性名称的变量来提高可读性?
        '使用Set语句将目标表的工作表对象分配给变量，以避免重复引用。
        '将公式字符串分配给变量，以避免重复编写相同的公式。
        '使用单引号将工作表名称括起来，以避免工作表名称中包含特殊字符时出现错误。
        '将公式分配给目标表的单元格，而不是逐个赋值。
        '结果进行去重删除空白单元格
                
        With wsTarget_cust
            ' 客户简称
            .Range("B" & i - 2).value = wsSource.Range("E3").value
            
            ' 客户类型和客户细类
            Dim formula_CustType As String
            Dim formula_CustClass As String
            Dim lookupSheet As Worksheet
            Set lookupSheet = ThisWorkbook.Sheets("附1-客户类型总览")
            
            ' 填充公式
            formula_CustType = "=VLOOKUP(C" & i - 2 & ",'" & lookupSheet.Name & "'!A:B,2,0)"
            formula_CustClass = "=VLOOKUP(C" & i - 2 & ",'" & lookupSheet.Name & "'!A:C,3,0)"
            
            .Range("D" & i - 2).formula = formula_CustType
            .Range("E" & i - 2).formula = formula_CustClass
        End With


        ' 关闭文件
        wb.Close SaveChanges:=False

        ' 计数器加一
        i = i + 1

        ' 获取下一个文件名
        fileName = Dir
    Loop

    ' 对目标表进行格式化
    lastRowTarget = wsTarget.Cells(wsTarget.Rows.Count, "A").End(xlUp).row
    lastRowTarget_cust = wsTarget_cust.Cells(wsTarget_cust.Rows.Count, "B").End(xlUp).row
    
    With wsTarget.Range("A4:AK" & lastRowTarget)
        .Borders.LineStyle = xlContinuous
        .Borders.Weight = xlThin
        .Font.Name = "微软雅黑"
        .Font.Size = 10
        .RowHeight = 15
        
        ' 获取最大行
        Dim targetRange As Range
        Set targetRange = wsTarget.Range("A4:AK" & lastRowTarget)
        
        ' 客户细型
        Dim formula_Custind As String
        ' 客户类型
        'Dim formula_CustType As String
        ' 客户大类
        'Dim formula_CustClass As String
        ' 客户资产规模
        Dim formula_CustAssetScale As String
        
        'Dim lookupSheet As Worksheet
        Set lookupSheet = ThisWorkbook.Sheets("客户清单")
        
        ' 填充公式
        formula_Custind = "=iferror(VLOOKUP($E4,'" & lookupSheet.Name & "'!B:G,2,0),"""")"
        formula_CustType = "=iferror(VLOOKUP($E4,'" & lookupSheet.Name & "'!B:G,3,0),"""")"
        formula_CustClass = "=iferror(VLOOKUP($E4,'" & lookupSheet.Name & "'!B:G,4,0),"""")"
        formula_CustAssetScale = "=IF(VLOOKUP($E4,'" & lookupSheet.Name & "'!B:G,6,0)=0,""<暂未更新>"",VLOOKUP($E4,'" & lookupSheet.Name & "'!B:G,6,0))"
        
        ' 填充公式到整个范围
        ' 填充公式到第四行
        .Range("F1").formula = formula_Custind
        .Range("G1").formula = formula_CustType
        .Range("H1").formula = formula_CustClass
        .Range("I1").formula = formula_CustAssetScale
        
        
        .Range("F1:F" & lastRowTarget - 3).formula = .Range("F1").formula
        .Range("G1:G" & lastRowTarget - 3).formula = .Range("G1").formula
        .Range("H1:H" & lastRowTarget - 3).formula = .Range("H1").formula
        .Range("I1:I" & lastRowTarget - 3).formula = .Range("I1").formula
        
        
    End With



    With wsTarget_data.Range("A4:H" & lastRowTarget)
        .Borders.LineStyle = xlContinuous
        .Borders.Weight = xlThin
        .Font.Name = "微软雅黑"
        .Font.Size = 10
        .RowHeight = 15
    End With

    With wsTarget_sup.Range("A4:V" & lastRowTarget)
        .Borders.LineStyle = xlContinuous
        .Borders.Weight = xlThin
        .Font.Name = "微软雅黑"
        .Font.Size = 10
        .RowHeight = 15
    End With
    
    With wsTarget_cust.Range("A2:I" & lastRowTarget_cust)
    '获取最大行
        'lastRowTarget = .Cells(.Rows.Count, "B").End(xlUp).Row
        .Borders.LineStyle = xlContinuous
        .Borders.Weight = xlThin
        .Font.Name = "微软雅黑"
        .Font.Size = 10
        .RowHeight = 15
            ' 在去重前获取目标表的范围
        'Dim targetRange As Range
        Set targetRange = wsTarget_cust.Range("A2:I" & lastRowTarget_cust)
            
            ' 进行去重
        targetRange.RemoveDuplicates columns:=Array(2, 3, 4, 5, 6, 7, 8, 9), Header:=xlYes
            
            ' 删除空行
        Dim emptyRow As Range
        For Each emptyRow In targetRange.Rows
            If Application.WorksheetFunction.CountA(emptyRow) = 0 Then
                    emptyRow.Delete
            End If
        Next emptyRow
                  
    End With

    ' 提示处理完成
    MsgBox "处理已完成", vbInformation

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
            If Not prodDict.Exists(prodName) Then
                prodDict.Add prodName, 1 '如果为√把对应配置好的产品名添加到列表中
            Else
                prodDict(prodName) = prodDict(prodName) + 1
            End If
        End If
    Next i
    
    
    '处理加工后的产品名称多个用逗号隔开
    Dim result As String
    For Each key In prodDict.Keys
        If result = "" Then
            result = key '& ": " & prodDict(Key)
        Else
            result = result & "," & key '& ": " & prodDict(Key)
        End If
    Next key
    
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
        If Not dict.Exists(Trim(market)) Then
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
            If Not prodDict.Exists(prodName) Then
                prodDict.Add prodName, 1 '如果为√把对应配置好的产品名添加到列表中
            Else
                prodDict(prodName) = prodDict(prodName) + 1
            End If
        End If
    Next i
    
    
    '处理加工后的产品名称多个用逗号隔开
    Dim result As String
    For Each key In prodDict.Keys
        If result = "" Then
            result = key '& ": " & prodDict(Key)
        Else
            result = result & "," & key '& ": " & prodDict(Key)
        End If
    Next key
    
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



Sub 检查文件格式()
    Dim folderPath As String
    Dim fileName As Variant
    Dim dict As Object
    Set dict = CreateObject("Scripting.Dictionary")
    Application.ScreenUpdating = False '优化代码，屏幕更新
    
    ' 提示用户选择文件夹
    With Application.fileDialog(msoFileDialogFolderPicker)
        If .Show = -1 Then
            folderPath = .SelectedItems(1)
        Else
            Exit Sub
        End If
    End With
    
    ' 遍历文件夹下的所有.xlsx文件
    fileName = Dir(folderPath & "\*.xlsx")
    Do While fileName <> ""
        Dim wb As Workbook
        Set wb = Workbooks.Open(folderPath & "\" & fileName)
        
        ' 检查A4单元格是否为空
        If Not IsEmpty(wb.Sheets("案例调研").Range("A4").value) Then
            ' 如果不为空，将文件路径与文件名添加到字典中
            dict.Add fileName, folderPath
        End If
        
        ' 关闭工作簿
        wb.Close SaveChanges:=False
        fileName = Dir
    Loop
    
    ' 创建格式校验工作表
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Sheets.Add
    ws.Name = "格式校验"
    ws.Range("A1").value = "序号"
    ws.Range("B1").value = "文件目录与文件名"
    
    ' 将文件路径与文件名添加到格式校验工作表
    Dim row As Long
    row = 2
    For Each fileName In dict.Keys
        ws.Cells(row, 1).value = row - 1
        ws.Cells(row, 2).value = dict(fileName) & "\" & Replace(fileName, " ", "%20") ' 使用编码版本替换空格
        row = row + 1
    Next fileName
    
    ' 清除字典
    Set dict = Nothing
    Application.ScreenUpdating = True '优化代码，屏幕更新
End Sub

