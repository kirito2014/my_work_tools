 
 
 '/简历生成工具使用宏代码
 '/编写人：  王穆军/战略创新部
 '/编写日期：2023/10/23
 '/更新日期：2023/11/1
 '/更新记录：
 'v0.1 2023/09/12 框架搭建
 'v1.0 2023/09/25 主体代码编写完成
 'v1.1 2023/10/17 添加检验简历代码
 'v1.2 2023/10/20 模板更新修改简历项目生成方式，添加第一学历最高学历语句
 'v1.3 2023/10/23 格式调整，修改bug,新增格式修改代码,添加技能标签
 'v1.4 2023/10/27 修复bug 添加工作与项目经历少于10条排序，新增日期检验规则，日期前小后大，工作日期与项目日期验证
 'v1.5 2023/11/1  添加超过10家公司任职下最早工作日期的获取与项目经历判定逻辑，添加跨项目经历日期判定
 'v1.6 2023/11/9  工作经历大于10条的情况 存在第二第三学历的情况判断
 
 
 Sub 主程序()
    Dim userChoice As VbMsgBoxResult
    
    ' 弹出消息框，让用户选择操作
    userChoice = MsgBox("是否执行检核程序: " + vbLf + "是则检核,否则直接生成 ", vbYesNoCancel + vbQuestion, "操作选择")
    
    ' 根据用户选择执行相应的操作
    If userChoice = vbYes Then
        ' 用户选择 "是"，执行 ValidateAndDisplayData 子过程
        Call ValidateAndDisplayData

    ElseIf userChoice = vbNo Then
        ' 用户选择 "否"，执行 worksheetsgenerate 子过程
        Call worksheetsgenerate


        
    Else
        ' 用户选择 "取消"，退出程序
        Exit Sub
    End If
End Sub

 
 Private Sub worksheetsgenerate()
    ' 关闭屏幕刷新，提升运行效率
    Application.ScreenUpdating = False
    ' 关闭窗口提示
    Application.DisplayAlerts = False

    Const SOURCE_SHEET_NAME As String = "工作表2"
    Const TEMPLATE_SHEET_NAME As String = "Template"
    Const DATA_SHEET_NAME As String = "ProcessedData"
    
    ' 检查文件夹是否存在，如果存在则删除
    Dim folderPath As String
    folderPath = ThisWorkbook.Path & "\简历_" & Format(Date, "yyyymmdd")
    
    If Dir(folderPath, vbDirectory) <> "" Then
        On Error Resume Next
        Kill folderPath & "\*.*"
        RmDir folderPath
        On Error GoTo 0
    End If
    
    ' 创建新文件夹
    MkDir folderPath
     
 
 
    ' 获取当前工作表

    Dim sourcews As Worksheet
    Set sourcews = ThisWorkbook.Worksheets(SOURCE_SHEET_NAME)
    
    '前置数据处理，对原表格数据经进行处理，删除检核sheet
    
    Call DeleteValidationResultSheetIfExists
    
    Call CleanYearFromCell
    
    

    ' 获取当前工作表的A列的最大行
    Dim lastRow As Long
    lastRow = GetMaxRowInColumnAWithMerge(sourcews)

    ' 初始化循环索引
    Dim i As Long
    i = 2 ' 从第2行开始 去除标题行

    Do While i <= lastRow
        ' 获取当前人员的起始行和结束行
        Dim startrow As Long
        Dim endrow As Long
        Dim targetws As Worksheet
        Dim empname As String
        
        ' 复制Template到最后并设置为目标sheet
        Sheets(TEMPLATE_SHEET_NAME).Copy After:=Sheets(Sheets.Count)
        
        Set targetws = ThisWorkbook.Worksheets(Sheets.Count)
        
        
        startrow = i
        endrows = GetMergedCellRangeRows(sourcews, i)
        endrow = endrows(1)
        

        ' 重命名新增工作表名称，重命名为当前简历人名称
        empname = sourcews.Range("A" & startrow).Value
        targetws.Name = empname
        Set targetws = ThisWorkbook.Worksheets(empname)

        ' 填写个人信息
        targetws.Cells(3, 3).Value = sourcews.Range("E" & startrow).Value ' 姓名
        targetws.Cells(3, 5).Value = sourcews.Range("F" & startrow).Value ' 工作年限
        
        '判断并校验 工作年限是否符合要求 即判断毕业时间与当前日期差数
        '判断最早的工作日期是否早于毕业时间
        
        'todo 毕业信息需要进行特殊处理 ，如果是多学历则进行拼接
        '判断 j 和 l 列是否都存在，如果都存在判断 j 和 l 是否相同，相同则targetws.Cells(4, 5).Value =sourcews.Range("H" & startrow).Value
        '否则按照 targetws.Cells(4, 5).Value =  "最高学历：" & sourcews.Range("H" & startrow).Value & chr(13) & "第一学历:" &  sourcews.Range("J" & startrow).Value 的格式进行拼接
        '同理处理 GI 列
        '存储教育经历的字符串
        
        Dim education_date As String
        Dim education_school As String
        Dim education_major As String
        
        '初始化为空值
         education_date = ""
         education_school = ""
         education_major = ""
        
        '判断有第一学历存在的情况
        If Trim(sourcews.Range("K" & startrow).Value) <> "" Then
        
            If Trim(sourcews.Range("H" & startrow).Value) <> Trim(sourcews.Range("K" & startrow).Value) Then
            
                education_date = "最高学历：" & Format(sourcews.Range("H" & startrow).Value, "yyyy年mm月") + Chr(13) + "第一学历:" & Format(sourcews.Range("K" & startrow).Value, "yyyy年mm月") + Chr(13)
                education_school = "最高学历：" & sourcews.Range("I" & startrow).Value + Chr(13) + "第一学历:" & sourcews.Range("L" & startrow).Value + Chr(13)
                education_major = "最高学历：" & sourcews.Range("J" & startrow).Value + Chr(13) + "第一学历:" & sourcews.Range("M" & startrow).Value + Chr(13)
            
            Else
                education_date = sourcews.Range("H" & startrow).Value  '毕业时间(单一学历，但填了两边)
                education_school = sourcews.Range("I" & startrow).Value    '毕业学校
                education_major = sourcews.Range("J" & startrow).Value    '专业
            End If
        Else
            education_date = sourcews.Range("H" & startrow).Value  '毕业时间(单一学历)
            education_school = sourcews.Range("I" & startrow).Value  '毕业学校
            education_major = sourcews.Range("J" & startrow).Value   '专业
        End If
        
        '判断有第二第三学历存在的情况
        '判断第二学历是否为空
        If IsEmpty(sourcews.Range("AY" & startrow).Value) = False Then
        
            education_date = education_date & "第二学历：" & Format(sourcews.Range("AY" & startrow).Value, "yyyy年mm月") + Chr(13)
            education_school = education_school & "第二学历:" & sourcews.Range("AZ" & startrow).Value + Chr(13)
            education_major = education_major & "第二学历:" & sourcews.Range("BA" & startrow).Value + Chr(13)
             '判断第三学历是否存在
            If IsEmpty(sourcews.Range("BB" & startrow).Value) = False Then
            
                education_date = education_date & "第三学历：" & Format(sourcews.Range("BB" & startrow).Value, "yyyy年mm月")
                education_school = education_school & "第三学历:" & sourcews.Range("BC" & startrow).Value
                education_major = education_major & "第三学历:" & sourcews.Range("BD" & startrow).Value
            Else
            
                targetws.Cells(4, 3).Value = education_date
                targetws.Cells(4, 5).Value = education_school
                targetws.Cells(5, 3).Value = education_major
                
            End If
        
        '为空的情况下
        Else
            education_date = education_date
            education_school = education_school
            education_major = education_major
        
        End If
    
        targetws.Cells(4, 3).Value = education_date
        targetws.Cells(4, 5).Value = education_school
        targetws.Cells(5, 3).Value = education_major
    
    

        targetws.Cells(5, 5).Value = sourcews.Range("G" & startrow).Value '最高学历
        '所在部门去除英文后缀名称
        
        targetws.Cells(6, 3).Value = Trim(RemoveEnglishCharacters(sourcews.Range("N" & startrow).Value))   '所在部门
        targetws.Cells(6, 5).Value = sourcews.Range("O" & startrow).Value '职称
        targetws.Cells(7, 3).Value = sourcews.Range("P" & startrow).Value '简介
        
        '填写能力与资质信息
        targetws.Cells(15, 3).Value = sourcews.Range("AP" & startrow).Value '业务技术能力
        targetws.Cells(16, 3).Value = sourcews.Range("AQ" & startrow).Value '资质认证
        targetws.Cells(17, 3).Value = sourcews.Range("AR" & startrow).Value '参与培训
        targetws.Cells(18, 3).Value = Replace(sourcews.Range("AS" & startrow).Value, ",", Chr(13)) '技能标签
        
        ' 其他个人信息的填写类似，根据单元格位置和目标工作表位置进行修改

        ' 填写工作经历,判断最大行,根据实际的工作条数进行添加
        Dim workExpStartRow As Long
        Dim workExpEndRow As Long
        Dim maxnoemptyrow As Long '最大非空行，用以判断非空行
        workExpStartRow = startrow ' 工作经历从当前人员的开始行开始
        workExpEndRow = endrow ' 工作经历到当前人员的结束行结束
        
        '获取最大行判断最大行，判断其他行行数并排序 判断最大行 粘贴到原表格
        
        
        'Rows(10).Select
        'Rows(Selection.row & ":" & (Selection.row + 5)).Insert Shift:=xlDown
        maxnoemptyrow = FindMaxNonEmptyRow(sourcews, workExpStartRow, workExpEndRow)
        Debug.Print "工作经历行最大行非空" & maxnoemptyrow
        
        '/////////判断逻辑 获取到最大行不一定是工作经历的行数，所以需要判断最大非空行来确定有多少行经历，
        '如果多余2条以上那就需要新增行然后再进行数据处理////////////
        '如从第6行到14行为当前人的数据行，但实际上工作经历只有两行，使用FindMaxNonEmptyRow 能检查出最大行
        
        Dim workexps As Long
        
        workexps = maxnoemptyrow - workExpStartRow + 1
        
        Dim newSheet As Worksheet
        Set newSheet = ThisWorkbook.Sheets.Add
        newSheet.Name = "ProcessedData_work"
        
        If workExpEndRow >= maxnoemptyrow Then
             If workexps > 1 And workexps <= 10 Then  ' 实际行数大于1需要在第10行下新增对应行数 需要对完成的内容进行排序
                
                '新增行数
                targetws.Activate
                targetws.Range("B9").Select
                ActiveCell.EntireRow.Offset(1).Resize(workexps - 1).Insert Shift:=xlDown

                '对应数据处理,新增一个sheet用于排序
                

                
                ' 复制数据和格式到新sheet页

                sourcews.Range("Q" & startrow & ":U" & maxnoemptyrow).Copy
                newSheet.Range("A1").PasteSpecial Paste:=xlPasteValuesAndNumberFormats
                
                '获取粘贴完成后的最大行
    
                lastRow_new = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                
                '修改单元格格式
                
                newSheet.Range("A1:A" & lastRow_new).NumberFormat = "yyyy/mm"
                newSheet.Range("B1:B" & lastRow_new).NumberFormat = "yyyy/mm"
                newSheet.Columns("E:E").ColumnWidth = 15
                newSheet.Columns("E:E").WrapText = True
                
                ' 循环设置日期格式
            
                For i = 1 To lastRow_new
                    newSheet.Cells(i, "A").Value = Format(newSheet.Cells(i, "A").Value, "yyyy/mm")
                    'newSheet.Cells(i, "B").Value = Format(newSheet.Cells(i, "B").Value, "yyyy/mm")
                Next i
            
                ' 对新工作表的数据按 A 列的日期从近到远排序
                newSheet.Sort.SortFields.Clear
                newSheet.Sort.SortFields.Add Key:=newSheet.Range("A1:A" & lastRow_new), _
                    SortOn:=xlSortOnValues, Order:=xlDescending, DataOption:=xlSortNormal
                With newSheet.Sort
                    .SetRange newSheet.Range("A1:E" & lastRow_new)
                    .Header = xlNo
                    .MatchCase = False
                    .Orientation = xlTopToBottom
                    .SortMethod = xlPinYin
                    .Apply
                End With
                
                newSheet.Range("A1:E" & lastRow_new).Copy
                
                targetws.Range("B10").PasteSpecial Paste:=xlPasteValuesAndNumberFormats
    
                ' 清除剪贴板
                Application.CutCopyMode = False
                newSheet.Delete
                
             '当=10行且有额外的工作经历
             ElseIf workexps = 10 And IsEmpty(sourcews.Range("AT" & startrow).Value) = False Then
             
                '对应数据处理,新增一个sheet用于排序
                
                
                'Dim newSheet As Worksheet
                'Set newSheet = ThisWorkbook.Sheets.Add
                'newSheet.Name = "ProcessedData_work"
                
                ' 复制数据和格式到新sheet页

                sourcews.Range("Q" & startrow & ":U" & maxnoemptyrow).Copy
                newSheet.Range("A1").PasteSpecial Paste:=xlPasteValuesAndNumberFormats

                
                '判断AT行最大行
                
                lastrow_AT = FindMaxNonEmptyRow(sourcews, startrow, endrow, "AT")
                
                sourcews.Range("AT" & startrow & ":AX" & lastrow_AT).Copy newSheet.Cells(11, 1)
                
                '获取粘贴完成后的最大行,获取AT行最大行
                
                lastRow_new = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                
                '修改单元格格式
                
                newSheet.Range("A1:A" & lastRow_new).NumberFormat = "yyyy/mm"
                newSheet.Range("B1:B" & lastRow_new).NumberFormat = "yyyy/mm"
                newSheet.Columns("E:E").ColumnWidth = 15
                newSheet.Columns("E:E").WrapText = True
                
                ' 循环设置日期格式
            
                For i = 1 To lastRow_new
                    newSheet.Cells(i, "A").Value = Format(newSheet.Cells(i, "A").Value, "yyyy/mm")
                    'newSheet.Cells(i, "B").Value = Format(newSheet.Cells(i, "B").Value, "yyyy/mm")
                Next i
            
                ' 对新工作表的数据按 A 列的日期从近到远排序
                newSheet.Sort.SortFields.Clear
                newSheet.Sort.SortFields.Add Key:=newSheet.Range("A1:A" & lastRow_new), _
                    SortOn:=xlSortOnValues, Order:=xlDescending, DataOption:=xlSortNormal
                With newSheet.Sort
                    .SetRange newSheet.Range("A1:E" & lastRow_new)
                    .Header = xlNo
                    .MatchCase = False
                    .Orientation = xlTopToBottom
                    .SortMethod = xlPinYin
                    .Apply
                End With
                
                '添加条数
                targetws.Activate
                targetws.Range("B9").Select
                ActiveCell.EntireRow.Offset(1).Resize(lastRow_new - 1).Insert Shift:=xlDown
                
                '数据粘贴
                newSheet.Range("A1:E" & lastRow_new).Copy
                targetws.Range("B10").PasteSpecial Paste:=xlPasteValuesAndNumberFormats
    
                ' 清除剪贴板
                Application.CutCopyMode = False
                newSheet.Delete
             
             '只有一次工作经历 不新增行数直接在当前表格第十行添加数据,免去排序的步骤节省系统资源
             ElseIf workexps = 1 Then
             
                sourcews.Range("Q" & startrow & ":U" & startrow).Copy
                targetws.Range("B10").PasteSpecial Paste:=xlPasteValuesAndNumberFormats
    
                ' 清除剪贴板
                Application.CutCopyMode = False
                newSheet.Delete
                
             End If
            
        'ElseIf workExpEndRow - workExpStartRow + 1 >= maxnoemptyrow Then
        
        End If
        
        
        
        '从这里开始修改项目经历判断，如果存在多行 > 10 的情况下 先整合项目经历到新sheet页
        
        Call get_project_info(startrow, endrow)
        
        Dim dataWS As Worksheet
        'Dim datalastrow As Long
        Set dataWS = ThisWorkbook.Worksheets(DATA_SHEET_NAME)

        maxnoemptyrow = dataWS.Cells(dataWS.Rows.Count, "A").End(xlUp).Row
        Debug.Print "项目经历行最大行非空" & maxnoemptyrow

        Dim projectexps As Long
        Dim startdaterow As Long

                
        projectexps = maxnoemptyrow
        
        '找目标工作表项目经历开始行
        
        startdaterow = FindSecondStartTimeRow(targetws)
        
         If maxnoemptyrow > 1 Then   ' 实际行数大于1需要新增对应行数
            
            '新增行数
            targetws.Activate
            
            targetws.Cells(startdaterow, 2).Select
            ActiveCell.EntireRow.Offset(1).Resize(projectexps - 1).Insert Shift:=xlDown
            '对应数据处理
            '复制数据和格式
            dataWS.Range("A1" & ":E" & maxnoemptyrow).Copy

            
            'targetws.Range("B" & startdaterow).PasteSpecial Paste:=xlPasteAllUsingSourceTheme, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
            targetws.Range("B" & startdaterow).PasteSpecial Paste:=xlPasteValuesAndNumberFormats

            ' 清除剪贴板
            Application.CutCopyMode = False
         
         ElseIf projectexps = 1 Then   '只有一次工作经历 不新增行数直接在当前表格第十行添加数据
   
            sourcews.Range("Q" & startrow & ":U" & startrow).Copy
            targetws.Range("B" & startdaterow).PasteSpecial Paste:=xlPasteValuesAndNumberFormats

            ' 清除剪贴板
            Application.CutCopyMode = False
         
         End If
             
        Application.DisplayAlerts = False ' 禁用警告提示
         '删除生成的表格
        
        ThisWorkbook.Sheets(DATA_SHEET_NAME).Delete
        
        ' 优化格式内容

        
        '将当前处理人员的表格复制粘贴到docx文件并关闭
        ' 将sheet页保存到docx文件中并以 个人简历_明细_yyyymmdd 格式命名
        
        '删除生成的sheet页
        
        Call CreateVisibleWordAppAndSaveAsDocx(empname, folderPath)
        
        
        
        ' 子循环处理工作经历用于跳出循环
        For j = workExpStartRow To workExpEndRow
            ' 填写工作经历的信息，类似于个人信息的填写方式，todo如果项目经历工作经历行数大于已有行数需要新增行数
            'Debug.Print "12211"
            ' 可以根据单元格位置和目标工作表位置进行修改
            ' 这里可以加入判断，如果当前行不是合并单元格的第一行则跳出循环
            '处理完工作经历跳出循环
            If j >= workExpEndRow And sourcews.Range("A" & j).MergeCells Then
                Exit For
              
            End If
        Next j



        ' 更新循环索引到下一个人的起始行
        i = endrow + 1
    Loop

    
    '将生成的sheet页进行删除,删除除源数据和
    
    Call DeleteOtherSheets
    
    Call 简历格式优化
    
    MsgBox "已完成", vbInformation
    
    ' 开启屏幕刷新
    Application.ScreenUpdating = True
    ' 开启窗口提示
    Application.DisplayAlerts = True
End Sub




Private Sub DeleteValidationResultSheetIfExists()
    Dim ws As Worksheet
    Dim sheetExists As Boolean
    
    sheetExists = False
    
    ' 检查是否存在名为 "表格检验结果" 的工作表
    For Each ws In ThisWorkbook.Sheets
        If ws.Name = "表格检验结果" Then
            sheetExists = True
            Exit For
        End If
    Next ws
    
    ' 如果工作表存在，则删除它
    If sheetExists Then
        Application.DisplayAlerts = False ' 禁用警告框，以免删除时出现确认提示
        ThisWorkbook.Sheets("表格检验结果").Delete
        Application.DisplayAlerts = True ' 恢复警告框
    End If
End Sub



Private Sub ValidateAndDisplayData()


    ' 关闭屏幕刷新，提升运行效率
    Application.ScreenUpdating = False
    ' 关闭窗口提示
    Application.DisplayAlerts = False

    Dim sourcews As Worksheet
    Dim dataWS As Worksheet
    Dim newSheet As Worksheet
    Dim startrow As Long
    Dim lastRow As Long
    Dim i As Long
    Dim graduationDate As Date '毕业日期
    Dim startDate As Date
    Dim yearsOfWork As String
    Dim yearsOfWork_m As String
    Dim validationResults As New Scripting.Dictionary ' 创建一个字典来存储校验结果
    
    
    ' 选择你的工作表
    Set sourcews = ThisWorkbook.Worksheets("工作表2") ' 将"Sheet1"替换为你的工作表名称

 
    
    Call DeleteValidationResultSheetIfExists
    
    ' 获取A列的最大行数包括合并单元格 实际物理表格最大行
    
    lastRow = GetMaxRowInColumnAWithMerge(sourcews)
    
     ' 初始化循环索引
    
    i = 2 ' 从第2行开始
    k = 1

    Do While i <= lastRow
    
        ' 获取当前人员的起始行和结束行
        
        'Dim startrow As Long
        
        Dim endrow As Long
        
        Dim targetws As Worksheet
        
        Dim empname As String
        

        startrow = i
        
        '获取当前人员的开始行和结束行 不用于计算工作经历日期
        
        endrows = GetMergedCellRangeRows(sourcews, i)
        
        startrow = endrows(0) '开始行
        
        endrow = endrows(1) ' 结束行
        

        ' 个人信息日期信息获取
        
        empname = sourcews.Range("A" & startrow).Text ' 姓名
        
        yearsOfWork = sourcews.Range("F" & startrow).Text ' 工作年限
        
        graduationDate = sourcews.Range("H" & startrow).Text ' 毕业时间
                
        ' 获取工作经历,判断最大行
        
        Dim workExpStartRow As Long
        
        Dim workExpEndRow As Long
        
        Dim maxnoemptyrow As Long '最大非空行，用以判断非空行
        
        workExpStartRow = startrow ' 工作经历从当前人员的开始行开始
        
        workExpEndRow = endrow ' 工作经历到当前人员的结束行结束
        
        maxnoemptyrow = FindMaxNonEmptyRow(sourcews, workExpStartRow, workExpEndRow)
        
        

        
        '获取额外的工作经历是否存在
        
        extraflag = FindMaxNonEmptyRow(sourcews, startrow, endrow, "AT")
        
        If extraflag <> 0 Then
            '当不为零的时候取AT startrow -AT extraflag 的开始日期
            startDate = FindEarliestDate(sourcews, startrow, maxnoemptyrow, maxnoemptyrow, extraflag)
            
            '当为零时表示没有任职超过10家以上有额外的数据内容
        Else
        
            startDate = FindEarliestDate(sourcews, startrow, maxnoemptyrow)  '最早开始工作日期 判断AT列是否存在数据 有则判断没有则开始日期为Q列startdate
                
        End If
        
        
        

        Debug.Print empname & "的最早开始工作日期为" & startDate
        
        
        '///////////////////////////////开始进行日期校验处理///////////////////////////
        
        '/////校验工作日期是否早于毕业日期这存在多个毕业日期，按最早的毕业日期算/////
        
        
         ' 在校验结果的描述中添加校验信息，用 vbCrLf 分隔多个信息
        Dim validationResultDescription As String
        'Dim k As Long
        validationResultDescription = ""
    
        If IsDate(startDate) Then
        
        Else
            validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的开始工作日期不是日期格式; " + Chr(13)
        End If
        
        ' 检查毕业日期和开始工作日期是否有效
        If IsDate(graduationDate) And IsDate(startDate) Then
            ' 计算工作年限 当前日期-最早工作日期
            yearsOfWork_m = Round(DateDiff("m", startDate, Now()) / 12, 0)
            
            '检查工作日期是否早于毕业日期
            
            If graduationDate > startDate Then
                ' 显示错误信息（可以根据需要进行处理）
                validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的毕业日期晚于开始工作日期，请检查数据是否填写正确; " + Chr(13)
            End If
            
            ' 检查工作年限是否填写
            If yearsOfWork = "" Then
                ' 存储校验结果到字典
                validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的工作年限未填写; " + Chr(13)
            Else
                ' 检查工作年限是否合理 掐头去尾
                If InStr(yearsOfWork, "年") > 1 Then
                
                    validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的工作年限填写了“年”字段，请检查（应为" & yearsOfWork_m & "）; " + Chr(13)
                    yearsOfWork = Replace(yearsOfWork, "年", "")
                    
                    If Abs(yearsOfWork_m - yearsOfWork) >= 1 Then
                    ' 存储校验结果到字典
                        validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的工作年限与实际经验差距大，请检查（应为" & yearsOfWork_m & "）; " + Chr(13)
                    End If
                End If
            End If
        Else
            ' 如果日期无效，存储校验结果到字典
            validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的毕业日期或开始工作日期无效; " + Chr(13)
        End If
        
        '/////校验最早项目日期是否早于工作日期/////
        
        
        Call get_project_info(startrow, endrow) '获取排序后最早的项目日期与最早的工作日期做对比
        
        Set dataWS = ThisWorkbook.Sheets("ProcessedData")
        
        startprojectdate = dataWS.Range("A" & dataWS.Cells(dataWS.Rows.Count, "A").End(xlUp).Row).Value
        

        If IsDate(startprojectdate) And IsDate(startDate) Then
 
            If startprojectdate > startDate Then
                ' 显示错误信息（可以根据需要进行处理）
                validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的初次项目日期早于开始工作日期，请检查数据是否填写正确; " + Chr(13)
        
            End If
        
        Else
            ' 如果日期无效，存储校验结果到字典
                validationResultDescription = validationResultDescription & "第 " & i & " 行数据中，" & empname & "的初次项目日期或开始工作日期无效; " + Chr(13)
        End If
        
        '////////////////////项目时间校验，开始日期的小于结束的日期//////////////'
        
        '加时间顺序校验 要求后面的时间大于前面的时间
        
        For j = startrow To maxnoemptyrow
        
            If IsDate(sourcews.Range("Q" & maxnoemptyrow).Value) > IsDate(sourcews.Range("R" & maxnoemptyrow).Value) Then
            
                validationResultDescription = validationResultDescription & "第 " & startrow & " 行数据中，" & empname & "的工作日期顺序填写有误，请检查; " + Chr(13)
            Else
                If sourcews.Range("Q" & maxnoemptyrow).Value = "至今" Then
                
                    validationResultDescription = validationResultDescription & "第 " & startrow & " 行数据中，" & empname & "的工作日期开始不能填写为“至今”，请检查; " + Chr(13)
                
                End If

             
             End If
            
        Next j
        
        '//////////////////////////项目日期时间校验是否有跨公司日期/////////////////////////////////////////////////////////////'
        '示例：如A公司任职时间为 2020/10-2021/06 项目时间应该在此时间段中出现如2020/10-2021/09 这样的数据即为跨公司
        '逻辑实现在每段公司任职时间判断循环中，判断每个项目的开始和结尾是否有交叉
        
        ' 公司任职时间       |___________|________________________________________|_____________|
        '                2020/10 A  2021/09                   B              2022/06    C    2022/09
        '项目实施时间        |___________|___________________________________________|__________|
        '                2020/10 A1 2021/09                  B1                    2022/07  C1 2022/09
        
        '引入函数计算每段工作经历是否与填写的项目经历有交叉
        validationResultDescription = CheckDateRanges(startrow, endrow)
        
        '删除新建数据工作表
        Application.DisplayAlerts = False
        dataWS.Delete
                
        
        '////////////////////项目名称校验  **银行或公司 + **项目///////////

                
        '对多项目列进行检查V AA AF AK
        Dim projectname As String
        Dim columnsDict As Object
        Set columnsDict = CreateObject("Scripting.Dictionary")
        Dim colLabelAsString As String
        
        ' 定义正则表达式模式匹配 "yyyy/mm" 格式
        Dim regexPattern As String
        regexPattern = "^\d{4}/\d{1,2}$"
        
        ' 创建正则表达式对象
        Dim regex As Object
        Set regex = CreateObject("VBScript.RegExp")
        regex.Global = True
        regex.IgnoreCase = True
        regex.Pattern = regexPattern
            
        ' 符合 "yyyy/mm" 格式
        ' 检查相邻两个单元格的值
        Dim adjacentCellValue1 As String
        Dim adjacentCellValue2 As String
    
        



        ' 定义需要添加到字典的列标签
        Dim columnsArray As Variant
        columnsArray = Array("X", "AC", "AH", "AM")
        
        ' 遍历列标签数组，将每个标签添加到字典
        For Each colLabel In columnsArray
            columnsDict(colLabel) = colLabel
        Next colLabel
        
        ' 从字典中取出并使用列标签
        For Each colLabel In columnsDict.Keys
            colLabelAsString = colLabel
            Debug.Print "Column Label: " & colLabelAsString
            maxnoemptyrow = FindMaxNonEmptyRow(sourcews, workExpStartRow, workExpEndRow, colLabelAsString)

            Debug.Print colLabel & "列项目经历行最大行非空" & maxnoemptyrow
            
            For tt = startrow To maxnoemptyrow
                If maxnoemptyrow - startrow + 1 <= 10 Or maxnoemptyrow - startrow = 0 Then
                
                    projectname = sourcews.Range(colLabelAsString & tt).Value
                    
                
                    If IsBankOrCompanyProject(sourcews.Range(colLabelAsString & tt)) = False Then
                    
                        validationResultDescription = validationResultDescription & "第 " & colLabelAsString & " 列 " & tt & " 行数据中，“" & projectname & "”中项目名称没有按照格式填写; " + Chr(13)
                    
                    End If
                    
                    adjacentCellValue1 = sourcews.Range(colLabelAsString & tt).Offset(0, -2).Value
                    adjacentCellValue2 = sourcews.Range(colLabelAsString & tt).Offset(0, -1).Value
                    
                    ' 使用正则表达式检查 adjacentCellValue1 和 adjacentCellValue2 是否为 "yyyy/mm" 格式
                    If regex.Test(adjacentCellValue1) And regex.Test(adjacentCellValue2) Then
                        ' 两个都符合日期格式下检查项目时间前面是否大于后面的时间
                        
                        If adjacentCellValue1 > adjacentCellValue2 Then
                        
                            validationResultDescription = validationResultDescription & "第 " & tt & " 行数据中，“" & projectname & "” 项目的开始日期与结束日期的顺序填写错误; " + Chr(13)
                        
                        End If
                        
                        
                        
                        
                        ' 排除 "至今" 的情况
                    Else
                    
                        If adjacentCellValue2 = "至今" And regex.Test(adjacentCellValue1) Then
                            ' 右边为至今，左边为正常
                            
                        
                        Else
                        
                        validationResultDescription = validationResultDescription & "第 " & colLabelAsString & " 列 " & tt & " 行数据中，“" & projectname & "”中日期时间没有按照yyyy/mm格式填写; " + Chr(13)
                        
                        End If
                    End If
                    
                    
                    
                ElseIf maxnoemptyrow = 0 Then
                
                    Exit For
                
                End If
                
            
            Next tt
            
        Next colLabel
        
    


    
        ' 存储校验结果到字典，如果有校验信息则存储
        If validationResultDescription <> "" Then
        
            validationResults(i) = validationResultDescription
            
        End If
        
        
        Debug.Print validationResults(i)
        k = k + 1
        
        '/////////////////////////////// 子循环处理用于跳出循环///////////////////////
        
        For j = workExpStartRow To workExpEndRow
        
            ' 这里可以加入判断，如果当前行不是合并单元格的第一行则跳出循环
            '处理完工作经历跳出循环
            
            If j >= workExpEndRow And sourcews.Range("A" & j).MergeCells Then
            
                Exit For
              
            End If
            
        Next j



        ' 更新循环索引到下一个人的起始行
        i = endrow + 1
        
    Loop

    
    ' 新增sheet页最后并设置为目标sheet
    Sheets.Add After:=Sheets(Sheets.Count)
    Set newSheet = ThisWorkbook.Worksheets(Sheets.Count)
    newSheet.Name = "表格检验结果"
        


    ' 将校验结果写入新工作表的A列
    newSheet.Cells(1, 1).Value = "行号"
    newSheet.Cells(1, 2).Value = "姓名"
    newSheet.Cells(1, 3).Value = "校验结果"
        
    '删除字典内的空项
    
    ' 遍历字典中的每个键
    For Each Key In validationResults.Keys
        ' 检查对应的值是否为空
        If validationResults(Key) = "" Then
            ' 如果值为空，从字典中删除该键
            validationResults.Remove Key
        End If
    Next Key
    
    i = 2
    For Each Key In validationResults.Keys
        newSheet.Cells(i, 1).Value = Key
        newSheet.Cells(i, 2).Value = sourcews.Range("A" & Key).Text
        newSheet.Cells(i, 3).Value = validationResults(Key)
        i = i + 1
    Next Key
    
   ' 选择 A1 单元格及其当前区域
    newSheet.Range("A1").CurrentRegion.Select
    newSheet.Columns("C:C").ColumnWidth = 90
    
    ' 添加边框
    Selection.Borders.LineStyle = xlContinuous
    Selection.Borders.Weight = xlThin
    
    
    ' 设置左对齐、垂直居中和自动换行
    With Selection
        .HorizontalAlignment = xlLeft
        .VerticalAlignment = xlCenter
        .WrapText = True
        .Rows.AutoFit
        .Columns.AutoFit
    End With

    
    ' 提示sheet页已生成
    MsgBox "校验结果已生成在新的工作表中。", vbInformation, "校验完成"
    
    ' 跳出全部过程
    Exit Sub
    
        ' 关闭屏幕刷新，提升运行效率
    Application.ScreenUpdating = True
    ' 关闭窗口提示
    Application.DisplayAlerts = True
    
End Sub





'强检验项目名称是否匹配
Function IsBankOrCompanyProject(cellValue As String) As Boolean
    Dim containsBankOrCompany As Boolean
    Dim containsProject As Boolean
    
    ' 检查文本中是否同时包含"银行"或"公司"和"项目"
    containsBankOrCompany = (InStr(1, cellValue, "银行", vbTextCompare) > 0 Or InStr(1, cellValue, "公司", vbTextCompare) > 0)
    'containsProject = InStr(1, cellValue, "项目", vbTextCompare) > 0
    
    ' 返回是否同时包含"银行"或"公司"和"项目"
    IsBankOrCompanyProject = containsBankOrCompany 'and  containsProject
End Function







' 获取合并单元格的开始行和结束行
Function GetMergedCellRangeRows(ws As Worksheet, i As Long) As Variant
    Dim startrow As Long
    Dim endrow As Long
    Dim cell As Range
    
    Set cell = ws.Cells(i, 1)
    
    ' 初始化开始行和结束行为单元格的行号
    startrow = cell.Row
    endrow = cell.Row
    
    ' 检查单元格是否为合并单元格
    If cell.MergeCells Then
        startrow = cell.MergeArea.Cells(1, 1).Row
        endrow = cell.MergeArea.Cells(cell.MergeArea.Rows.Count, 1).Row
    End If
    
    ' 返回开始行和结束行的数组
    GetMergedCellRangeRows = Array(startrow, endrow)
End Function


'获取总行数，外循环
Function GetMaxRowInColumnAWithMerge(ws As Worksheet) As Long
    Dim lastRow As Long
    Dim maxRow As Long
    Dim cell As Range
    'Dim ws As Worksheet
    'Set ws = ThisWorkbook.Worksheets("工作表2") ' 替换为你的工作表名称
    
    ' 查找A列的最大行数
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    
    ' 初始化最大行为A列的最大行数
    maxRow = lastRow
    
    ' 获取A列的最大行单元格
    Set cell = ws.Cells(lastRow, 1)
    
    ' 检查最大行单元格是否为合并单元格
    If cell.MergeCells Then
        maxRow = cell.MergeArea.Rows.Count + maxRow - 1
    End If
    
    ' 返回结果
    GetMaxRowInColumnAWithMerge = maxRow
End Function




'去除部门英文名称
Function RemoveEnglishCharacters(inputString As String) As String
    Dim i As Integer
    Dim resultString As String
    
    resultString = ""
    
    For i = 1 To Len(inputString)
        If Not IsAlpha(Mid(inputString, i, 1)) Then
            resultString = resultString & Mid(inputString, i, 1)
        End If
    Next i
    
    ' 去除末尾的数字和点号
    Do While Len(resultString) > 0 And IsNumeric(Right(resultString, 1)) Or Right(resultString, 1) = "."
        resultString = Left(resultString, Len(resultString) - 1)
    Loop
    
    RemoveEnglishCharacters = resultString
End Function

'根据传入的参数判断 该列的最大非空行
Function FindMaxNonEmptyRow(ws As Worksheet, startrow As Long, endrow As Long, Optional colNum As String = "Q") As Long
    Dim maxNonEmptyRow As Long
    Dim currentRow As Long
    
    ' 设置初始值
    maxNonEmptyRow = 0
    
    ' 循环检查指定范围内的每一行
    For currentRow = startrow To endrow
        If Not IsEmpty(ws.Cells(currentRow, colNum).Value) Then
            maxNonEmptyRow = currentRow
        End If
    Next currentRow
    
    FindMaxNonEmptyRow = maxNonEmptyRow
End Function


' 查询项目经历中开始时间的所在行
Function FindSecondStartTimeRow(ws As Worksheet) As Long
    Dim lastRow As Long
    Dim foundCount As Long
    Dim cell As Range
    
    ' 设置要搜索的工作表
    ' Dim ws As Worksheet
    ' Set ws = ThisWorkbook.Worksheets("工作表2") ' 替换为你的工作表名称
    
    ' 获取A列的最后一行
    lastRow = ws.Cells(ws.Rows.Count, "B").End(xlUp).Row
    
    ' 初始化计数器
    foundCount = 0
    
    ' 在A列中查找第二次出现的"开始时间 "
    For Each cell In ws.Range("B1:B" & lastRow)
        If Trim(cell.Value) = "开始时间" Then
            foundCount = foundCount + 1
            If foundCount = 2 Then ' 找到第二次出现的
                FindSecondStartTimeRow = cell.Row + 1
                Exit Function
            End If
        End If
    Next cell
    
    ' 如果未找到第二次出现的"开始时间 "，返回最后一行的下一行
    FindSecondStartTimeRow = lastRow + 1
End Function



Private Sub CreateResumeDocx()
    ' 定义文件夹路径，格式为 "简历_yyyymmdd"
    Dim folderPath As String
    folderPath = ThisWorkbook.Path & "\简历_" & Format(Date, "yyyymmdd")
    
    ' 检查文件夹是否存在，如果存在则删除
    If Dir(folderPath, vbDirectory) <> "" Then
        On Error Resume Next
        Kill folderPath & "\*.*"
        RmDir folderPath
        On Error GoTo 0
    End If
    
    ' 创建新文件夹
    MkDir folderPath
    
    ' 获取最大的工作表
    Dim maxSheet As Worksheet
    Set maxSheet = Sheets(Sheets.Count)
    
    ' 保存当前处理人员的表格到新的 docx 文件中
    If Not maxSheet Is Nothing Then
        Dim docName As String
        docName = folderPath & "\个人简历_明细_" & Format(Date, "yyyymmdd") & ".docx"
        
        ' 保存为 docx 文件
        maxSheet.Copy
        'ActiveSheet.Copy
        ActiveSheet.SaveAs2 fileName:=docName, FileFormat:=17 ' 17 表示 Word 文档的文件格式
        ActiveWorkbook.Close False
    End If
    
    ' 删除最大的工作表
    If Not maxSheet Is Nothing Then
        Application.DisplayAlerts = False
        maxSheet.Delete
        Application.DisplayAlerts = True
    End If
    
    ' 保存当前 Excel 文件
    ThisWorkbook.Save
End Sub


Private Sub CreateVisibleWordAppAndSaveAsDocx(ByVal empname As String, ByVal folderPath As String)
    ' 创建一个可见的 Word 应用程序实例

    Dim emplyname  As String
    emplyname = empname
    

    Dim wordApp As Object
    Set wordApp = CreateObject("Word.Application")
    wordApp.Visible = False ' 如果需要可见Word应用程序
    
    ' 创建新 Word 文档
    Dim wordDoc As Object
    Set wordDoc = wordApp.Documents.Add
    
    ' 获取最大的工作表
    Dim maxSheet As Worksheet
    Set maxSheet = ThisWorkbook.Sheets(ThisWorkbook.Sheets.Count)
    
    ' 保存当前处理人员的表格到新的 docx 文件中
    If Not maxSheet Is Nothing Then
        Dim docName As String
        docName = folderPath & "\个人简历_" & emplyname & "_" & Format(Date, "yyyymmdd") & ".docx"
        
        ' 复制当前工作表的内容到新建的 Word 文档
        maxSheet.Activate
        maxSheet.Range("B2").CurrentRegion.Copy
        
        'wordApp.Selection.Paste
        wordDoc.Content.Paste
        wordDoc.SaveAs2 docName ' 17 表示 Word 文档的文件格式
        wordDoc.Close
    End If
    
    ' 释放 Word 应用程序对象
    wordApp.Quit
    Set wordDoc = Nothing
    Set wordApp = Nothing
    
    
    ' 保存当前 Excel 文件
    ThisWorkbook.Save
End Sub






Function IsAlpha(char As String) As Boolean
    IsAlpha = char Like "[a-zA-Z]"
End Function

'清理年限并根据年限更新职称名称
'处理F工作年限 L列职称信息 todo 联动合并单元格

Private Sub CleanYearFromCell()
    Dim ws As Worksheet
    Dim cell As Range
    
    ' 选择你的工作表
    Set ws = ThisWorkbook.Worksheets("工作表2") ' 将"Sheet1"替换为你的工作表名称
    
    ' 遍历工作表中的F列单元格
    For Each cell In ws.Range("F2:F" & ws.Cells(Rows.Count, "F").End(xlUp).Row)
        If Not IsNumeric(cell.Value) Then
            ' 如果单元格内容是数字，继续检查是否包含月份或年等字样
            If InStr(1, cell.Text, "年") > 0 Or InStr(1, cell.Text, "月") > 0 Or InStr(1, cell.Text, "年份") > 0 Then
                ' 如果包含这些字样，将它们替换为空字符串
                cell.Value = Replace(Replace(Replace(cell.Text, "年", ""), "月", ""), "年份", "")
            End If
        Else
            ' 如果不是数字，清空单元格
            
        End If
    Next cell
    

End Sub


Private Sub DeleteOtherSheets()
    Dim ws As Worksheet
    
    Application.DisplayAlerts = False ' 禁用警告提示
    
    For Each ws In ThisWorkbook.Sheets
        If ws.Name <> "工作表2" And ws.Name <> "Template" Then
            ws.Delete
        End If
    Next ws
    
    'Application.DisplayAlerts = True ' 启用警告提示
End Sub


Private Sub get_project_info(ByVal startrow As Long, ByVal endrow As Long)
    Dim ws As Worksheet
    Dim newSheet As Worksheet
    Dim lastRow As Long
    Dim newRow As Long
    Dim i As Long
    Dim OlastRow As Long
    Dim remainrow As Long

    ' 选择你的工作表
    Set ws = ThisWorkbook.Worksheets("工作表2") ' 将"Sheet2"替换为你的工作表名称

    'lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).Row
    newRow = 1
    remianrow = 0

    ' 创建新工作表
    Set newSheet = ThisWorkbook.Sheets.Add
    newSheet.Name = "ProcessedData"

    i = startrow ' 从第2行开始，因为第1行通常是标题

    Do While i <= endrow
        'If i + 9 <= endrow Then
            If WorksheetFunction.CountA(ws.Range("V" & i & ":V" & endrow)) = 10 Then
                ' V-Z 列满足10行
                ws.Range("V" & i & ":Z" & i + 9).Copy newSheet.Cells(newRow, 1)

                
                If WorksheetFunction.CountA(ws.Range("AA" & i & ":AA" & endrow)) = 10 Then
                    ' AA-AE 列满足10行
                    lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                    ws.Range("AA" & i & ":AE" & i + 9).Copy newSheet.Cells(lastRow + 1, 1)

                    
                    If WorksheetFunction.CountA(ws.Range("AF" & i & ":AF" & endrow)) = 10 Then
                        ' AF-AJ 列满足10行
                        lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                        ws.Range("AF" & i & ":AJ" & i + 9).Copy newSheet.Cells(lastRow + 1, 1)

                        
                        If WorksheetFunction.CountA(ws.Range("AK" & i & ":AO" & endrow)) = 10 Then
                            ' P-T 列满足10行
                            lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                            ws.Range("AK" & i & ":AO" & i + 9).Copy newSheet.Cells(lastRow + 1, 1)
                            'newRow = newRow + 10
                            'i = i + 10
                        Else
                            ' P-T 列不满足10行
                            Dim pMaxRow As Long
                            pMaxRow = WorksheetFunction.CountA(ws.Range("AK" & i & ":AK" & endrow))
                            If pMaxRow = 0 Then
                                Exit Do
                            Else
                                lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                                ws.Range("AK" & i & ":AO" & i + pMaxRow - 1).Copy newSheet.Cells(lastRow + 1, 1)
                                Exit Do
                            End If
                        End If
                    Else
                        ' K-O 列不满足10行
                        Dim kMaxRow As Long
                        kMaxRow = WorksheetFunction.CountA(ws.Range("AF" & i & ":AF" & endrow))
                    
                        If kMaxRow = 0 Then
                            Exit Do
                        Else
                            lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                            ws.Range("AF" & i & ":AJ" & i + kMaxRow - 1).Copy newSheet.Cells(lastRow + 1, 1)
                            Exit Do
                        End If
                    End If
                Else
                    ' F-J 列不满足10行，判断剩余有多少行将代码复制过去
                    Dim fMaxRow As Long
                    fMaxRow = WorksheetFunction.CountA(ws.Range("AA" & i & ":AA" & endrow))
                    
                    If fMaxRow = 0 Then
                        Exit Do
                    Else
                        lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
                        ws.Range("AA" & i & ":AE" & i + fMaxRow - 1).Copy newSheet.Cells(lastRow + 1, 1)
                        Exit Do
                    End If
                    
                End If
            Else
                ' A-E 列不满足10行,
                Dim aMaxRow As Long
                
                aMaxRow = WorksheetFunction.CountA(ws.Range("V" & i & ":V" & endrow))
                If aMaxRow = 0 Then
                    Exit Do
                Else
                    ws.Range("V" & i & ":Z" & i + aMaxRow - 1).Copy newSheet.Cells(newRow, 1)
                    Exit Do
                End If
                newRow = newRow + 10
                i = i + 10
            End If
        'Else
            'Exit Do ' 如果不足 10 行则跳出循环
        'End If
    Loop

    ' ... 以下是剩余的代码，用于处理日期格式设置和排序等 ...
    
    '获取粘贴完成后的最大行
    
    lastRow = newSheet.Cells(newSheet.Rows.Count, "A").End(xlUp).Row
    
    '修改单元格格式
    
    newSheet.Range("A1:A" & lastRow).NumberFormat = "yyyy/mm"
    newSheet.Range("B1:B" & lastRow).NumberFormat = "yyyy/mm"
    newSheet.Columns("E:E").ColumnWidth = 15
    newSheet.Columns("E:E").WrapText = True
    
    ' 循环设置日期格式

    For i = 1 To lastRow
        newSheet.Cells(i, "A").Value = Format(newSheet.Cells(i, "A").Value, "yyyy/mm")
        newSheet.Cells(i, "B").Value = Format(newSheet.Cells(i, "B").Value, "yyyy/mm")
    Next i

    ' 对新工作表的数据按 A 列的日期从近到远排序
    newSheet.Sort.SortFields.Clear
    newSheet.Sort.SortFields.Add Key:=newSheet.Range("A2:A" & lastRow), _
        SortOn:=xlSortOnValues, Order:=xlDescending, DataOption:=xlSortNormal
    With newSheet.Sort
        .SetRange newSheet.Range("A1:E" & lastRow)
        .Header = xlNo
        .MatchCase = False
        .Orientation = xlTopToBottom
        .SortMethod = xlPinYin
        .Apply
    End With
    
    '添加边框
    ' 给单元格添加边框
    With newSheet.Range("A1:E" & lastRow).Borders
        .LineStyle = xlContinuous ' 设置线条为连续线
        .Weight = xlThin ' 设置线条粗细
        .ColorIndex = xlAutomatic ' 使用自动颜色
        
    End With

    ' 数据处理完成,返回处理总行数
    'newSheet.Delete
    
    OlastRow = lastRow
    'MsgBox "数据已处理并粘贴到新的工作表 'ProcessedData' 中，并按日期排序。", vbInformation, "数据处理完成"
    Debug.Print "总行数" & OlastRow
    
End Sub

Private Sub test_pro()

'Call get_project_info(49, 58)
Call get_project_info(2, 11)

End Sub

Sub 简历格式优化()
    Dim folderPath As String
    Dim folderName As String
    Dim fileName As String
    Dim resumeDoc As Document
    Dim resumeTable As Table
    Dim processedFolder As String
    
    ' 检查是否有打开的文档

    
    ' 获取当前工作目录
    folderPath = ThisWorkbook.Path
    
    ' 获取当前日期以生成文件夹名
    folderName = "简历_" & Format(Now, "yyyymmdd")
    
    ' 构建文件夹的完整路径
    folderPath = folderPath & "\" & folderName
    
    ' 检查文件夹是否存在
    If Dir(folderPath, vbDirectory) <> "" Then
        ' 打开已存在的文件夹
        ChDir folderPath
        fileName = Dir(folderPath & "\个人简历*.docx")
        
        ' 遍历文件夹中的文件
        Do While fileName <> ""
            ' 打开简历文件
            Set resumeDoc = Documents.Open(folderPath & "\" & fileName)
            
            ' 处理文档中的所有表格
            For Each resumeTable In resumeDoc.Tables
                ' 设置表格为根据窗口自自动调整
                resumeTable.AutoFitBehavior (wdAutoFitWindow)
                ' 为表格添加边框
                resumeTable.Borders.Enable = True
                resumeTable.Range.Font.Name = "宋体"
                resumeTable.Range.Font.Size = 10
                ' 将表格高度统一设置为1.5厘米
                'resumeTable.Rows.HeightRule = wdRowHeightExactly
                'resumeTable.Rows.Height = CentimetersToPoints(1.5)
            Next resumeTable
            
            ' 保存并关闭已处理的文档
            resumeDoc.Save
            resumeDoc.Close
            fileName = Dir
        Loop
        
        ' 将“文档名称+已处理”写入文件夹名
        processedFolder = folderName & " 已处理"
        MsgBox "处理完成。处理文件夹：" & processedFolder, vbInformation
    Else
        ' 如果文件夹不存在，给出消息提示
        MsgBox "文件夹不存在或没有找到文件。", vbExclamation
    End If
End Sub

Sub testdate()
Call get_project_info(2, 11)
dateee = CheckDateRanges(2, 11)

Debug.Print dateee

End Sub


Function FindEarliestDate(ws As Worksheet, QStartRow As Long, QEndRow As Long, Optional ATStartRow As Variant, Optional ATEndRow As Variant) As String
    Dim dateRangeQ As Range
    Dim dateRangeAT As Range
    Dim cell As Range
    Dim earliestDate As Date
    Dim currentDate As Date
    
    ' 设置日期范围Q列
    Set dateRangeQ = ws.Range("Q" & QStartRow & ":Q" & QEndRow)
    
    ' 设置日期范围AT列，如果有传入参数
    If Not IsMissing(ATStartRow) And Not IsMissing(ATStartRow) Then
        Set dateRangeAT = ws.Range("AT" & ATStartRow & ":AT" & ATEndRow)
    End If
    
    ' 如果没有传入AT列的参数，只考虑Q列
    If dateRangeAT Is Nothing Then
        earliestDate = CDate(dateRangeQ.Cells(1).Value)
        
        ' 循环遍历日期范围Q列
        For Each cell In dateRangeQ

            currentDate = CDate(cell.Value)
            
            If currentDate < earliestDate Then
                earliestDate = currentDate
            End If
        Next cell
    Else
        ' 如果传入了AT列的参数，同时考虑Q列和AT列
        earliestDate = CDate(dateRangeQ.Cells(1).Value)
        
        ' 循环遪历日期范围Q列
        For Each cell In dateRangeQ
            'Dim currentDate As Date
            currentDate = CDate(cell.Value)
            
            If currentDate < earliestDate Then
                earliestDate = currentDate
            End If
        Next cell
        
        ' 循环遍历日期范围AT列
        For Each cell In dateRangeAT
            'Dim currentDate As Date
            currentDate = CDate(cell.Value)
            
            If currentDate < earliestDate Then
                earliestDate = currentDate
            End If
        Next cell
    End If
    
    ' 将最早的日期以 "yyyy/mm" 格式返回
    FindEarliestDate = Format(earliestDate, "yyyy/mm")
End Function

Private Function CheckDateRanges(ByVal startrow As Long, ByVal endrow As Long)
    Dim dataWS As Worksheet
    Dim mainWS As Worksheet
    Dim lastQ As Long
    'Dim lastR As Long
    Dim lastAT As Long
    'Dim lastAU As Long
    Dim lastMainRow As Long
    Dim i As Long
    Dim j As Long
    Dim k As Long
    Dim l As Long
    Dim errorMessage As String
    


    ' 设置数据工作表和主工作表
    Set dataWS = ThisWorkbook.Sheets("ProcessedData") ' 请替换为你的数据工作表的名称
    Set mainWS = ThisWorkbook.Sheets("工作表2") ' 请替换为你的主工作表的名称

    ' 获取 Q 列、R 列、AT 列和 AU 列的最后一行
    lastQ = FindMaxNonEmptyRow(mainWS, startrow, endrow, "Q")
    'lastR = 2
    lastAT = FindMaxNonEmptyRow(mainWS, startrow, endrow, "AT")
    'lastAU = 2
    lastMainRow = dataWS.Cells(dataWS.Rows.Count, "A").End(xlUp).Row
    ' 初始化错误消息
    errorMessage = ""

    ' 循环主工作表的 Q 列和 R 列数据
    For i = startrow To lastQ ' 假定第一行是标题行，从第二行开始
        Dim dateQ As Date
        Dim dateR As Date
        dateQ = mainWS.Cells(i, "Q").Value
        
        If mainWS.Cells(i, "R").Value = "至今" Then
            dateR = Format(Now(), "yyyy/mm")
        Else
            dateR = mainWS.Cells(i, "R").Value
        End If

        ' 循循环数据工作表的 A 列和 B 列数据
        For j = 1 To lastMainRow ' 假定第一行是标题行，从第二行开始
            ' 从数据工作表获取日期范围
            Dim dateA As Date
            Dim dateB As Date
            dateA = dataWS.Cells(j, "A").Value
            
            
            If dataWS.Cells(j, "B").Value = "至今" Then
                dateB = Format(Now(), "yyyy/mm")
            Else
                dateB = dataWS.Cells(j, "B").Value
            End If

            Debug.Print " 工作开始时间："; dateQ & " 工作结束时间：" & dateR & " 项目开始时间：" & dateA & " 项目结束时间：" & dateB

            ' 检查日期范围是否存在错误
            If (dateA < dateQ And dateQ < dateB) Or (dateA < dateR And dateR < dateB) Then
                errorMessage = errorMessage & "跨工作经历的项目日期错误：工作经历第 " & i & " 行Q列，工作经历：" & mainWS.Cells(i, "S").Value & ",项目名称：" & dataWS.Cells(j, "C").Value & vbCrLf
            End If
        Next j
    Next i

    ' 循环主工作表的 AT 列和 AU 列数据，如果有数据
    If lastAT > 1 Then
        ' 循环数据工作表的 A 列和 B 列数据
        For k = startrow To lastAT ' 假定第一行是标题行，从第二行开始
            ' 从主工作表获取日期范围
            dateQ = mainWS.Cells(k, "AT").Value
            dateR = mainWS.Cells(k, "AU").Value

            ' 循循环数据工作表的 A 列和 B 列数据
            For l = 1 To lastMainRow ' 假定第一行是标题行，从第二行开始
                ' 从数据工作表获取日期范围
                dateA = dataWS.Cells(l, "A").Value
                
                If dataWS.Cells(l, "B").Value = "至今" Then
                    dateB = Format(Now(), "yyyy/mm")
                Else
                    dateB = dataWS.Cells(l, "B").Value
                End If
                
                Debug.Print " 工作开始时间："; dateQ & " 工作结束时间：" & dateR & " 项目开始时间：" & dateA & " 项目结束时间：" & dateB
    
                ' 检查日期范围是否存在错误
                If (dateA < dateQ And dateQ < dateB) Or (dateA < dateR And dateR < dateB) Then
                    errorMessage = errorMessage & "跨工作经历的项目日期错误：工作经历第 " & k & " 行AT列，工作经历：" & mainWS.Cells(k, "AV").Value & ",项目名称：" & dataWS.Cells(l, "C").Value & vbCrLf
                End If
            Next l
        Next k
    End If

    ' 输出错误消息
    If errorMessage <> "" Then
        Debug.Print errorMessage
        CheckDateRanges = errorMessage
    End If
   
End Function


