
'修订记录：
'       2024/01/26 添加了删除空行的功能、优化了删除重复行少数据的问题

Sub 数据预处理()
Set ws = ThisWorkbook.Sheets("数据来源")
confirm.Show
'Call CheckBlankCellsInColumnAWithMergedCells(ws) '删除空行
'Call move_column(ws)        '移动列
'Call ProcessmainData(ws)    '删除重复数据
'Call ReplaceYearAndMonth(ws) '替换日期不合规的地方

MsgBox "数据预处理完成！请继续操作", vbInformation
End Sub
Sub CheckBlankCellsInColumnAWithMergedCells(ByVal ws As Worksheet)
    'Dim ws As Worksheet
    Dim lastRow As Long
    Dim rng As Range
    Dim cell As Range
    Dim row As Long
    ' 修改工作表名称
    'Set ws = ThisWorkbook.Sheets("数据来源") ' 将"Sheet1"替换为你实际的工作表名称
    
    ' 获取A列的最后一行
    lastRow = GetMaxRowInColumnAWithMerge(ws)
    
     For row = 2 To lastRow
                personName = Trim(UCase(ws.Cells(row, 5).Value))
                
                'debug.print personName

    
                '获取当前人员的开始行和结束行
                endrows = GetMergedCellRangeRows(ws, row)
                
                startRow = endrows(0) '开始行
                
                endRow = endrows(1) ' 结束行
        
                ' 如果该人的记录已存在
                If personName = "" Then
                    ' 获取该人的记录
                    Call delete_empty_rows(ws)
                
                End If
                row = endRow
            Next row
    
    'MsgBox "A列中没有空白单元格", vbInformation
End Sub






'有未填写名称行直接删除所在行再去执行下一步
Sub delete_empty_rows(ByVal ws As Worksheet)
    Dim row As Long
    Dim lastRow As Long
    Dim personName As String
    Dim updateTime As Variant

    'Set ws = ThisWorkbook.Sheets("数据来源")
    ' 获取数据最后一行
    lastRow = GetMaxRowInColumnAWithMerge(ws)

    ' 从最后一行向前遍历数据
    For row = lastRow To 2 Step -1
        
        endrows = GetMergedCellRangeRows(ws, row)
          
        startRow = endrows(0) ' 开始行
        endRow = endrows(1)   ' 结束行
        personName = Trim(ws.Cells(startRow, 5).Value)
        
        ' 检查是否为未填写名称行
        If personName = "" Then
            ' 获取当前人员的开始行和结束行
            'debug.print "第" & startRow & "行到第" & endRow & "行为无效数据，已删除;"

            ' 删除内容为空的行
            ws.Rows(startRow & ":" & endRow).Delete

            ' 将当前行设置为结束行，以便下一次循环跳过已删除的行
            

        End If
        
        row = startRow
        
    Next row
End Sub



'移动表格信息
 Sub move_column(ByVal ws As Worksheet)
'
' 宏1 宏
'

'

If InStr(1, ws.Range("R1").Value, "结束时间", vbTextCompare) > 0 Then

    GoTo endofsub

Else
    Columns("H:H").EntireColumn.AutoFit
    Columns("T:U").Select
    Selection.Cut
    Columns("R:R").Select
    Selection.Insert Shift:=xlToRight
 
    Columns("Y:Z").Select

    Selection.Cut
    Columns("V:V").Select
    Selection.Insert Shift:=xlToRight

    Columns("AE:AE").Select
    Selection.Cut
    Columns("AB:AB").Select
    Selection.Insert Shift:=xlToRight

    Columns("AJ:AJ").Select
    Selection.Cut
    Columns("AG:AG").Select
    Selection.Insert Shift:=xlToRight
    Columns("AO:AO").Select
    Selection.Cut
    Columns("AL:AL").Select
    Selection.Insert Shift:=xlToRight
End If

endofsub:
    


End Sub






Sub ProcessmainData(ByVal ws As Worksheet)
    ' 关闭屏幕刷新，提升运行效率
    Application.ScreenUpdating = False
    ' 关闭窗口提示
    Application.DisplayAlerts = False
    
    Dim lastRow As Long, row As Long
    Dim removeDupSheet As Worksheet
    Dim personName As Variant, updateTime As Date
    Dim startRow As Long, endRow As Long
    Dim newRow As Long

    ' 获取最后一行
    lastRow = GetMaxRowInColumnAWithMerge(ws)

    ' 创建名为RemoveDuplicate的新表格
    On Error Resume Next
    Set removeDupSheet = Sheets("RemoveDuplicate")
    On Error GoTo 0

    If removeDupSheet Is Nothing Then
        Set removeDupSheet = Sheets.Add(After:=Sheets(Sheets.Count))
        removeDupSheet.Name = "RemoveDuplicate"
    End If

    ' 在新表格中添加表头
    With removeDupSheet
        .Cells(1, 1).Value = "PersonName"
        .Cells(1, 2).Value = "UpdateTime"
        .Cells(1, 3).Value = "StartRow"
        .Cells(1, 4).Value = "EndRow"
        .Cells(1, 5).Value = "DeleteFlag"
    End With

    ' 从后往前遍历数据
    newRow = 2 ' 从新表格的第二行开始赋值
    For row = lastRow To 2 Step -1
        ' 获取当前人员的开始行和结束行
        endrows = GetMergedCellRangeRows(ws, row)
        startRow = endrows(0) '开始行
        endRow = endrows(1) ' 结束行

        personName = Trim(UCase(ws.Cells(startRow, 1).Value))
        updateTime = ws.Cells(startRow, 3).Value

        ' 将人员信息存储到RemoveDuplicate表格中
        removeDupSheet.Cells(newRow, 1).Value = personName
        removeDupSheet.Cells(newRow, 2).Value = updateTime
        removeDupSheet.Cells(newRow, 3).Value = startRow
        removeDupSheet.Cells(newRow, 4).Value = endRow
        removeDupSheet.Cells(newRow, 5).Value = "N" ' 默认删除标志为N
        newRow = newRow + 1
        
        row = startRow
    Next row

    ' 在RemoveDuplicate表格中进行去重处理
    RemoveDuplicates removeDupSheet, ws

    ' 删除数据表格
    removeDupSheet.Delete
    
    ' 关闭屏幕刷新，提升运行效率
    Application.ScreenUpdating = True
    ' 关闭窗口提示
    Application.DisplayAlerts = True
End Sub

Sub RemoveDuplicates(ws As Worksheet, ows As Worksheet)
    Dim lastRow As Long, row As Long
    Dim personData As Object

    ' 获取最后一行
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).row

    ' 用于存储每个人的数据，包括删除标志和序号
    Set personData = CreateObject("Scripting.Dictionary")

    ' 从后往前遍历数据
    For row = lastRow To 2 Step -1
        Dim personName As String
        Dim updateTime As Date
        Dim startRow As Long, endRow As Long
        Dim deleteFlag As String

        personName = ws.Cells(row, 1).Value
        updateTime = ws.Cells(row, 2).Value
        startRow = ws.Cells(row, 3).Value
        endRow = ws.Cells(row, 4).Value
        deleteFlag = ws.Cells(row, 5).Value

        ' 如果该人的记录已存在
        If personData.Exists(personName) Then
            Dim storedUpdateTime As Date
            storedUpdateTime = personData(personName)("updateTime")

            ' 比较时间，保留最新的记录
            If updateTime > storedUpdateTime Then
                ' 更新删除标志为Y
                ws.Cells(personData(personName)("row"), 5).Value = "Y"
                ' 更新当前记录到字典中
                personData(personName)("updateTime") = updateTime
                personData(personName)("row") = row
            Else
                ' 更新删除标志为Y
                ws.Cells(row, 5).Value = "Y"
            End If
        Else
            ' 如果该人的记录不存在，添加记录到字典中
            Set personData(personName) = CreateObject("Scripting.Dictionary")
            personData(personName)("updateTime") = updateTime
            personData(personName)("row") = row
        End If
    Next row

    ' 删除标志为Y的行
    Dim deleteRange As Range
    For row = lastRow To 2 Step -1
        If ws.Cells(row, 5).Value = "Y" Then
            Dim startRow1 As Long
            Dim endRow1 As Long
            startRow1 = ws.Cells(row, 3).Value
            endRow1 = ws.Cells(row, 4).Value

            ' 构建要删除的范围
            If deleteRange Is Nothing Then
                Set deleteRange = ows.Rows(startRow1 & ":" & endRow1)
            Else
                Set deleteRange = Union(deleteRange, ows.Rows(startRow1 & ":" & endRow1))
            End If
        End If
    Next row

    ' 删除标志为Y的行
    If Not deleteRange Is Nothing Then
        deleteRange.Delete
    End If
End Sub

Sub ReplaceYearAndMonth(ByVal ws As Worksheet)
    'Dim ws As Worksheet
    Dim replaceColumns As Variant
    Dim replaceRange As Range
    Dim column As Variant
    Dim cell As Range
    
    ' 修改为你要替换的列，多列之间用数组表示
    replaceColumns = Array("Q", "R", "V", "W", "AA", "AB", "AF", "AG", "AK", "AL", "AT", "AU")
    
    ' 修改为你的工作表名称
    'Set ws = ThisWorkbook.Sheets("数据来源")
    
    ' 将列转换为范围
    For Each column In replaceColumns
        If replaceRange Is Nothing Then
            Set replaceRange = ws.Range(ws.Cells(2, column), ws.Cells(ws.Rows.Count, column).End(xlUp))
        Else
            Set replaceRange = Union(replaceRange, ws.Range(ws.Cells(2, column), ws.Cells(ws.Rows.Count, column).End(xlUp)))
        End If
    Next column

    ' 循环遍历每个单元格，进行替换操作
    For Each cell In replaceRange
        If Not IsEmpty(cell.Value) Then
            ' 替换年为"/"
            cell.Value = Replace(cell.Value, "年", "/")
            ' 替换月为""
            cell.Value = Replace(cell.Value, "月", "")
        End If
    Next cell
End Sub




Function HasDuplicates(ByVal ws As Worksheet) As Boolean
    'Dim ws As Worksheet
    Dim lastRow As Long
    Dim rng As Range
    Dim cell As Range
    
    ' 指定要操作的工作表
    'Set ws = ThisWorkbook.Sheets("111")
    
    ' 获取 A 列的最后一行
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).row
    
    ' 设置范围为 A 列
    Set rng = ws.Range("A1:A" & lastRow)
    
    ' 遍历 A 列中的每个单元格
    For Each cell In rng
        ' 检查是否有重复值
        If Application.WorksheetFunction.CountIf(rng, cell.Value) > 1 Then
            ' 如果有重复值，返回 True
            HasDuplicates = True
            Exit Function
        End If
    Next cell
    
    ' 如果未发现重复值，返回 False
    HasDuplicates = False
End Function



Function IsRowInDictionary(row As Long, personData As Object) As Boolean
    Dim personName As Variant
    For Each personName In personData.Keys
        If row >= personData(personName)("startRow") And row <= personData(personName)("endrow") Then
            IsRowInDictionary = True
            Exit Function
        End If
    Next personName
    IsRowInDictionary = False
End Function






