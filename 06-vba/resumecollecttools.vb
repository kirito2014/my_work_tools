'生成以姓名sheet,通过模板复制生成
Sub worksheetsgenerate()

'定义基本变量后期可改为字典形式
'基本情况
Dim EmpName, WorkExp, GradDate, GradSchool, speciality, HighDeGree, Department, Title, Introduce As String
'工作经历
Dim Workdate_start, Workdate_end, Company_name, work_potition, work_content As String
'项目经历
Dim projectdate_start, projectdate_end, project_name, project_role, project_content As String
'能力资质
Dim ability, aptitude, train As String
Dim sourcews, targetws As Worksheet

'关闭屏幕刷新,提升运行效率
Application.ScreenUpdating = False
'关闭窗口提示
Application.DisplayAlerts = False
'复制Template 到最后
'判断存在sheet个数 超过2个则删除sheets.index >2 的sheet
'获取sheet个数
SheetsInCount = Sheets.Count
Debug.Print SheetsInCount
If SheetsInCount > 3 Then
'删除sheet
    For i = 4 To SheetsInCount
        Sheets(i).Delete
    Next i
End If

'《《《《《《《《《《《《《《《《从此处开始处理简历循环》》》》》》》》》》》》》》》》

'判断第一个人的简历起止行，用到一个函数
 ' 设置工作表
 Set sourcews = ThisWorkbook.Worksheets("工作表1")  
    
'在最后新增工作表
Sheets("Template").Copy After:=Sheets(Sheets.Count)
'重命名新增工作表名称，重命名为当前简历人名称
Sheets(Sheets.Count).Name = "Sheet1"

'从第2行开始循环每一行数据，直到填写人为下一人为止
'获取当前人合并单元格的最大行,进行子循环
maxrow = GetMaxRowInColumnAWithMerge(sourcews) '
for i = 2 to


nextrow =
For i = 2 To nextrow



Next i








'<<<<<<<<<<<<<<<<<<<<<<<<<<<<从此处生成docx文件>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>













'<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<处理结束，打扫战场>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

'开启屏幕刷新
Application.ScreenUpdating = True
'关闭窗口提示
Application.DisplayAlerts = True




End Sub
'获取此单元格开始行数
Function GetStartRowOfMergedCell(cell As Range) As Long
    Dim mergedRange As Range
    
    On Error Resume Next
    ' 尝试获取单元格所在的合并单元格范围
    Set mergedRange = cell.MergeArea
    On Error GoTo 0
    
    ' 如果单元格不在合并单元格中，返回单元格所在的行号
    If mergedRange Is Nothing Then
        GetStartRowOfMergedCell = cell.row
    Else
        ' 如果单元格在合并单元格中，返回合并单元格的开始行号
        GetStartRowOfMergedCell = mergedRange.Cells(1, 1).row
    End If
End Function



Function GetMaxRowInColumnAWithMerge(ws As Worksheet) As Long
    Dim lastRow As Long
    Dim maxRow As Long
    Dim cell As Range
    
    ' 查找A列的最大行数
    lastRow = ws.Cells(ws.Rows.Count, "A").End(xlUp).row
    
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


Sub TestGetMaxRowInColumnAWithMerge()
    Dim ws As Worksheet
    Set ws = ThisWorkbook.Worksheets("工作表1") ' 替换为你的工作表名称
    MsgBox "A列的最大行号（包括合并单元格）是：" & GetMaxRowInColumnAWithMerge(ws)
End Sub

