

'执行确认进行执行宏操作

Sub confirm_run()

Dim x As Integer

If MsgBox("确认生成请点击确认按钮", vbYesNo, "警告") = vbYes Then

MsgBox ("正在生成行业交付中心战略执行总表拆分文件，请等待文件生成！")

Call procedure_all
    
End If

End Sub



'检测是否已创建文件夹，没有则进行创建

Sub create_folder()

thisworkbokname = ThisWorkbook.Name

Set objFSO = CreateObject("Scripting.FileSystemObject")

NewFolderforexplit = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0)

NewFolderforjoin = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0)

If objFSO.FolderExists(NewFolderforexplit) Then

objFSO.DeleteFolder (NewFolderforexplit)

Set newfolder = objFSO.createfolder(NewFolderforexplit)

Else

Set newfolder = objFSO.createfolder(NewFolderforexplit)

End If

If objFSO.FolderExists(NewFolderforjoin) Then

objFSO.DeleteFolder (NewFolderforjoin)

Set newfolder = objFSO.createfolder(NewFolderforjoin)

Else

Set newfolder = objFSO.createfolder(NewFolderforjoin)

End If

End Sub





'-------------------拆分总程序，用于拆分全部各业务部表数据------------------------

Public Sub procedure_all()

'循环汇总表中所有部门名称，依次进行表格拆分

Dim sheetfordept As Worksheet

Dim serno

Dim maxdeptrow '定义A列最大行数字为11

Dim filepath, savefilename

ThisWorkbook.Sheets(3).Select

thisworkbokname = ThisWorkbook.Name

maxdeptrow = 8

'Debug.Print maxdeptrow

serno = ""

countsheet = Worksheets.Count

Call create_folder

'判断已存在最新工作表是否为生成表，数量大于8张退出

If countsheet = 7 Then

For i = 3 To maxdeptrow

    curntdeptname = ThisWorkbook.Sheets(3).Cells(i, 1).Value
    
    filepath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\合并工作簿"
    
    savefilename = filepath & "\" & curntdeptname & "_专业交付中心考核表.xlsx"
    
    If IsFileExists(savefilename) = True Then
    
     MsgBox (curntdeptname & "已存在合并文件,文件在 " & filepath & "下")
     
    Else
    
    'Debug.Print curntdeptname
    
    '调用部门得分汇总子程序进行各部门拆分并对拆分后数据进行美化，以源工作表名_部门名进行命名
    
    'Call sp_process_dept(curntdeptname)
    
    serno = i - 2
    
    'Debug.Print serno
    
    
    For j = 3 To countsheet
    
    
    
    Call sp_process_dept(curntdeptname, j)
    
    Next j
     
    Call Auto_Split_Delete_Sheet
    
    Call join_workbook(curntdeptname, thisworkbokname)
    
    Call ReOrderSheet(curntdeptname, thisworkbokname)
    
    End If
     
Next i

ElseIf countsheet > 7 Then

MsgBox ("源表格大于预期表数量，请确认是否还有未删除存量表格")

End If

'MsgBox ("ok")

End Sub

 Sub rr()
 Call sp_process_dept("银行业务一部")
 End Sub
 


'----------------------筛选业务部汇总得分-----------------------------

Public Sub sp_process_dept(ByVal deptinname, ByVal sheetnum)

'用于计算当前工作表总行数与列数

Dim rowmax As Long

Dim colmax As Long

Dim data As Range

'定义工作表类型用于简化代码别名

Dim mysheet1 As Worksheet

'部门名称，用于传入参数

Dim deptname





'对sheet2中A列进行循环获取所有业务部名称

deptname = deptinname

'MsgBox (deptname)

'重新制定短名方便使用

'mysheet1 = Sheets(2)

'刷新减少内存占用

Application.ScreenUpdating = False
      
Application.DisplayAlerts = False


'------------------------创建新工作表，统一为最后的附3最后方便管理-------

'在最后创建新工作表

Worksheets.Add After:=Worksheets(Worksheets.Count)

'获取新工作表index(非必须)

'shtidx = Sheets(ActiveWorkbook.Sheets.Count).Index

'----------------------筛选业务部汇总得分----------------

Sheets(sheetnum).Select

'获取当前工作表所有有效行数

rowmax = Sheets(sheetnum).Range("A10000").End(xlUp).Row

'获取当前工作表数据列中非空表格中列数10

colmax = Sheets(sheetnum).Range("IV3").End(xlToLeft).Column

'MsgBox ("最大行数：" & rowmax - 2 & ",最大列数：" & colmax)

Sheets(sheetnum).Range("a1").Select

'对所选数据根据条件进行筛选，如果出现拆分错误的情况，如没有标题行参照重点客户执行情况将标题行到最后一行为数据区域进行筛选即可

Sheets(sheetnum).Range("a1").AutoFilter field:=1, Criteria1:=deptname

'对最大行数据内容进行选择并复制到名为data 的range类变量中

'获取筛选后当前工作表数据列中非空表格中列数

'colmax = Sheets(2).Range("IV3").End(xlToLeft).Column

Sheets(sheetnum).Range("$a$1:$f$" & rowmax).Select

'筛选后的行数获取拼接获得最终选取------------获取非空行数后选择与最后粘贴后的进行对比确认

Sheets(sheetnum).Range(Selection, Selection.End(xlDown)).Copy Destination:=Sheets(ActiveWorkbook.Sheets.Count).Range("a1")

'Set data = Selection

'------------将筛选后的数据粘贴到新工作表--------------------


'选定最后一个工作表并选择A1单元格

'Sheets(ActiveWorkbook.Sheets.Count).Select

'Sheets(ActiveWorkbook.Sheets.Count).Range("a1").Select

'粘贴并保持列宽

'Sheets(ActiveWorkbook.Sheets.Count).Paste

'Selection.PasteSpecial Paste:=xlPasteColumnWidths, skipblanks:=True, Transpose:=False

'检测表格是否有数据

If Sheets(ActiveWorkbook.Sheets.Count).Range("a1").Value = "" Then

    MsgBox (deptname & "粘贴出现错误，请检查表格是否有数据，或数据是否存在！")
    
ElseIf Sheets(ActiveWorkbook.Sheets.Count).Range("a1").Value <> "" Then

'清空粘贴板并冲命名表格并返回初始工作表
    
    '加一个判断粘贴列数是否相等的判断
    
    'if
    
    'data.Clear
    
    '将表格重命名为"原表格 + （当前）部门名称

    Sheets(ActiveWorkbook.Sheets.Count).Name = Sheets(sheetnum).Name & "_" & deptname
    
    Sheets(ActiveWorkbook.Sheets.Count).Select
    
    convadress = CSN(colmax)
    
    'Debug.Print rowmax
    
    'Debug.Print convadress
    
    '对生成的表格自动调整行高和列宽
    
    Call autojust
    
    
    '第一表格已生成

    
End If


'切回原sheet并取消筛选

Sheets(sheetnum).Select

Sheets(sheetnum).Range("a1").AutoFilter

Sheets(sheetnum).Range("a1").Select

Application.ScreenUpdating = True
     
Application.DisplayAlerts = True

'当前工作表deptname 分类一筛选完毕，循环继续筛选下一部门

End Sub



Public Sub autojust()

' 对生成的表格进行判断，根据行高进行调整最适合的行高和列宽



    Dim newshtname
    
    newshtname = Sheets(ActiveWorkbook.Sheets.Count).Name
    
    Debug.Print newshtname
    
    
    '对工作表名按照分隔符"_"进行截取，获取前缀

    splitname = GetArray(newshtname, 0)
    
    inrowmax = Sheets(ActiveWorkbook.Sheets.Count).Range("A10000").End(xlUp).Row

    '获取当前工作表数据列中非空表格中列数10

    incolmax = Sheets(ActiveWorkbook.Sheets.Count).Range("IV2").End(xlToLeft).Column

    'MsgBox ("最大行数：" & rowmax - 2 & ",最大列数：" & colmax)

    inconvadress = CSN(incolmax)
    
    Debug.Print splitname
    
    Debug.Print inrowmax
    
    Debug.Print inconvadress
    
    
    '对生成的表格按照设定好的内容自动调整行高和列宽
    
    If splitname <> "说明" Or splitname <> "考核指标" Then
    
         For Each rw In Sheets(ActiveWorkbook.Sheets.Count).Rows("1:" & inrowmax)
       
               If rw.Row = 2 Then
         
                rw.RowHeight = 30
        
               Else
         
               rw.RowHeight = 20
          
              End If
    
         Next
       
         For Each Cl In Sheets(ActiveWorkbook.Sheets.Count).Columns("A:" & inconvadress)
        
                If Cl.Column = 1 Then
        
                Cl.ColumnWidth = 12
        
                Else
        
                Cl.ColumnWidth = 10
        
              End If
        
         Next
         
    Else
    
             For Each rw In Sheets(ActiveWorkbook.Sheets.Count).Rows("1:" & inrowmax)
       
               If rw.Row = 2 Then
         
                rw.RowHeight = 30
        
               Else
         
               rw.RowHeight = 20
          
              End If
    
         Next
       
         For Each Cl In Sheets(ActiveWorkbook.Sheets.Count).Columns("A:" & inconvadress)
        
                If Cl.Column <= 2 Then
        
                Cl.ColumnWidth = 12
        
                Else
        
                Cl.ColumnWidth = 10
        
              End If
        
         Next
    
   
    
    End If
    
    '单元格内容进行居中对齐,默认全部，对于重点客户的不进行处理
    
     Sheets(ActiveWorkbook.Sheets.Count).Range("a1").CurrentRegion.Select
    
     Selection.HorizontalAlignment = xlCenter
    
     Selection.VerticalAlignment = xlCenter
    
End Sub

'分割表名并提取

Function GetArray(shetname, idx)

'从 dt 中取出第 idx 个数据: GetArray(A1,2)

aa = Split(shetname, "_")

GetArray = aa(idx)

End Function


Function get_row(addr)

get_row = ThisWorkbook.Worksheets("Tabelle1").Range(addr).Row

End Function

Function get_column(addr)

get_column = ThisWorkbook.Worksheets("Tabelle1").Range(addr).Column

End Function


'用于地址转换如输入"A"则输出对应数字码为1 ,互相转换

Private Function CSN(Col)

Dim i, j, si, sj

If IsNumeric(Col) Then

j = Col Mod 26: i = (Col - j) / 26: If j = 0 Then j = 26

If i > 0 Then CSN = Chr(64 + i) & Chr(64 + j) Else CSN = Chr(64 + j)

Else

If Len(Col) = 1 Then sj = Col Else si = Mid(Col, 1, 1): sj = Mid(Col, 2, 1)

If si <> "" Then i = Asc(si) - 64

If sj <> "" Then j = Asc(sj) - 64

CSN = 26 * i + j

End If

End Function

'检测文件是否存在

Function IsFileExists(ByVal strFileName As String) As Boolean
    
    If Dir(strFileName, 16) <> Empty Then
        
        IsFileExists = True
    
    Else
        
        IsFileExists = False
    
    End If

End Function



Sub kkk()

'Call sp_process_dept("非银业务部")
'Call sp_process_cust("非银业务部")
'Call sp_vip_cust_exec("非银业务部", 9)
Call sp_dept_corp_front("银行业务一部", 1)
End Sub


Private Sub Auto_Split_Delete_Sheet()
'
' 自动拆分工作表 宏,并把源工作簿拆分的工作表删除
'
'
'把各个工作表以单独的工作簿文件保存在本工作簿所在的文件夹下的“拆分工作簿”文件夹下
'获取活动工作簿所在路径 并判断该路径下是否存在文件夹"拆分工作簿",如果不存在则创建
'遍历活动工作簿中的每个工作表，复制并另存为新的工作簿，工作簿文件名以工作表名称命名
'如果遇到隐藏工作表，则先打开隐藏，复制并另存为后关闭隐藏
'
    
    Application.ScreenUpdating = False '关闭屏幕更新
    
    Dim xpath, isNext As String
    
    Dim sht As Worksheet
    
    Dim wshcnt
    
    thisworkbokname = ThisWorkbook.Name
   


    xpath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\拆分工作簿"
   
    If Len(Dir(xpath, vbDirectory)) = 0 Then MkDir xpath '如果文件夹不存在，则新建文件夹
    
    '复制说明和附件sheet 检查文件夹内是否有已经存在的说明sheet防止重复生成
    
    If IsFileExists(xpath & "\说明.xlsx") = False Then
    
    For Each sht In Worksheets
 
     Application.DisplayAlerts = False
     
     If sht.Index = 1 Or sht.Index = 2 Then
     
                sht.Copy
                
                ActiveWorkbook.SaveAs filename:=xpath & "\" & sht.Name & ".xlsx"
                
                ActiveWorkbook.Close
     End If
     
     Application.DisplayAlerts = True
     
     Next
    
    End If
    

    For Each sht In Worksheets
    
     If sht.Index > 7 Then
     
        If sht.Visible = False Then
        
            'MsgBox "有隐藏工作表" & sht.Name
            
            '隐藏工作表是否拆分
            
            isNext = InputBox("1:跳过不处理" & Chr(10) & "2:处理并保持隐藏" & Chr(10) & "3:处理并取消隐藏" & Chr(10) & "空:不输入或其他值则默认不执行", "【" & sht.Name & "】为隐藏工作表,请选择执行方式")
            
            If isNext = 2 Or isNext = 3 Then
               
                
                sht.Visible = True '取消工作表的隐藏
                
                sht.Copy
                
                ActiveWorkbook.SaveAs filename:=xpath & "\" & sht.Name & ".xlsx"
                
                ActiveWorkbook.Close
                
                If isNext = 2 Then
                
                    sht.Visible = False '恢复工作表的隐藏
                    
                End If
                
                
             End If
             
        ElseIf sht.Visible = True Then
           
            sht.Copy
            
            ActiveWorkbook.SaveAs filename:=xpath & "\" & sht.Name & ".xlsx"
            
            ActiveWorkbook.Close
            
        End If
        
    End If
    
    Next
    
    '删除非源表下的生成sheet
    
 For Each sht In Worksheets
 
     Application.DisplayAlerts = False
     
     If sht.Index > 7 Then
     
     sht.Delete

     End If
     
     Application.DisplayAlerts = True
     
 Next
    
    'MsgBox "工作簿拆分结束"
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    
    
End Sub






Public Sub test()

Call join_workbook("银行", "客户经营2.0年度绩效考核跟踪表_20220228.xlsm")

End Sub


Public Sub join_workbook(ByVal indeptname, ByVal inorgbokname)

'指定文件夹，对文件夹下的文件进行合并按照9各部门的后缀，按照一定的排序顺序进行合并

'thisworkbook.name 为最终合并表名称

'GetArray（sheet("1"),0）功能表_部门

    Application.ScreenUpdating = False '关闭屏幕更新
    
    Application.DisplayAlerts = False
    
    Dim xpath, ypath, isNext, deptname As String
    
    Dim sht As Worksheet
    
    Dim filename
    
    thisworkbokname = ThisWorkbook.Name
    
    deptname = indeptname '传入部门名称用于处理相关部门的表格
    
    orgbokname = inorgbokname '获取原表格名称用以表格处理结束后的激活

    xpath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\合并工作簿"
    
    ypath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\拆分工作簿"
    
    savefilename = xpath & "\" & deptname & "_专业交付中心考核.xlsx"
   
    If Len(Dir(xpath, vbDirectory)) = 0 Then MkDir xpath '如果文件夹不存在，则新建文件夹
    
    If IsFileExists(savefilename) = False Then
    
    Debug.Print deptname & " 不存在合并文件,开始创建合并文件"
    
    Workbooks.Add
    
    ActiveWorkbook.SaveAs filename:=savefilename
    
    '创建文件命名
    
     filename = Dir(ypath & "\*.xlsx")
    
    While filename <> ""
    
    '当存在拆分文件的时候先判断sheet页的名字，先添加说明，最后添加附件三张sheet表格并保存为以部门编号为名字的新sheet页
    
    '不显示应用窗口节省内存
    
    'Application.Visible = False
    
        Set wb = Workbooks.Open(ypath & "\" & filename, UpdateLinks:=3)
        
        '挨个打开sheet页对每个sheet页的名字进行判断，满足条件sheets.name = "说明"或sheets.index = 1则加为第一张,同理附件作为最后进行，其他sheet页判断名字并指定插入顺序
        
            For Each Sheet In ActiveWorkbook.Sheets
            
                If Sheet.Name = "说明" Then
            
                i = Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets.Count
                
                Debug.Print Sheet.Name
                
                Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(1)
                
                ElseIf Sheet.Name = "考核指标" Then
                
                '附件页放到最后一页
                
                i = Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets.Count
                
                'Debug.Print Sheet.Name
                
                Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                
                Else
                
                Debug.Print GetArray(Sheet.Name, 1)
                
                '当sheet不是附件页的时候，判断后缀是否是部门号，再进行前缀判断
                
                    If GetArray(Sheet.Name, 1) = deptname Then
                        
                        i = Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets.Count
                    
                        If GetArray(Sheet.Name, 0) = "专业交付中心战略执行考核" Then
                        
                        Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                        
                        ElseIf GetArray(Sheet.Name, 0) = "客户2.0战略执行过程" Then
                        
                        Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                        
                        ElseIf GetArray(Sheet.Name, 0) = "客户2.0战略执行结果" Then
                        
                        Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                        
                        ElseIf GetArray(Sheet.Name, 0) = "解决方案和产品创新2.0战略执行" Then
                        
                        Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                        
                        ElseIf GetArray(Sheet.Name, 0) = "组织2.0战略执行" Then
                        
                        Sheet.Copy After:=Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(i)
                        
                        
                        End If
                        
                    
                    End If
                    
                
                End If
                              
            Next Sheet
        
        '不关闭所有工作簿
        
        wb.Close
        
        
        
        filename = Dir
        
    Wend
    
    End If
    
    Workbooks(deptname & "_专业交付中心考核.xlsx").Sheets(1).Delete '删除自带的sheet1表格
    
    Workbooks(deptname & "_专业交付中心考核.xlsx").Close Savechanges:=True '保存并关闭合并的表格
    
    Workbooks(orgbokname).Activate '激活原表格进行下一部门的数据处理
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    
    Application.DisplayAlerts = True '恢复窗口提示
    
End Sub


'/////////////////////////////////////////////////////////////以下为对生成的合并工作簿合并功能/////////////////////////////////////////////////////////////////////////////‘



Function splitshetname(shetname, idx)

'从 字符串 中取出第 idx 个数据: splitshetname(A1,2)

bb = Split(shetname, ",")

splitshetname = bb(idx)

End Function

Sub GetSheetList(ByVal deptname)

    Dim sht     As Object       '// sheet
    
    Dim s       As String       '// 追加sheet名
    
    Dim i       As Long         '// loop count
    
    '// 追加sheet
    
    Call Sheets.Add(After:=Sheets(Sheets.Count))
    
    s = "AddSheet"
    
    ActiveSheet.Name = s
    
    ActiveSheet.Activate
    
    ActiveSheet.Range("A1").Select
    
    '// loop 添加指定表数
    
    Call loopinsert(deptname)
    
    Call ChangeOrder
    
    
End Sub


Public Sub loopinsert(ByVal deptname)

'//指定用到的变量，存放排序的表格与拆分后的表格，并将内容插入新增工作表的单元格内

Dim arryyy      As String

Dim getnewname  As String

For i = 0 To 6

'//在此处指定排序顺序

arryyy = "说明,专业交付中心战略执行考核,考核指标,客户2.0战略执行过程,客户2.0战略执行结果,解决方案和产品创新2.0战略执行,组织2.0战略执行"

'/引用自定义函数，按照顺序进行排序，注意第一个数字为0代表位置为1的内容

getnewname = splitshetname(arryyy, i)

'//将数据插入新增工作表的对应单元格内，如拆分名为"说明"时，i为0 则插入到cells(1,1)也就是 A1 单元格中，最后一个则为cells(6,1)

If getnewname = "说明" Or getnewname = "考核指标" Then

Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname

'ElseIf getnewname = "考核指标" Then

'Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname

Else

Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname & "_" & deptname

End If

Debug.Print getnewname

Next i

End Sub

'///按照给定的顺序调整顺序
Public Sub ChangeOrder()

    Dim ar()    As String       '// sheet名数组
    Dim i       As Integer      '// loop count
    Dim s       As String       '// cell值
    
    Sheets("AddSheet").Select
    Sheets("AddSheet").Activate
    Sheets("AddSheet").Range("A1").Select
    
    i = 0
    ReDim ar(i)
    
    '// loop A列
    Do
        '// cell值取得
        s = ActiveCell.Offset(i, 0).Value
        
        '// cell值为空的场合
        If (s = "") Then
            '// 跳出loop
            Exit Do
        End If
        
        '// 把sheet名放到数组中
        ReDim Preserve ar(i)
        ar(i) = s
        
        i = i + 1
    Loop
    
    '// 按照AddSheet的顺序排列
    i = 0
    Do
        '// 数组要素为空
        If (i > UBound(ar)) Then
            '// 跳出loop
            Exit Do
        End If
        
        '// 将数组当前循环值的表名移动到当前循环计数器值的右侧
        Sheets(ar(i)).Move before:=Sheets(i + 1)
        
        i = i + 1
    Loop
    
    '// 删除的确认对话框不表示
    Application.DisplayAlerts = False
    
    '// "AddSheet"sheet删除
    Sheets("AddSheet").Delete
    
    Application.DisplayAlerts = True
End Sub



Public Sub ReOrderSheet(ByVal indeptname, ByVal inorgbokname)

'///打开合并工作簿，对文件夹下的文件进行合并按照给定的排序顺序进行排序

    Application.ScreenUpdating = False '关闭屏幕更新
    
    Application.DisplayAlerts = False  '关闭警告提示
    
    Dim ypath, deptname, openfilename, filename As String
    
    Dim sht As Worksheet
    
    thisworkbokname = ThisWorkbook.Name '用于获取工作簿名称以查找对应文件夹
     
    deptname = indeptname '传入部门名称用于处理相关部门的表格
    
    orgbokname = inorgbokname '获取原表格名称用以表格处理结束后的激活
    
    ypath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\合并工作簿"
    
    openfilename = ypath & "\" & deptname & "_专业交付中心考核.xlsx"
    
    If IsFileExists(openfilename) = False Then
    
    MsgBox deptname & " 不存在合并文件请检查！"
      
    Else
      
     filename = openfilename
    
    'While filename <> ""
    
    '当存在合并文件的时候先判断sheet页的名字 格式getarr(sheetname,1)
    
    '不显示应用窗口节省内存
    
    'Application.Visible = False
    
        Set wb = Workbooks.Open(filename, UpdateLinks:=3)
        
        '挨个打开sheet页对每个sheet页的名字进行判断，满足条件sheets.name = "说明"或sheets.index = 1则加为第一张,同理附件作为最后进行，其他sheet页判断名字并指定插入顺序
            
               Call GetSheetList(deptname)

        '不关闭所有工作簿 保存并关闭合并的表格
        
               wb.Close Savechanges:=True
        
        
    'Application.Visible = False
        
               'filename = Dir
        
    'Wend
    
    End If

    'Workbooks(deptname & "_客户经营2.0绩效考核跟踪表.xlsx").Close Savechanges:=True '保存并关闭合并的表格
    
    Workbooks(orgbokname).Activate '激活原表格进行下一部门的数据处理
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    
    Application.DisplayAlerts = True '恢复窗口提示
    
End Sub


