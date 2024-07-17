'/*----------------------------------------------------------------
'// Copyright (c) 2021 SunlineData.Co.Ltd. All rights reserved.
'// 版权所有。
'//
'// 文件名：行业交付中心项目报告自动拆分模板
'// 文件功能描述：
'//     循环拆分按照部门拆分的工作簿,并将其自动调整列宽行高
'//     自动按照部门号合并到同一个工作簿并自动排序,删除拆分文件,重命名工作表
'//
'// 创建者：战略创新部SIDept/王穆军
'// 时间：2021年10月21日
'//
'// 修改人：
'// 时间：
'// 修改说明：
'//
'// 修改人：
'// 时间：
'// 修改说明：
'//
'// 版本：V1.0.0
'//----------------------------------------------------------------*/

'/////////////////////以下为源码信息（标注*处为可替换修改不影响功能）///////////////////////////////


'//执行确认进行执行宏操作

Sub confirm_run()

Dim x As Integer
Do
    Select Case MsgBox("是否执行单部门文件生成", vbYesNoCancel, "确认执行")
    Case vbYes
        MsgBox " 生成单部门拆分文件,请等待！", vbInformation
        Call procedure_single
        Exit Do
    Case vbNo
        MsgBox "生成全部部门拆分文件，时间较长，请耐心等待！", vbInformation
        Call procedure_all
        Exit Do
    Case vbCancel
        MsgBox "取消操作", vbInformation
        Exit Do
    Case Else
        Exit Do
    End Select
    
Loop

End Sub





'-------------------多部门拆分总程序,用于拆分全部各业务部表数据------------------------

Public Sub procedure_all()

Application.ScreenUpdating = False
'Application.DisplayAlerts = False

'循环汇总表中所有部门名称,依次进行表格拆分
'Dim serno
Dim maxdeptrow '定义A列最大行数字
Dim i, d        As Integer      '// loop count
Dim getdeptname As String
    
    
ThisWorkbook.Sheets(1).Select '设定初始工作表
thisworkbokname = ThisWorkbook.Name '当前工作簿名称

Sheets("部门清单").Visible = -1 '取消隐藏部门配置表
Sheets("部门清单").Select '激活配置表
Sheets("部门清单").Range("A2").Select
maxdeptrow = Sheets("部门清单").Range("A10000").End(xlUp).Row
    
    d = 0
    
    For i = 2 To maxdeptrow
    
        If Sheets("部门清单").Cells(i, 2) = "Y" Then
        
            getdeptname = Sheets("部门清单").Cells(i, 1).Value
            
            Sheets(1).Range("C1").Value = getdeptname '将当前的运行结果赋值到C3标题单元格
            
            Debug.Print getdeptname
            
            Call procedure_single
            
            d = d + 1
            
        ElseIf Sheets("部门清单").Cells(i, 2) = "N" Then
        
            Debug.Print "根据部门清单配置，" & getdeptname & "不生成拆分文件，进行下一部门流程"
        
        End If
        
        
        
    
    Next
     
MsgBox "已生成" & d & "个部门的文件请在文件夹查看", vbInformation
    
ActiveWorkbook.Sheets("部门清单").Visible = 0 '隐藏配置文件
ActiveWorkbook.Sheets(1).Select
Application.ScreenUpdating = False

End Sub


'-------------------单次拆分总程序,用于拆分全部各业务部表数据------------------------

Public Sub procedure_single()

'循环汇总表中所有部门名称,依次进行表格拆分
Dim sheetfordept As Worksheet
'Dim serno
Dim maxdeptrow '定义A列最大行数字
Dim filepath, savefilename
Dim get_date
Dim sht As Worksheet

ThisWorkbook.Sheets(1).Select '设定初始工作表
thisworkbokname = ThisWorkbook.Name '当前工作簿名称
get_date = Format_Time(Date, 5) '当前年月日
maxdeptrow = ThisWorkbook.Sheets(2).Range("A10000").End(xlUp).Row
countsheet = Worksheets.Count '获取初始工作表数量

Sheets("部门清单").Visible = -1 '取消隐藏部门配置表

'检查上一级目录是否存在
Call create_folder

'*判断已存在最新工作表是否为生成表,数量大于11张退出

If countsheet = 12 Then '匹配单元格数量12则为未生成处理表格

    curntdeptname = ThisWorkbook.Sheets(1).Range("C1").Value
    
    'Debug.Print curntdeptname
    
    filepath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & curntdeptname  '当前工作簿所在文件夹/工作簿名称文件夹/部门名称文件夹
    
    If Len(Dir(filepath, vbDirectory)) = 0 Then MkDir filepath '如果文件夹不存在,则新建文件夹
    
            savefilename = filepath & "\合并工作簿" & "\" & curntdeptname & Month(Date) & "月项目报告_" & get_date & ".xlsx"
    
        If IsFileExists(savefilename) Then
    
            MsgBox curntdeptname & "已存在合并文件,文件在 " & filepath & "下", vbOKOnly, "文件存在提示"
     
        Else
    
    '调用部门得分汇总子程序进行各部门拆分并对拆分后数据进行美化,以源工作表名_部门名进行命名
    
            Call Sp_Project_Split(curntdeptname)
    
            Call Auto_Split_Delete_Sheet(curntdeptname)
    
            Call join_workbook(curntdeptname, thisworkbokname)
    
            Call delete_file(curntdeptname)
        
            Call rename_run
            
            Call ReOrderSheet(curntdeptname, thisworkbokname)
    
         End If
         
         'MsgBox GetArray(thisworkbokname, 0) & "_" & curntdeptname & ",拆分已完成,请检查文件目录！", vbInformation
         
 ElseIf countsheet > 12 Then '当大于11表格时则为存在

    '删除非源表下的生成sheet
    
        For Each sht In Worksheets
 
         Application.DisplayAlerts = False
     
            If sht.Index > 12 Then
     
             sht.Delete

            End If
     
        Application.DisplayAlerts = True
     
        Next

'If MsgBox("源表格大于预期表数量,已将存量表格删除," + Chr(13) + "请重新执行程序！", vbOKCancel, "是否重新执行") = vbYes Then
    
 '   Call procedure_single
    
'End If



End If



Sheets("部门清单").Visible = 0
ActiveWorkbook.Sheets(1).Select
Debug.Print "已生成" & curntdeptname & "部门的文件请在文件夹查看", vbInformation

End Sub



'//检测文件依赖的文件夹是否存在,方便生成文件的管理,检测部门配置文件是否存在 "部门配置信息.xlsx"

Sub create_folder()

thisworkbokname = ThisWorkbook.Name

Set objFSO = CreateObject("Scripting.FileSystemObject")

NewFolderforexplit = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0)
NewFolderforjoin = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0)

If objFSO.FolderExists(NewFolderforexplit) Then
    'objFSO.DeleteFolder (NewFolderforexplit)
    'Set newfolder = objFSO.createfolder(NewFolderforexplit)
Else
    Set newfolder = objFSO.createfolder(NewFolderforexplit)

End If

If objFSO.FolderExists(NewFolderforjoin) Then
    'objFSO.DeleteFolder (NewFolderforjoin)
    'Set newfolder = objFSO.createfolder(NewFolderforjoin)
Else
    Set newfolder = objFSO.createfolder(NewFolderforjoin)
End If

End Sub

Sub ttt()
Debug.Print newfindtext(12, 银行交付1H部)
End Sub

Sub www()
Sp_Project_Split ("银行交付1H部")
Debug.Print findcol(12, "银行交付1H部")
End Sub

'----------------------每个工作表按一级部门拆分-----------------------------

Public Sub Sp_Project_Split(ByVal deptinname)

'用于计算当前工作表总行数与列数
'部门名称,用于传入参数

Dim rowmax, colmax, serno, sheetcnt     As Integer      '//最大行
Dim deptname                            As String       '//部门变量
Dim x                                                   '//自定义变量
Dim sht                                 As Worksheet    '//工作表定义

'对sheet2中A列进行循环获取所有业务部名称

deptname = deptinname
sheetcnt = Worksheets.Count
'刷新减少内存占用

Application.ScreenUpdating = False
Application.DisplayAlerts = False

'For Each sht In ActiveWorkbook.Sheets

'Debug.Print sht.Name

For x = 2 To sheetcnt


'在最后创建新工作表
Debug.Print Sheets(x).Name

'findcolexist = newfindtext(x, deptname)
findcolexist = findcol(x, deptname)
Worksheets.Add After:=Worksheets(ActiveWorkbook.Sheets.Count)


Sheets(x).Select
'*获取当前工作表所有有效行数
rowmax = Sheets(x).Range("A10000").End(xlUp).Row
'*获取当前工作表数据列中非空表格中列数10
colmax = Sheets(x).Range("IV2").End(xlToLeft).Column



'对所选数据根据条件进行筛选,如果出现拆分错误的情况,如没有标题行参照重点客户执行情况将标题行到最后一行为数据区域进行筛选即可

If findcolexist = "无" Or findcolexist = "" Then

    Sheets(x).Range("$A$1:$Z$2").Select

    Selection.Copy Destination:=Sheets(ActiveWorkbook.Sheets.Count).Range("a1")

    Sheets(ActiveWorkbook.Sheets.Count).Range("A3").Value = "无"
    
    Sheets(ActiveWorkbook.Sheets.Count).Range("A1:Z2").EntireColumn.AutoFit
    
    Sheets(ActiveWorkbook.Sheets.Count).Range("A1:Z2").EntireRow.AutoFit

ElseIf findcolexist <> "无" Or findcolexist <> "" Then

    Sheets(x).Range("a2").Select

    Sheets(x).Range("a2").AutoFilter field:=findcolexist, Criteria1:=deptname

'对最大行数据内容进行选择并复制到名为data 的range类变量中

'*获取筛选后当前工作表数据列中非空表格中列数

    Sheets(x).Range("$a$1:" & "$" & CSN(colmax) & "$" & rowmax).Select

'筛选后的行数获取拼接获得最终选取------------获取非空行数后选择与最后粘贴后的进行对比确认

    Sheets(x).Range(Selection, Selection.End(xlDown)).Copy Destination:=Sheets(ActiveWorkbook.Sheets.Count).Range("a1")
    
    serno = Sheets(ActiveWorkbook.Sheets.Count).Range("A10000").End(xlUp).Row
    
    For i = 3 To serno
    
        Sheets(ActiveWorkbook.Sheets.Count).Cells(i, 1).Value = i - 2
        
    Next i

End If


'------------将筛选后的数据粘贴到新工作表--------------------增加判断如果工作表是全年验收清单的情况下检测A3单元格是否为空

'检测表格是否有数据

If Sheets(ActiveWorkbook.Sheets.Count).Range("A2").Value = "" Then

    MsgBox (deptname & "粘贴出现错误,请检查表格是否有数据,或数据是否存在！")
    
ElseIf Sheets(ActiveWorkbook.Sheets.Count).Range("A2").Value <> "" Then

'清空粘贴板并冲命名表格并返回初始工作表
    
    '判断粘贴列数是否相等的判断 将表格重命名为"原表格 + （当前）部门名称,特殊判断情况数量太多的情况下
    
    Sheets(ActiveWorkbook.Sheets.Count).Name = Sheets(x).Name & "_" & deptname
    Sheets(ActiveWorkbook.Sheets.Count).Select
    
    convadress = CSN(colmax)
    
    '对生成的表格自动调整行高和列宽
    Call autojust
    
    Sheets(ActiveWorkbook.Sheets.Count).Range("$A$1:" & "$" & CSN(colmax) & "$" & rowmax).EntireColumn.AutoFit
    'Sheets(ActiveWorkbook.Sheets.Count).Range("$A$1:" & "$" & CSN(colmax) & "$" & rowmax).EntireRow.AutoFit
    
End If

'切回原sheet并取消筛选

Sheets(x).Select
Sheets(x).Range("a2").AutoFilter
Sheets(x).Range("a2").Select

Next x


Application.ScreenUpdating = True
Application.DisplayAlerts = True

'当前工作表deptname 分类一筛选完毕,循环继续筛选下一部门

End Sub

Sub yyyy()
delete_file ("银行交付7D部")
End Sub

'//删除生成的拆分文件
Sub delete_file(ByVal deptname)

    Dim fs As Object
    Dim thisworkbokname As String
    Dim ypath, yfile As String
    
    Set fs = CreateObject("scripting.filesystemobject")
    thisworkbokname = ThisWorkbook.Name
    yfile = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\拆分工作簿\*.*"
    ypath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\拆分工作簿"
    
    fs.deletefile yfile
    fs.DeleteFolder ypath
    
End Sub

'//////////////////////自动调整列宽与行高///////////////////////////////


Public Sub autojust()

' 对生成的表格进行判断,根据行高进行调整最适合的行高和列宽


    Dim newshtname As String
    
    newshtname = Sheets(ActiveWorkbook.Sheets.Count).Name
    
    '对工作表名按照分隔符"_"进行截取,获取前缀

    splitname = GetArray(newshtname, 0)
    
    inrowmax = Sheets(ActiveWorkbook.Sheets.Count).Range("A10000").End(xlUp).Row

    '获取当前工作表数据列中非空表格中列数10

    incolmax = Sheets(ActiveWorkbook.Sheets.Count).Range("IV2").End(xlToLeft).Column

    'MsgBox ("最大行数：" & rowmax - 2 & ",最大列数：" & colmax)

    inconvadress = CSN(incolmax)
    
    '对生成的表格按照设定好的内容自动调整行高和列宽
    
         For Each rw In Sheets(ActiveWorkbook.Sheets.Count).Rows("1:" & inrowmax)
               
               If rw.Row <= 2 Then
         
                rw.RowHeight = 30
        
               Else
         
               rw.RowHeight = 15
          
              End If
    
         Next
       
         For Each Cl In Sheets(ActiveWorkbook.Sheets.Count).Columns("A:" & inconvadress)
        
                If Cl.Column = 4 Then
        
                Cl.ColumnWidth = 30
        
                Else
        
                Cl.ColumnWidth = 15
        
              End If
        
         Next
         
    
    '单元格内容进行居中对齐,默认全部,对于重点客户的不进行处理
    
     Sheets(ActiveWorkbook.Sheets.Count).Range("a1").CurrentRegion.Select
     Selection.HorizontalAlignment = xlCenter
     'Selection.VerticalAlignment = xlCenter
    
End Sub




Private Sub Auto_Split_Delete_Sheet(ByVal curntdeptname)
'//---------------------------------------------------------------------------
'//自动拆分工作表 宏,并把源工作簿拆分的工作表删除
'//
'//
'//把各个工作表以单独的工作簿文件保存在本工作簿所在的文件夹下的“拆分工作簿”文件夹下
'//获取活动工作簿所在路径 并判断该路径下是否存在文件夹"拆分工作簿",如果不存在则创建
'//遍历活动工作簿中的每个工作表,复制并另存为新的工作簿,工作簿文件名以工作表名称命名
'//如果遇到隐藏工作表,则先打开隐藏,复制并另存为后关闭隐藏
'//-----------------------------------------------------------------------------
    
    Application.ScreenUpdating = False '关闭屏幕更新
    Application.DisplayAlerts = False
    
    
    Dim xpath, isNext As String
    Dim sht As Worksheet
    Dim wshcnt As Integer
    
    thisworkbokname = ThisWorkbook.Name

    xpath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & curntdeptname & "\拆分工作簿"
   
    If Len(Dir(xpath, vbDirectory)) = 0 Then MkDir xpath '如果文件夹不存在,则新建文件夹
    
    '复制说明和附件sheet 检查文件夹内是否有已经存在的说明sheet防止重复生成
    
    If IsFileExists(xpath & "\标题及目录.xlsx") = False Then
    
    For Each sht In Worksheets
 
        If sht.Index = 1 Then
     
                sht.Copy
                
                ActiveWorkbook.SaveAs filename:=xpath & "\" & sht.Name & ".xlsx"
                
                ActiveWorkbook.Close
    
         End If
         
         
        
     
     Next
    
   End If
    

    For Each sht In Worksheets
    


     If sht.Index > 12 Then
     
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
     
     If sht.Index > 12 Then
     
        sht.Delete

     End If
     
     Application.DisplayAlerts = True
     
 Next
    
    'MsgBox "工作簿拆分结束"
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    Application.DisplayAlerts = True
    
    
End Sub



'//拆分单元格合并子程序

Public Sub join_workbook(ByVal indeptname, ByVal inorgbokname)

'//指定文件夹,对文件夹下的文件进行合并按照9各部门的后缀,按照一定的排序顺序进行合并
'//thisworkbook.name 为最终合并表名称
'//GetArray（sheet("1"),0）功能表_部门

    Application.ScreenUpdating = False '关闭屏幕更新
    
    Application.DisplayAlerts = False
    
    Dim xpath, ypath, isNext, deptname As String
    Dim get_date
    Dim sht As Worksheet
    Dim filename
    
    
    'get_date = Year(Date) & Month(Date) & Day(Date) '当前年月日
    get_date = Format_Time(Date, 5)
    thisworkbokname = ThisWorkbook.Name
     
    deptname = indeptname '传入部门名称用于处理相关部门的表格
    
    orgbokname = inorgbokname '获取原表格名称用以表格处理结束后的激活

    xpath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\合并工作簿"
    
    ypath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\拆分工作簿"
    
    savefilename = xpath & "\" & deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx"
   
    If Len(Dir(xpath, vbDirectory)) = 0 Then MkDir xpath '如果文件夹不存在,则新建文件夹
    
    If IsFileExists(savefilename) = False Then
    
    Debug.Print deptname & " 不存在合并文件,开始创建合并文件"
    
    Workbooks.Add
    
    ActiveWorkbook.SaveAs filename:=savefilename
    
    '创建文件命名
    
     filename = Dir(ypath & "\*.xlsx")
    
    While filename <> ""
    
    '当存在拆分文件的时候先判断sheet页的名字,先添加说明,最后添加附件三张sheet表格并保存为以部门编号为名字的新sheet页
    
    '不显示应用窗口节省内存
    
    'Application.Visible = False
    
        Set wb = Workbooks.Open(ypath & "\" & filename, UpdateLinks:=3)
        
        '挨个打开sheet页对每个sheet页的名字进行判断,满足条件sheets.name = "说明"或sheets.index = 1则加为第一张,同理附件作为最后进行,其他sheet页判断名字并指定插入顺序
        
            For Each Sheet In ActiveWorkbook.Sheets
            
                If Sheet.Name = "标题及目录" Then
            
                i = Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Sheets.Count
                
                Debug.Print Sheet.Name
                
                Sheet.Copy After:=Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Sheets(1)
                
                Else
                
                Debug.Print GetArray(Sheet.Name, 1)
                
                '当sheet不是附件页的时候,判断后缀是否是部门号,再进行前缀判断
                
                    If GetArray(Sheet.Name, 1) = deptname Then
                        
                        i = Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Sheets.Count
                        
                        Sheet.Copy After:=Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Sheets(i)
     
                    End If
                    
                
                End If
                              
            Next Sheet
            

        wb.Close
        
        
        
        filename = Dir
        
    Wend
    
    End If
    
    Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Sheets(1).Delete '删除自带的sheet1表格
    
    Workbooks(deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx").Close Savechanges:=True '保存并关闭合并的表格
    
    'Call rename_worksheet(deptname, orgbokname)
    
    Workbooks(orgbokname).Activate '激活原表格进行下一部门的数据处理
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    
    Application.DisplayAlerts = True '恢复窗口提示
    
End Sub
Sub rename_run()
Call rename_worksheet(ThisWorkbook.Sheets(1).Range("C1").Value, ThisWorkbook.Name)

End Sub

'//拆分单元格合并子程序

Public Sub rename_worksheet(ByVal deptname, ByVal orgbokname)

'//指定文件夹,对文件夹下的文件进行合并删除部门后缀,维持数据的一致性
'//thisworkbook.name 为最终合并表名称
'//GetArray（sheet("1"),0）功能表_部门

    Application.ScreenUpdating = False '关闭屏幕更新
    Application.DisplayAlerts = False
    
    Dim xpath, filename As String
    Dim get_date
    Dim sht             As Worksheet

    'get_date = Year(Date) & Month(Date) & Day(Date) '当前年月日
    get_date = Format_Time(Date, 5)
    thisworkbokname = ThisWorkbook.Name
    xpath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\合并工作簿"
    savefilename = xpath & "\" & deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx"
    
    If IsFileExists(savefilename) = False Then
        MsgBox deptname & " 不存在要更改的文件,请确保文件存在再进行操作", vbInformation
    Else
        filename = Dir(xpath & "\*.xlsx")
        While filename <> ""
            Set wb = Workbooks.Open(xpath & "\" & filename, UpdateLinks:=3)
                For Each Sheet In ActiveWorkbook.Sheets
                    If Sheet.Name = "标题及目录" Then
                        Debug.Print Sheet.Name
                    Else
                        If GetArray(Sheet.Name, 1) = deptname Then
                            'Debug.Print Sheet.Name
                            Debug.Print GetArray(Sheet.Name, 0)
                            newname = GetArray(Sheet.Name, 0)
                            Sheet.Name = newname
                        End If
                    End If
                Next Sheet
            wb.Close Savechanges:=True
        filename = Dir
        Wend
    End If
    
    Workbooks(orgbokname).Activate '激活原表格进行下一部门的数据处理
    Application.ScreenUpdating = True  '恢复屏幕更新
    Application.DisplayAlerts = True '恢复窗口提示
    Debug.Print "1212"
End Sub


Sub qget_date()
Dim a, b As String
a = Format(Date, "yyyy年m月d日") '当前年月日
b = Format(Time, "hh:mm") '当前时间
MsgBox a & " " & b '显示日期时间
End Sub


'/////////////////////////////以下为对生成的合并工作簿排序功能///////////////////////////////////

Function splitshetname(shetname, idx)

'从 字符串 中取出第 idx 个数据: splitshetname(A1,2)

bb = Split(shetname, ",")

splitshetname = bb(idx)

End Function



Public Sub ReOrderSheet(ByVal indeptname, ByVal inorgbokname)

'///打开合并工作簿,对文件夹下的文件进行合并按照给定的排序顺序进行排序

    Application.ScreenUpdating = False '关闭屏幕更新
    Application.DisplayAlerts = False  '关闭警告提示
    
    Dim ypath, deptname, openfilename, filename As String
    Dim sht As Worksheet
    Dim get_date
    
    thisworkbokname = ThisWorkbook.Name '用于获取工作簿名称以查找对应文件夹
    deptname = indeptname '传入部门名称用于处理相关部门的表格
    orgbokname = inorgbokname '获取原表格名称用以表格处理结束后的激活
    'get_date = Year(Date) & Month(Date) & Day(Date) '当前年月日
    get_date = Format_Time(Date, 5)
    
    ypath = Application.ActiveWorkbook.Path & "\" & GetArray(thisworkbokname, 0) & "\" & deptname & "\合并工作簿"
    openfilename = ypath & "\" & deptname & Month(Date) & "月项目报告_" & get_date & ".xlsx"
    
    If IsFileExists(openfilename) = False Then
        MsgBox deptname & " 不存在合并文件请检查！"
    Else
      
     filename = openfilename
    
    '当存在合并文件的时候先判断sheet页的名字 格式getarr(sheetname,1)
    
    '不显示应用窗口节省内存
    
        Set wb = Workbooks.Open(filename, UpdateLinks:=3)
        
        '循环打开sheet页对每个sheet页的名字进行判断,满足条件sheets.name = "说明"或sheets.index = 1则加为第一张,同理附件作为最后进行,其他sheet页判断名字并指定插入顺序
            
               Call GetSheetList(deptname)

        '不关闭所有工作簿 保存并关闭合并的表格
        
               wb.Close Savechanges:=True
        
    End If
    
    Workbooks(orgbokname).Activate '激活原表格进行下一部门的数据处理
    
    Application.ScreenUpdating = True  '恢复屏幕更新
    Application.DisplayAlerts = True '恢复窗口提示
    
End Sub

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
    
    Call loopinsert
    Call ChangeOrder
    
    
End Sub


Public Sub loopinsert()

'//指定用到的变量,存放排序的表格与拆分后的表格,并将内容插入新增工作表的单元格内

Dim arryyy      As String
Dim getnewname  As String

For i = 0 To 10 '/排除隐藏单元格之外总表格格数-1 如有12个工作表其中1个为隐藏工作表 则此处应为11 -1（隐藏）-1  = 9

'//在此处指定排序顺序,要求,用英文逗号“,”分隔,源工作表名称无空格,名称一一对应

'//////////////////////////////////////////////////////////////////////////////////////////////////////////

arryyy = "标题及目录,在建项目清单,风险-11月部门周报风险,风险-10月GM偏差5%项目风险,风险-长期暂停、异常项目最新进展,10月项目利润登记表-外包项目,10月项目利润登记表-实施项目,11月已登记终验的项目清单,11月里程碑&终验完成情况,12月里程碑计划清单,全年验收项目清单1209"

'//////////////////////////////////////////////////////////////////////////////////////////////////////////

'/引用自定义函数,按照顺序进行排序,注意第一个数字为0代表位置为1的内容

getnewname = splitshetname(arryyy, i)
'//将数据插入新增工作表的对应单元格内,如拆分名为"说明"时,i为0 则插入到cells(1,1)也就是 A1 单元格中,最后一个则为cells(6,1)

    'If getnewname = "说明" Then
        Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname
    'ElseIf getnewname = "考核指标" Then
       ' Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname
    'Else
      '  Sheets("AddSheet").Cells(i + 1, 1).Value = getnewname & "_" & deptname
   ' End If

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
        If (s = "") Then '// cell值为空的场合
            Exit Do             '// 跳出loop
        End If

        ReDim Preserve ar(i)         '// 把sheet名放到数组中
        ar(i) = s
        i = i + 1
    Loop
    
    '// 按照AddSheet的顺序排列
    i = 0
    Do

        If (i > UBound(ar)) Then         '// 数组要素为空
            Exit Do             '// 跳出loop
        End If
        
        '// 将数组当前循环值的表名移动到当前循环计数器值的右侧
        Debug.Print ar(i)
        Debug.Print i + 1
        Sheets(ar(i)).Move before:=Sheets(i + 1)
        'Debug.Print Sheets(ar(i)).Name
        'Debug.Print Sheets(i + 1).Name
        i = i + 1
    Loop
    
    '// 删除的确认对话框不表示
    Application.DisplayAlerts = False
    
    '// "AddSheet"sheet删除
    Sheets("AddSheet").Delete
    Sheets("部门清单").Delete
    Application.DisplayAlerts = True
End Sub



'列数转字母
Function CNtoW(ByVal num As Long) As String
    CNtoW = Replace(Cells(1, num).Address(False, False), "1", "")
End Function
'字母转列数
Function CWtoN(ByVal AB As String) As Long
    CWtoN = Range("a1:" & AB & "1").Cells.Count
End Function

Sub findtest()
Debug.Print newfindtext(12, 银行交付7D部)
End Sub


Function newfindtext(ByVal inndex, ByVal deptname)  '表格索引号和查找字符

Dim rowmax, colmax As Integer
Dim transnumber As String
Dim findcelltext

'Sheets(inndex).Select
ThisWorkbook.Sheets(inndex).Select

rowmax = Sheets(inndex).Range("A10000").End(xlUp).Row

colmax = Sheets(inndex).Range("IV3").End(xlToLeft).Column

For i = 1 To colmax

 transnumber = CNtoW(i)
 
Set findcell = Columns(transnumber).Find(deptname, LookAt:=xlPart)

    If Not findcell Is Nothing Then
    
        findcelltext = i
        
        Exit For
        
    Else
    
        findcelltext = "无"
        
    End If

Next

 newfindtext = findcelltext

End Function

Rem 输入日期格式，输出格式化后的日期
Function Format_Time(STARTDATE, n_Flag)
    Dim y, m, d, h, mi, s
    Format_Time = ""
    If IsDate(STARTDATE) = False Then Exit Function
    y = CStr(Year(STARTDATE))
    yy = CStr(Year(STARTDATE))
    m = CStr(Month(STARTDATE))
    mm = CStr(Month(STARTDATE) + 1)
    If Len(m) = 1 Then m = "0" & m
    d = CStr(Day(STARTDATE))
    If Len(d) = 1 Then d = "0" & d
    h = CStr(Hour(STARTDATE))
    If Len(h) = 1 Then h = "0" & h
    mi = CStr(Minute(STARTDATE))
    If Len(mi) = 1 Then mi = "0" & mi
    s = CStr(Second(STARTDATE))
    If Len(s) = 1 Then s = "0" & s
    If mm = 12 Then y = y + 1
    If mm = 12 Then mm = "01"
    'If d <> "01" Then d = "01"
    Select Case n_Flag
        Case 1
            ' yyyy-mm-dd hh:mm:ss
            Format_Time = y & "-" & mm & "-" & d & " " & h & ":" & mi & ":" & s
        Case 2
            ' yyyy-mm-dd
            Format_Time = y & "-" & m & "-" & d
        Case 3
            ' hh:mm:ss
            Format_Time = h & ":" & mi & ":" & s
        Case 4
            ' yyyy年mm月dd日
            Format_Time = y & "年" & m & "月" & d & "日"
        Case 5
            ' yyyymmdd
            Format_Time = yy & m & d
        Case 6
            'yyyymm
            Format_Time = yy & m
        Case 7
            'yyyy-mm+1-dd
            Format_Time = y & "-" & m + 1 & "-" & d
        Case 8
            'mm
            Format_Time = m
    End Select
End Function


Function Findaddress(ByVal worksheetindex, ByVal deptname)

'传入工作簿索引与部门号,输出该工作表中部门号表格所在的列号 用于筛选条件

getaddress = Sheets(worksheetindex).Range("1:" & Sheets(worksheetindex).Rows.Count).Find(deptname).Address

Findaddress = getcol(getaddress)

End Function

Function getcol(ByVal addressss)

    m = addressss
    
    getcol = CSN(Split(m, "$")(1))
    
End Function


Function GetArray(shetname, idx)

'从 dt 中取出第 idx 个数据: GetArray(A1,2)

    aa = Split(shetname, "_")

    GetArray = aa(idx)

End Function

'//用于地址转换如输入"A"则输出对应数字码为1 ,互相转换

 Function CSN(Col)

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


'//检测文件是否存在

Function IsFileExists(ByVal strFileName As String) As Boolean
    If Dir(strFileName, 16) <> Empty Then
        IsFileExists = True
    Else
        IsFileExists = False
    End If
End Function


Sub lllsls()
Debug.Print findcol(5, "银行交付7D部")
End Sub



Function findcol(ByVal inndex, ByVal deptname) '已废弃使用新函数newfindtext
'/传入表索引,查找第一个部门所在的行号

Dim rowmax, colmax As Integer
Dim transnumber As String

ThisWorkbook.Sheets(inndex).Select

Debug.Print Sheets(inndex).Name
'*获取当前工作表所有有效行数
rowmax = Sheets(inndex).Range("A10000").End(xlUp).Row
'*获取当前工作表数据列中非空表格中列数10
colmax = Sheets(inndex).Range("IV3").End(xlToLeft).Column

transnumber = CNtoW(colmax) '更新为新函数

'Debug.Print transnumber

If ThisWorkbook.Sheets(inndex).Name = "全年验收项目清单1209" Then

    a = 5

Else



For Each Rng In Sheets(inndex).Range("$a$3:" & "$" & transnumber & "$" & rowmax)
        'Debug.Print Rng.Value
     If Rng = deptname Then
         a = Rng.Column
         Exit For
     ElseIf Rng = "" Then

         a = "无"
     ElseIf Rng <> deptname Then
     End If

Next

End If

findcol = a

End Function

