 Sub CommandButton1_Click()
    Application.ScreenUpdating = False                  '取消绘制屏幕
    Application.Calculation = xlCalculationManual       '将计算模式设置为“手动”
    
    Dim sheet_count As Integer
    Dim serialno As Integer
    Dim last_update_log As String
    
    
    
    
    sheet_count = Worksheets.Count
    serialno = 0
    
    Call delete_indexrows
    
    For i = 2 To sheet_count
        If Not LCase(Left(Sheets(i).Name, 3)) = "rem" Then
            'Sheets(i).Cells(4, 12) = "日"
            'Sheets(i).Cells(5, 12) = "13"

            Sheets(i).Cells(1, 1) = "返回"
            sheet_name = Sheets(i).Name
            last_update_log = get_update_log_date(sheet_name)
            Debug.Print last_update_log
            Sheets("index").Hyperlinks.Add Anchor:=ThisWorkbook.Sheets(Sheets(i).Name).Cells(1, 1), _
                Address:="", _
                SubAddress:="#index!A1"
            serialno = serialno + 1
            Sheets("index").Cells(serialno + 2, 2) = serialno
            Sheets("index").Cells(serialno + 2, 3) = Sheets(i).Name '目录名称赋值 为sheet页名称
            Sheets("index").Hyperlinks.Add Anchor:=ThisWorkbook.Sheets("index").Cells(serialno + 2, 3), _
                Address:="", _
                SubAddress:="'" & Sheets(i).Name & "'!A1"
            Sheets("index").Cells(serialno + 2, 4) = UCase(Sheets(i).Cells(5, 3).Value) '表英文名称
            Sheets("index").Cells(serialno + 2, 5) = Sheets(i).Cells(4, 3).Value '表中文名称
            Sheets("index").Cells(serialno + 2, 6) = Sheets(i).Cells(4, 8).Value '分析人员
            'Sheets("index").Cells(serialno + 2, 7) = "I" + UCase(Left(Sheets(i).Cells(5, 3).Value, 1)) + "L" '归属层次
            Sheets("index").Cells(serialno + 2, 7) = UCase(Left(Sheets(i).Cells(5, 3).Value, 1)) '归属层次
            'Sheets("index").Cells(serialno + 2, 8) = UCase(Right(Left(Sheets(i).Cells(5, 3).Value, 4), 2)) '归属主题
            Sheets("index").Cells(serialno + 2, 8) = UCase(Right(Left(Sheets(i).Cells(5, 3).Value, 3), 3)) '归属主题
            Sheets("index").Cells(serialno + 2, 9) = Sheets(i).Cells(4, 12).Value '时间粒度
            Sheets("index").Cells(serialno + 2, 10) = Sheets(i).Cells(5, 12).Value '保留周期
            Sheets("index").Cells(serialno + 2, 12) = Sheets(i).Cells(5, 8).Value '创建日期
            Sheets("index").Cells(serialno + 2, 13) = "Y" '是否自动生成脚本
            Sheets("index").Cells(serialno + 2, 14) = Sheets(i).Cells(4, 10).Value '算法
            Sheets("index").Cells(serialno + 2, 15) = LCase(Sheets(i).Cells(5, 3).Value + "_pc.sql") '脚本名称
            Sheets("index").Cells(serialno + 2, 16) = Sheets(i).Cells(4, 5).Value '主键
            Sheets("index").Cells(serialno + 2, 21) = last_update_log '主键
        Else
        End If
    Next
    
    Application.ScreenUpdating = True                   '打开绘制屏幕
    Application.Calculation = xlCalculationAutomatic    '将计算模式设置为“自动”

End Sub
Function get_update_log_date(ByVal ws_name) As String

    Dim ws As Worksheet
    Dim startrow As Long, endrow As Long
    Dim last_update_log As String
    Dim i As Long
    
    Set ws = ThisWorkbook.Sheets(ws_name)
    startrow = 0
    endrow = 0
    For i = 1 To ws.Cells(ws.Rows.Count, 2).End(xlUp).row
        If ws.Cells(i, 2).Value = "聚合层基本信息" Then
            startrow = i
        ElseIf ws.Cells(i, 2).Value = "聚合层字段映射（第1组）" Then
            endrow = i
            Exit For
        End If
    Next i
    
    'Debug.Print startrow & endrow
    If startrow = 0 Or endrow = 0 Or startrow >= endrow Then
        get_update_log_date = "格式错误，未找到关键字"
        Exit Function
    End If
    
    For i = endrow - 1 To startrow + 1 Step -1
        If Trim(ws.Cells(i, 2).Value) <> "" Then
            last_update_log = ws.Cells(i, 2).Value & " " & ws.Cells(i, 4).Value
            'Debug.Print last_update_log
            Exit For
        End If
    Next i
    
    If Trim(last_update_log) <> "" Then
        get_update_log_date = last_update_log
    
    Else
    
        get_update_log_date = "未找到有效的更新日志"
    End If

End Function

Function get_update_log_date1(sheetnamecell As Range) As String

    Dim ws As Worksheet
    Dim startrow As Long, endrow As Long
    Dim last_update_log As String
    Dim i As Long
    
    On Error Resume Next

        Set ws = ThisWorkbook.Sheets(sheetnamecell.Value)
    On Error GoTo 0
    startrow = 0
    endrow = 0
    
    If ws Is Nothing Then
        last_update_log = "工作表不存在"
        Exit Function
    End If
    
    For i = 1 To ws.Cells(ws.Rows.Count, 2).End(xlUp).row
        If ws.Cells(i, 2).Value = "聚合层基本信息" Then
            startrow = i
        ElseIf ws.Cells(i, 2).Value = "聚合层字段映射（第1组）" Then
            endrow = i
            Exit For
        End If
    Next i
    
    'Debug.Print startrow & endrow
    If startrow = 0 Or endrow = 0 Or startrow >= endrow Then
        get_update_log_date1 = "格式错误，未找到关键字"
        Exit Function
    End If
    
    For i = endrow - 1 To startrow + 1 Step -1
        If Trim(ws.Cells(i, 2).Value) <> "" Then
            last_update_log = ws.Cells(i, 2).Value
            Debug.Print last_update_log
            Exit For
        End If
    Next i
    
    If Trim(last_update_log) <> "" Then
        get_update_log_date1 = last_update_log
    
    Else
    
        get_update_log_date1 = "未找到有效的更新日志"
    End If



End Function

Sub delete_indexrows()
    Dim indexWs As Worksheet
    Set indexWs = ThisWorkbook.Sheets("index")
    
    ' 获取 index 工作表的最后一行
    Dim lastRow As Long
    lastRow = indexWs.Cells(indexWs.Rows.Count, "C").End(xlUp).row
    
    ' 删除第 3 行到最后一行的内容
    If lastRow >= 3 Then
        indexWs.Rows("3:" & lastRow).Clear
    End If
End Sub








Sub Create_PDM_Table()
'
' 创建AGL（聚合层）
'

'
Dim col As Long '列
Dim row As Long '行
Dim strTblName As String '表名
Dim creSQL As String, strSQL As String, strDelSQL As String, disSQL As String
Dim strDrpSQL As String
Dim comTblName As String, comColName As String
Dim comPKlName As String
Dim sheetName As String
Dim sourceWs As Worksheet


Dim strMaincomm As String '主注释
Dim strScriptName As String '脚本名称
Dim strComm1 As String
Dim strComm2 As String
Dim strComm3 As String
Dim strComm4 As String
Dim strComm5 As String
Dim strDIST As String '分布键
Dim strGRANT As String '权限管理
Dim ColType As String

Dim strCellcom As String '字段中文注释
Dim strCellcom1 As String '去掉回车后的字段中文注释
Dim strCellcom2 As String '处理英文单引号'后的中文注释

Dim strOper As String '操作员
Dim strScriptNameALL As String

'创建目录
Dim folderPath As String
'定义文件夹路径
folderPath = ThisWorkbook.Path & "\AGL_DDL\"
'创建文件夹
If Dir(folderPath, vbDirectory) = "" Then
   MkDir folderPath
Else
   '如果路径已存在则什么都不做
End If

sheetName = "rem-数据字典"
Set sourceWs = ThisWorkbook.Sheets(sheetName)

'下面是取日期的定义
 Dim currentDate As Date
 Dim lenMon As Integer
 Dim lenDay As Integer
 Dim YearStr As String
 Dim MonStr As String
 Dim DayStr As String
 Dim DateStr1 As String
 Dim DateStr2 As String

 currentDate = Date
 YearStr = Year(currentDate)
 lenMon = Len(Month(currentDate))
 lenDay = Len(Day(currentDate))
    If lenMon = 1 Then
       MonStr = "0" & Month(currentDate)
    Else
       MonStr = Month(currentDate)
    End If
    If lenDay = 1 Then
       DayStr = "0" & Day(currentDate)
    Else
       DayStr = Day(currentDate)
    End If
 DateStr1 = YearStr & MonStr & DayStr
 DateStr2 = YearStr & "-" & MonStr & "-" & DayStr
'日期处理完毕

    Set ADO_Stream = CreateObject("ADODB.Stream")
    ADO_Stream.Type = 2
    ADO_Stream.Mode = 3
    ADO_Stream.Charset = "UTF-8"
    ADO_Stream.Open


row = 2

Do While sourceWs.Cells(row, 1).Value <> ""

    '获取表英文名
    strTblName = sourceWs.Cells(row, 2).Value
    '表中文名称
    strTblChName = sourceWs.Cells(row, 3).Value
    '脚本名称
    strScriptName = LCase(strTblName)
    
    
    '获取归属schema
    If UCase(Left(strTblName, 3)) = "S01" Then
         table_schema = "AGL_CORP"
    ElseIf UCase(Left(strTblName, 3)) = "S02" Then
         table_schema = "AGL_RTLB"
    ElseIf UCase(Left(strTblName, 3)) = "S03" Then
         table_schema = "AGL_LOAN"
    ElseIf UCase(Left(strTblName, 3)) = "S04" Then
         table_schema = "AGL_ASSM"
    ElseIf UCase(Left(strTblName, 3)) = "S05" Then
         table_schema = "AGL_FINM"
    ElseIf UCase(Left(strTblName, 3)) = "S06" Then
         table_schema = "AGL_OPRS"
    ElseIf UCase(Left(strTblName, 3)) = "S07" Then
         table_schema = "AGL_COMM"
    ElseIf UCase(Left(strTblName, 3)) = "S08" Then
         table_schema = "AGL_COMM"
    ElseIf UCase(Left(strTblName, 3)) = "S09" Then
         table_schema = "AGL_COMM"
    ElseIf UCase(Left(strTblName, 3)) = "S10" Then
         table_schema = "AGL_COMM"
    ElseIf UCase(Left(strTblName, 3)) = "S11" Then
         table_schema = "AGL_COMM"
    ElseIf UCase(Left(strTblName, 3)) = "S12" Then
         table_schema = "AGL_COMM"
    Else
         table_schema = "AGL_COMM"
    End If
    
    
    '增全量判断
    If Cells(row, 11).Value <> "" And Cells(row, 11).Value = "EV_I" Then
        strScriptType = "流水表"
    ElseIf Cells(row, 11).Value <> "" And Cells(row, 11).Value = "ST_F" Then
        strScriptType = "拉链表"
    Else
        strScriptType = "算法错误，请检查！！！"
    End If
    
    strScriptName = LCase(strTblName)
    strOper = Environ("Username") & "@" & Environ("COMPUTERNAME")
    strMaincomm = "/*" & Chr(10) & "Purpose:    聚合模型层-" & strScriptType & "建表脚本" & Chr(10) & "Author:     Sunline" & Chr(10) & _
                   "Usage:      spark-sql --keytab /home/etluser/userkey/CL.keytab --principal CL -f " & strScriptName & Chr(10) & "CreateDate: " & DateStr1 & _
                      Chr(10) & "FileType:   DDL" & Chr(10) & "Logs:" & Chr(10) & "    " & strOper & " " & DateStr2 & " 新建脚本" & Chr(10) & "*/" & Chr(10) & Chr(10)
    strComm1 = "--\timing" & Chr(10) & "--\echo ""drop table " & table_schema & "." & strTblName & """" & Chr(10)





    strCellcom = Cells(row, 6).Value '字段中文注释
    strCellcom1 = Replace(Replace(strCellcom, vbCrLf, " "), Chr(10), " ") '去掉回车后的字段中文注释
    strCellcom2 = Replace(strCellcom, "'", "''") '处理英文单引号'后的中文注释




    If Cells(row, 2).Value <> Cells(row - 1, 2).Value Then

        ADO_Stream.WriteText strMaincomm
        ADO_Stream.WriteText strComm1

        strDrpSQL = "DROP TABLE IF EXISTS  " & table_schema & "." & strTblName & ";" & Chr(10) & Chr(10)
        ADO_Stream.WriteText strDrpSQL

        strComm2 = "--\echo ""create table  " & table_schema & "." & strTblName & """" & Chr(10)
        ADO_Stream.WriteText strComm2
        creSQL = "CREATE TABLE  " & table_schema & "." + strTblName + " (" & Chr(10)
        ADO_Stream.WriteText creSQL

        comTblName = Cells(row, 3).Value
    End If



    If Cells(row, 9).Value <> "" Then
       If Cells(row, 9).Value = "N" Then
           strDIST = strDIST
       End If
    End If
    If Cells(row, 9).Value <> "" Then
        If Cells(row, 9).Value = "Y" Then
           strDIST = strDIST & Cells(row, 5).Value & ","
        End If
    End If




    If Cells(row, 8).Value <> "" Then
    '判断是否可空
        ColType = UCase(Cells(row, 7).Value) '字段类型处理 字符类型统一处理为String
        
        If InStr(ColType, "CHAR") > 0 Or InStr(ColType, "TIMESTAMP") > 0 Then
            ColType = "STRING"
        Else
            ColType = UCase(Cells(row, 7).Value)
        End If
        
        
        If Cells(row, 2).Value <> Cells(row + 1, 2).Value Then
            If Cells(row, 11).Value <> "" And Cells(row, 11).Value = "ST_F" Then
            
                strSQL = "    ," & UCase(Cells(row, 5).Value) & " " & ColType & " COMMENT '" & strCellcom1 & "'" & Chr(10) & ")" & Chr(10) & "COMMENT '" & comTblName & "'" & Chr(10) & "PARTITIONED BY (" & Chr(10) & "    PT_DT STRING COMMENT '表分区日期'" + Chr(10) + ")" + Chr(10) + "STORED AS ORC" + Chr(10) + "TBLPROPERTIES (""orc.compress""=""SNAPPY"")" + Chr(10) + ";"
            
            ElseIf Cells(row, 11).Value <> "" And Cells(row, 11).Value = "EV_I" Then
                
                strSQL = "    ," & UCase(Cells(row, 5).Value) & " " & ColType & " COMMENT '" & strCellcom1 & "'" & Chr(10) & ")" & Chr(10) & "WITH (ORIENTATION=COLUMN, COMPRESSION=LOW, COMPRESSLEVEL=0)"
            
            End If
                    
        ElseIf Cells(row, 2).Value <> Cells(row - 1, 2).Value Then
        
                      strSQL = "     " & UCase(Cells(row, 5).Value) & " " & ColType & " COMMENT '" & strCellcom1 & "'"
        Else
        
                      strSQL = "    ," & UCase(Cells(row, 5).Value) & " " & ColType & " COMMENT '" & strCellcom1 & "'"
                      
        End If

   
    End If

    strSQL = strSQL + Chr(10)
    
    ADO_Stream.WriteText strSQL

    If Cells(row, 2).Value <> Cells(row + 1, 2).Value Then

        strComm5 = "-- Name:  " & table_schema & "." & strTblName & "; Type: ACL; Schema:  " & table_schema & "" & Chr(10)
        ADO_Stream.WriteText strComm5

        strGRANT = "--REVOKE ALL ON TABLE  " & table_schema & "." & strTblName & " FROM PUBLIC;" & Chr(10) & _
                    "--REVOKE ALL ON TABLE  " & table_schema & "." & strTblName & " FROM ${PDM_USER};" & Chr(10) & _
                    "--GRANT ALL ON TABLE  " & table_schema & "." & strTblName & " TO ${PDM_USER};" & Chr(10) & _
                    "--GRANT SELECT ON TABLE  " & table_schema & "." & strTblName & " TO ${PUBLIC_ROLE};" & Chr(10) & Chr(10)
        ADO_Stream.WriteText strGRANT

        ADO_Stream.SaveToFile folderPath & strScriptName & ".sql", 2
        ADO_Stream.SetEOS
        strScriptNameALL = strScriptNameALL & Chr(10) & strScriptName & ".sql"

    End If


    row = row + 1

 Loop


        ADO_Stream.Close
        Set ADO_Stream = Nothing
        'ADO_Stream.SaveToFile ThisWorkbook.Path & "\Create_PDM_tables_ddl.sql", 2
        'ADO_Stream.Close
        'Set ADO_Stream = Nothing

        'MsgBox ("P层建表语句文件：Create_PDM_tables_ddl.sql 已经创建成功！")
        MsgBox ("聚合层建表语句文件已经创建成功，本地路径：" & folderPath & Chr(10) & "文件列表为：" & strScriptNameALL)


End Sub





















