VERSION 5.00
Begin VB.UserControl UserControl1 
   ClientHeight    =   3600
   ClientLeft      =   0
   ClientTop       =   0
   ClientWidth     =   4800
   ScaleHeight     =   3600
   ScaleWidth      =   4800
   Begin VB.Image Image1 
      Height          =   3615
      Left            =   0
      Stretch         =   -1  'True
      Top             =   0
      Width           =   4815
   End
End
Attribute VB_Name = "UserControl1"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = True
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = True

'声明一个普通变量
Dim TestPropertyValue As Integer

'声明一个事件，在 aardio 中可以响应这个事件，
'注意参数加了ByRef表示传址，在 aardio 中就可以修改这这个参数的值
Public Event OnImageClick(ByRef TestPropertyValue As Integer)

'VB6 里点了 Image 图像控件触发的事件
Private Sub Image1_Click()
    '触发 COM 控件的事件（ 换句话说就是调用 aardio 中的函数 ）
    RaiseEvent OnImageClick(TestPropertyValue)
End Sub

'窗口调整大小触发这个函数，注意 aardio 控件都是自适应缩放的
Private Sub UserControl_Resize()
    Image1.Width = UserControl.Width
    Image1.Height = UserControl.Height
End Sub

'定义读属性 TestProperty 的函数，这是带参数的属性
Public Property Get TestProperty(Param As Integer) As Integer
    TestProperty = TestPropertyValue + Param
End Property

'定义写属性 TestProperty 的函数，带参数属性（参数要跟上面一致）
Public Property Let TestProperty(Param As Integer, ByVal v As Integer)
    TestPropertyValue = v - Param
End Property

'定义写属性 Picture 的函数，参数是一个 IDispatch 接口的 COM 对象
Public Property Let Picture(ByVal pic As Variant)
    Image1.Picture = pic
End Property

'定义 aardio 可以调用的控件方法，aardio 对象在 VB 里表示为 IDispatch 接口的 COM 对象
Public Sub CallAnything(ByVal dispatchObject As Object)

    '读写带参数属性
    dispatchObject("name") = "value"
    
    '直接调用 aardio 对象函数，读写 aardio 对象属性。
    dispatchObject.Log ("这是来自 VB 的参数。  也可以这样读 aardio 对象属性：" + dispatchObject.Name)
    
    Dim v As Variant
    
    'aardio 对象自动支持 VB 枚举接口（ IEnumVARIANT 接口 ）
    For Each Key In dispatchObject
        v = dispatchObject(Key)
        
        If TypeName(v) = "String" Then
            MsgBox v
            
            Exit For
        End If
    Next Key
    
    
End Sub

'定义其他 aardio 可以调用的控件函数
Public Function Add(ByVal a As Integer, ByVal b As Integer)
   Add = a + b
End Function

'测试输出参数
Public Function GetOutStr(ByRef str As String) As Integer
    str = "测试字符串"
    
    GetOutStr = Len(str)
End Function


