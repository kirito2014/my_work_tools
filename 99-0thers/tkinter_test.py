from tkinter import messagebox
from tkinter import *
from tkinter.ttk import *
from typing import Dict

# 美化界面
# from ttkbootstrap import Style


class WinGUI(Tk):
    widget_dic: Dict[str, Widget] = {}


    def __init__(self):
        super().__init__()

        self.v1 = BooleanVar()
        self.v2 = BooleanVar()
        self.v3 = BooleanVar()

        self.__win()
        self.widget_dic["tk_button_userBtn"] = self.__tk_button_userBtn(self)
        self.widget_dic["tk_input_user"] = self.__tk_input_user(self)
        self.widget_dic["tk_button_pwdBtn"] = self.__tk_button_pwdBtn(self)
        self.widget_dic["tk_input_pwd"] = self.__tk_input_pwd(self)
        self.widget_dic["tk_radio_button_l1"] = self.__tk_radio_button_l1(self)
        self.widget_dic["tk_radio_button_l2"] = self.__tk_radio_button_l2(self)
        self.widget_dic["tk_radio_button_l3"] = self.__tk_radio_button_l3(self)
        self.widget_dic["tk_check_button_ball1"] = self.__tk_check_button_ball1(self)
        self.widget_dic["tk_check_button_ball2"] = self.__tk_check_button_ball2(self)
        self.widget_dic["tk_check_button_ball3"] = self.__tk_check_button_ball3(self)
        self.widget_dic["tk_list_box_list"] = self.__tk_list_box_list(self)
        self.widget_dic["tk_select_box_choice"] = self.__tk_select_box_choice(self)
        self.widget_dic["tk_frame_lgypbziv"] = self.__tk_frame_lgypbziv(self)
        self.widget_dic["tk_table_table"] = self.__tk_table_table(self)



    def __win(self):
        self.title("Tkinter布局助手")
        # 设置窗口大小、居中
        width = 677
        height = 435
        screenwidth = self.winfo_screenwidth()
        screenheight = self.winfo_screenheight()
        geometry = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
        self.geometry(geometry)
        self.resizable(width=False, height=False)

        # 自动隐藏滚动条

    def scrollbar_autohide(self, bar, widget):
        self.__scrollbar_hide(bar, widget)
        widget.bind("<Enter>", lambda e: self.__scrollbar_show(bar, widget))
        bar.bind("<Enter>", lambda e: self.__scrollbar_show(bar, widget))
        widget.bind("<Leave>", lambda e: self.__scrollbar_hide(bar, widget))
        bar.bind("<Leave>", lambda e: self.__scrollbar_hide(bar, widget))

    def __scrollbar_show(self, bar, widget):
        bar.lift(widget)

    def __scrollbar_hide(self, bar, widget):
        bar.lower(widget)

    def __tk_button_userBtn(self, parent):
        btn = Button(parent, text="用户名")
        btn.place(x=60, y=60, width=98, height=30)
        return btn

    def __tk_input_user(self, parent):
        ipt = Entry(parent)
        ipt.place(x=180, y=60, width=198, height=30)
        return ipt

    def __tk_button_pwdBtn(self, parent):
        btn = Button(parent, text="密码",bootstyle="warning")
        btn.place(x=60, y=110, width=100, height=30)
        return btn

    def __tk_input_pwd(self, parent):
        ipt = Entry(parent)
        ipt.place(x=180, y=110, width=197, height=30)
        return ipt

    def __tk_radio_button_l1(self, parent):
        rb = Radiobutton(parent, text="男",value=1)
        rb.place(x=60, y=190, width=80, height=30)
        return rb

    def __tk_radio_button_l2(self, parent):
        rb = Radiobutton(parent, text="女",value=2)
        rb.place(x=180, y=190, width=80, height=30)
        return rb

    def __tk_radio_button_l3(self, parent):
        rb = Radiobutton(parent, text="未知",value=3)
        rb.place(x=300, y=190, width=80, height=30)
        return rb

    def __tk_check_button_ball1(self, parent):
        cb = Checkbutton(parent, text="篮球",variable=self.v1)
        cb.place(x=60, y=250, width=80, height=30)
        return cb

    def __tk_check_button_ball2(self, parent):
        cb = Checkbutton(parent, text="排球",variable=self.v2)
        cb.place(x=180, y=250, width=80, height=30)
        return cb

    def __tk_check_button_ball3(self, parent):
        cb = Checkbutton(parent, text="足球",variable=self.v3)
        cb.place(x=300, y=250, width=80, height=30)
        return cb

    def __tk_list_box_list(self, parent):
        lb = Listbox(parent)
        lb.insert(END, "编程语言")
        lb.insert(END, "Python")
        lb.insert(END, "Tkinter Helper")
        lb.insert(END, "java")
        lb.place(x=60, y=310, width=150, height=100)


        return lb

    def __tk_select_box_choice(self, parent):
        cb = Combobox(parent, state="readonly")
        cb['values'] = ("列表框", "Python", "Tkinter Helper")
        cb.place(x=230, y=310, width=150, height=30)
        return cb

    def __tk_table_table(self, parent):
        # 表头字段 表头宽度
        columns = {"ID": 44, "姓名": 44, "性别": 44}
        # 初始化表格 表格是基于Treeview，tkinter本身没有表格。show="headings" 为隐藏首列。
        tk_table = Treeview(parent, show="headings", columns=list(columns))
        for text, width in columns.items():  # 批量设置列属性
            tk_table.heading(text, text=text, anchor='center')
            tk_table.column(text, anchor='center', width=width, stretch=False)  # stretch 不自动拉伸

        tk_table.place(x=220, y=350, width=442, height=70)

        return tk_table

    def __tk_frame_lgypbziv(self, parent):
        frame = Frame(parent)
        frame.place(x=420, y=60, width=245, height=202)

        self.widget_dic["tk_label_sex"] = self.__tk_label_sex(frame)
        self.widget_dic["tk_label_class"] = self.__tk_label_class(frame)
        self.widget_dic["tk_text_textArea"] = self.__tk_text_textArea(frame)
        return frame

    def __tk_label_sex(self, parent):
        label = Label(parent, text="性别：", anchor="center")
        label.place(x=20, y=20, width=50, height=30)
        return label

    def __tk_label_class(self, parent):
        label = Label(parent, text="班级：", anchor="center")
        label.place(x=20, y=70, width=50, height=30)
        return label

    def __tk_text_textArea(self, parent):
        text = Text(parent)
        text.place(x=80, y=20, width=150, height=161)

        return text


class Win(WinGUI):
    def __init__(self):
        super().__init__()
        self.__event_bind()

    def user(self, evt):
        print("<Button>事件未处理", evt)
        print("输入文本框内容：", self.widget_dic.get("tk_text_textArea").get("1.0", "end-1c"))

    def pwd(self, evt):
        print("<Button>事件未处理", evt)
        print(self.v1.get())
        print(self.v2.get())
        print(self.v3.get())
        print("输入：", self.widget_dic.get("tk_input_pwd").get())
        messagebox.showerror("提示", message="密码错误")

    def boxSelect(self, evt):
        print("<<ListboxSelect>>事件未处理", evt)
        print(self.widget_dic["tk_list_box_list"].curselection())
        print(self.widget_dic["tk_list_box_list"].get(self.widget_dic["tk_list_box_list"].curselection()))
        print(self.widget_dic["tk_list_box_list"].get(1))


    def __event_bind(self):
        self.widget_dic["tk_button_userBtn"].bind('<Button>', self.user)
        self.widget_dic["tk_button_pwdBtn"].bind('<Button>', self.pwd)
        self.widget_dic["tk_list_box_list"].bind('<<ListboxSelect>>', self.boxSelect)


if __name__ == "__main__":
    # 导入ttkbootstrap，可以在文件头导入
    import ttkbootstrap
    win = Win()
    # 主题：cyborg,journal,darkly,flatly,solar,minty,litera,united,pulse,cosmo,lumen
    # 在这里使用主题
    style = ttkbootstrap.Style(theme='morph')
    win.mainloop()