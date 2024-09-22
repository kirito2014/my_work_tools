import tkinter as tk

class ScrollableChecklist:
    def __init__(self, master):
        self.master = master
        self.master.title("滚动复选框")

        self.frame = tk.Frame(self.master)
        self.frame.pack(fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(self.frame, bg='white')
        self.vsb = tk.Scrollbar(self.frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.checkboxes = []
        self.populate()

    def populate(self):
        for i in range(20):  # 创建20个复选框
            var = tk.IntVar()
            cb = tk.Checkbutton(self.canvas, text=f"选项 {i}", variable=var)
            self.checkboxes.append(var)
            self.canvas.create_window(10, 20 + i*25, window=cb, anchor="nw")

        self.canvas.config(scrollregion=self.canvas.bbox("all"))

def main():
    root = tk.Tk()
    app = ScrollableChecklist(root)
    root.mainloop()

if __name__ == "__main__":
    main()
