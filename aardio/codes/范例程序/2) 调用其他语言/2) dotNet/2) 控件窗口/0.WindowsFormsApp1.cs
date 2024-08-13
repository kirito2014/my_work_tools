using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace WindowsFormsApp1
{
    public class Form1: Form
    {

        //定义一个委托类型
        public delegate void aardioCallback();
        
        //定义一个委托类型字段
        public aardioCallback onButton1Click;
        
        public Form1()
        {
            InitializeComponent();
        }
        
        private void button1_Click(object sender, EventArgs e)
        {
			onButton1Click();
        }

        private System.ComponentModel.IContainer components = null;


        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        private void InitializeComponent()
        {
            //无边框窗口
            this.FormBorderStyle = FormBorderStyle.None;
            
            this.button1 = new System.Windows.Forms.Button();
            this.SuspendLayout();

            this.button1.Location = new System.Drawing.Point(298, 185);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(343, 122);
            this.button1.TabIndex = 0;
            this.button1.Text = "这是 C# 创建的控钮";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);

            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.button1);
            this.Name = "Form1";
            this.Text = "Form1";
            this.ResumeLayout(false);
        }

        private System.Windows.Forms.Button button1;
    }
}
