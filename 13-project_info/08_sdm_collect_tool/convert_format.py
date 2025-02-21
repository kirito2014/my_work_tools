import os
import sys

"""
将输入文件转换为unix格式并保存为UTF-8无BOM编码
args:
    input_file(str):输入文件路径
    output_file(str):输出路径(可选)
"""


try:
    import chardet #检测文件编码
except ImportError:
    print("请先安装chardet库.:pip install chardet")
    sys.exit(1)
def detect_file_encoding(file_path):
    """检查文件编码"""
    with open(file_path,'rb') as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    return result['encoding']
def convert_to_unix_and_utf8(input_file,output_file):
    try:
        #检测文件编码
        encoding = detect_file_encoding(input_file)
        if not encoding:
            raise ValueError("无法检测文件编码")
        print(f"文件编码：{encoding}")
        #尝试读取文件信息
        try:
            with open(input_file,'r',encoding=encoding) as infile:
                content = infile.read()
        except UnicodeDecodeError:
            #检测的编码无效尝试使用GBK编码
            print("使用检测编码失败，尝试使用GBK解码...")
            with open(input_file,'r',encoding='gb18030') as infile:
                content = infile.read()
        #替换文件结尾符 \n
        content = content.replace('\r\n','\n').replace('\r','\n')
        #保存新文件
        with open(output_file,'w',encoding='utf-8',newline='') as outfile:
            outfile.write(content)

        print(f"文件转换完成:{output_file}")
    except Exception as e:
        print(f"文件转换失败:{e}")

if __name__ == "__main__":
    if len(sys.argv)<2:
        print("用法:python convert_format.py <输入文件路径> [输出文件路径]")
        sys.exit(1)

    input_file = sys.argv[1]

    if not os.path.isfile(input_file):
        print(f"文件不存在：{input_file}")
        sys.exit(1)
    #没有提供保存路径的情况下默覆盖源文件
    output_file = sys.argv[2] if len(sys.argv) >2 else input_file
    convert_to_unix_and_utf8(input_file, output_file)