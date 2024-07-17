# -*- coding:utf-8 -*-
import sys
import os
import codecs

def create_dirs(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == 17:
                pass
            else:
                raise e
        #os.chmod(path,0755)
    else:
        pass 
def read_file(path,charset='utf-8'):
    with codecs.open(path,'r',charset) as h_file:
        return h_file.read()
def write_file(path,content,charset='utf-8',force_create_dir=True):
    if force_create_dir:
        dirs = os.path.split(path)[0]
        create_dirs(dirs)
    else:
        pass
    with codecs.open(path,'w',charset) as h_file:
        h_file.write(content)
        return True
def main():
    file_name = r"D:\vscode\sunline_etl_tools\logs\test.log"
    content = "write file test by wmj at 20231214."
    write_file(file_name,content)
    print("file [%s] content:%s" %(file_name,read_file(file_name)))

def get_file_path():
    current_path = os.path.dirname(os.getcwd())
    #file_name = r"D:vscode\sunline_etl_tools\logs\test.log"
    file_path = os.path.join(current_path)
    return file_path

if __name__ == "__main__":
    print(get_file_path())