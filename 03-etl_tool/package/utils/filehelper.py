#!/user/bin/env python2.7
# -*- coding:utf-8 -*-

"""
作业：zjj
日期：20231117
功能：

日志：

"""
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
        os.chmod(path, 755)
    else:
        pass

def read_file(path, charset='utf-8'):
    with codecs.open(path, 'r', charset) as h_file:
        return h_file.read()

def write_file(path, text, charset='utf-8', force_create_dir=True):
    if force_create_dir:
        dirs = os.path.split(path)[0]
        create_dirs(dirs)
    else:
        pass

    with codecs.open(path, 'w', charset) as h_file:
        h_file.write(text)
        return True

def main():
    file_name = "/tmp/test.log"
    content = "write file test by zjj at 20231118."
    write_file(file_name, content)
    print("file [%s] content: %s" % (file_name, read_file(file_name)))

if __name__ == '__main__':
    main()