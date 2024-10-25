import os
import sys

from package.function import resume_generator



def main():
    args = sys.argv
    if len(args) != 2:
        print("Usage: python gen_resume.py <resume_file>")
        return
    file_path = args[1]

    print(file_path)
    resume_generator.gen_resume(file_path)
    print("生成简历完成")

if __name__ == '__main__':
    main()