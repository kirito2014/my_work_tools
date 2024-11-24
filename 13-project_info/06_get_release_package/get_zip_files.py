import os,sys
import shutil
import zipfile
from datetime import datetime

def zip_subfolders(folder_path,version):
    current_date = datetime.now().strftime('%Y%m%d')

    for item in os.listdir(folder_path):
        if item.endswith('.zip'):
            os.remove(os.path.join(folder_path,item))
    
    for subfolder in os.listdir(folder_path):
        subfolder_path = os.path.join(folder_path,subfolder)

        if os.path.isdir(subfolder_path):
            zip_name = f"{subfolder.upper()}_{current_date}_V{version}.zip"
            zip_path = os.path.join(folder_path,zip_name)

        with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as zipf:
            for root,_,files in os.walk(subfolder_path):
                for file in files:
                    file_path = os.path.join(root,file)
                    zipf.write(file_path,os.path.relpath(file_path,folder_path))
    print("文件打包完成")

if __name__ == "__main__":
    # folder_path = "D:/git/tools/06_get_release_package/agl-package-0824-1/agl-package-new"
    # version = "1.0.0"
    if len(sys.argv) != 3:
            print("Usage: python get_zip_files.py <folder_path> <version>")
            sys.exit(1)
    folder_path = sys.argv[1]   
    version = sys.argv[2]  

    if os.path.isdir(folder_path):
        zip_subfolders(folder_path,version)
    else:
        print("文件路径无效")
