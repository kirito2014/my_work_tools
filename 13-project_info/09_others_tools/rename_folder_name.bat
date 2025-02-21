@echo off
setlocal enabledelayedexpansion
::设置要替换的字符串和新字符串
set "old_prefix=sdm"
set "new_prefix=对公组"
::便利目录下所有以sdm开头的文件夹
for /d %%d in (%old_prefix%*) do (
    set "old_folder=%%d"
    set "new_folder=!old_folder:%old_prefix%=%new_prefix%!"
    
    ::重命名文件夹
    ren "%%d" "!new_folder!"
    
    pushd "!new_folder!"
    
    for %%f in (*) do (
    set "old_file=%%f"
    set "new_file=!old_file:%old_prefix%=%new_prefix%!"
    
    ren "%%f" "!new_file!"
    )
    
    popd

)
echo complete!
pause