#! /bin/bash
#替换给定文件加下所有以_pc.hql结尾的文件中 如果文件中已经存在参数则不操作，如果不存在参数则在指定位置添加参数
#/* 1.1 drop main temp table */ 前 或set spark.sql.orc.compression.codec=zstd; 后添加set spark.sql.mergeSmallFiles.enabled=false;
#在/* 3.2 put data into target table */ 前一行添加set spark.sql.mergeSmallFiles.enabled=true;
#添加完检查是否成功

#接收文件路径参数
script_path=$1

#检查路径合法性
if [[ ! -d "$script_path" ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 脚本路径不存在:$script_path"
    exit 1
fi 

#定义列表名称
inserted_files=()
#已经插入过的列表
a_inserted_files=()

#循环路径下的所有文件(以_pc.hql)
find "$script_path" -type f -name "*_pc.hql" | while read -r file; do 
    #检查文件结尾
    #if [[ "$file" == *_pc.hql ]]; then 
        #检查文件是否已经包含set spark.sql.mergeSmallFiles.enabled=false; 和 set spark.sql.mergeSmallFiles.enabled=true; 字符
    if grep -q "set spark.sql.mergeSmallFiles.enabled=false;" "$file" && grep -q "set spark.sql.mergeSmallFiles.enabled=true;" "$file"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 文件 $file 已经包含小文件合并参数，跳过处理."
        a_inserted_files+=("$file")
        echo $file >> inserted_file.txt
        continue
    fi

    #如果没有参数则插入
    temp_file=$(mktemp)
    #插入 set spark.sql.mergeSmallFiles.enabled=false; 到 /* 1.1 drop main temp table */前 
    #插入 set spark.sql.mergeSmallFiles.enabled=true; 到 /* 3.2 put data into target table */ 前
    awk '
        /\/\* 1\.1 drop main temp table \*\// && !found_false {
            print "set spark.sql.mergeSmallFiles.enabled=false;"
            found_false = 1
        }
        /\/\* 3\.2 put data into target table \*\// && !found_true {
            print "set spark.sql.mergeSmallFiles.enabled=true;"
            found_true = 1
        }
        { print }
    ' "$file" >"$temp_file"

    #检查参数是否插入文件成功
    if grep -q "set spark.sql.mergeSmallFiles.enabled=false;" "$temp_file" && grep -q "set spark.sql.mergeSmallFiles.enabled=true;" "$temp_file"; then
        mv "$temp_file" "$file"
        inserted_files+=("$file")
        echo $file >> insert_file.txt
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 文件 $file 插入小文件合并参数成功."
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') [ FAILED ] 文件 $file 插入小文件合并参数失败."
        rm "$temp_file"
    fi
    #else
    #     echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 文件 $file 不是要处理的文件格式(_pc.hql结尾),跳过处理."
    # fi
done

#输出替换的文件列表
# echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 以下是已替换完成的列表:\n"> inserted_file.txt
# for inserted_file in "${inserted_files[@]}";do
#     echo $inserted_file > inserted_file.txt
# done
# #无需替换的脚本文件列表
# echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 以下是无需替换参数的列表\n"> inserted_file.txt
# for a_inserted_file in "${a_inserted_files[@]}";do
#     echo $a_inserted_file > inserted_file.txt
# done
# echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 总插入文件个数：${#inserted_files[@]}."
# echo "$(date '+%Y-%m-%d %H:%M:%S') [ INFO ] 无需替换的文件个数：${#a_inserted_files[@]}."