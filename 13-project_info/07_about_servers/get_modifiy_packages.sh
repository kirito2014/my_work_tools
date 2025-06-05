#!/bin/bash
# 用法：./export_changes.sh <基准分支> <输出ZIP名>
# 示例：./export_changes.sh main changes.zip

BASE_BRANCH="$1"  # 基准分支（如 main）
OUTPUT_ZIP="$2"   # 输出的ZIP文件名（如 changes.zip）

# 1. 获取当前分支名称
CURRENT_BRANCH=$(git branch --show-current)

# 2. 找出所有变更的文件（相比基准分支）
git diff --name-only $BASE_BRANCH..$CURRENT_BRANCH > changed_files.txt

# 3. 打包这些文件（如果存在变更）
if [ -s changed_files.txt ]; then
    echo "正在打包变更文件到 $OUTPUT_ZIP ..."
    zip -r "$OUTPUT_ZIP" $(cat changed_files.txt)
else
    echo "没有检测到变更文件！"
fi

# 4. 清理临时文件
rm -f changed_files.txt

echo "完成！变更已保存到 $OUTPUT_ZIP"