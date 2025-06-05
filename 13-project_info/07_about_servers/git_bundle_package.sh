#!/bin/bash
# 用法：./export_changes_clean.sh <基准分支> <变更分支> <输出ZIP名>
# 示例：./export_changes_clean.sh main feature_branch changes.zip

BASE_BRANCH="$1"
CHANGE_BRANCH="$2"
OUTPUT_ZIP="$3"
TEMP_DIR="temp_zip_content"

# 检查参数
[ $# -ne 3 ] && { echo "用法：$0 <基准分支> <变更分支> <输出ZIP名>"; exit 1; }

# 检查变更分支
git show-ref --verify --quiet "refs/heads/$CHANGE_BRANCH" || { echo "错误：分支 $CHANGE_BRANCH 不存在"; exit 1; }

# 检出分支
git checkout "$CHANGE_BRANCH" && git pull --quiet 2>/dev/null

# 获取变更文件
git diff --name-only "$BASE_BRANCH".."$CHANGE_BRANCH" > changed_files.txt
[ ! -s changed_files.txt ] && { echo "没有检测到变更文件"; rm changed_files.txt; exit 0; }

# 创建临时工作区
mkdir -p "$TEMP_DIR" && cd "$TEMP_DIR" || exit 1

# 处理变更文件
while IFS= read -r file; do
    mkdir -p "$(dirname "$file")"
    [ -f "../$file" ] && cp "../$file" "$file"
done < ../changed_files.txt

# 创建必要空目录（不生成.keep文件）
required_dirs=(
    "agls01/createtable"
    "agls01/config" 
    "agls01/pgm/hql"
)

for dir in "${required_dirs[@]}"; do
    # 使用zip直接添加空目录（不需要.keep文件）
    mkdir -p "$dir"
done

# 特殊打包方式保留空目录
zip -r "../$OUTPUT_ZIP" . -x "*" >/dev/null
for dir in $(find . -type d); do
    zip -r "../$OUTPUT_ZIP" "$dir" -x "*/.*" >/dev/null
done

# 添加真实文件
while IFS= read -r file; do
    [ -f "$file" ] && zip -r "../$OUTPUT_ZIP" "$file" >/dev/null
done < ../changed_files.txt

# 清理
cd .. && rm -rf "$TEMP_DIR" changed_files.txt
echo "生成成功: $OUTPUT_ZIP"
echo "包含内容:"
unzip -l "$OUTPUT_ZIP" | awk 'NR>3 && !/\/$/ {print $4}'