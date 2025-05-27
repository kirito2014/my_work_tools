#!/bin/bash

# 参数检查
if [ $# -ne 2 ]; then
  echo "用法: $0 <分支名称> <表名文件>"
  exit 1
fi

BRANCH_NAME=$1
TABLE_FILE=$2

# 检查文件是否存在
if [ ! -f "$TABLE_FILE" ]; then
  echo "错误: 表名文件 $TABLE_FILE 不存在"
  exit 1
fi

# 处理表名文件：删除空行、前后空格
TEMP_FILE=$(mktemp)
sed -e '/^[[:space:]]*$/d' -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' "$TABLE_FILE" > "$TEMP_FILE"

# 从agl-master创建新分支
echo "正在从agl-master创建分支 $BRANCH_NAME..."
git checkout agl-master || { echo "无法切换到agl-master分支"; exit 1; }
git pull origin agl-master || { echo "拉取agl-master最新代码失败"; exit 1; }
git checkout -b "$BRANCH_NAME" || { echo "创建分支失败"; exit 1; }

# 提取所有作业编号
JOB_IDS=$(awk -F'|' '{print $2}' "$TEMP_FILE" | sort | uniq)

# 保留需要的文件
echo "开始清理分支内容，保留指定文件..."

# 1. 保留createtable文件夹下指定的.hql文件
while IFS='|' read -r TABLE_NAME JOB_ID; do
  # 保留createtable_agl_xxxxx.hql文件 TABLE_NAME小写
  TABLE_NAME=$(echo "$TABLE_NAME" | tr '[:upper:]' '[:lower:]')
  CREATETABLE_FILE="createtable/createtable_${TABLE_NAME}.hql"
  if [ -f "$CREATETABLE_FILE" ]; then
    git reset -- "$CREATETABLE_FILE"
    git checkout -- "$CREATETABLE_FILE"
  fi
done < "$TEMP_FILE"

# 2. 保留config文件夹下作业编号.csv文件
for JOB_ID in $JOB_IDS; do
  CONFIG_FILE="config/${JOB_ID}.csv"
  if [ -f "$CONFIG_FILE" ]; then
    git reset -- "$CONFIG_FILE"
    git checkout -- "$CONFIG_FILE"
  fi
done

# 3. 保留pgm/hql/作业编号文件夹下的所有内容
for JOB_ID in $JOB_IDS; do
  JOB_DIR="pgm/hql/${JOB_ID}"
  if [ -d "$JOB_DIR" ]; then
    git reset -- "$JOB_DIR"
    git checkout -- "$JOB_DIR"
  fi
done

# 删除其他所有文件（除了.git目录和表名文件）
echo "正在删除不需要的文件..."
git ls-files | grep -v -E "^\.git/" | \
  grep -v -f <(awk -F'|' '{print "createtable/createtable_agl_" substr($1, 5) "\.hql"}' "$TEMP_FILE") | \
  grep -v -f <(awk -F'|' '{print "config/" $2 "\.csv"}' "$TEMP_FILE") | \
  grep -v -f <(awk -F'|' '{print "pgm/hql/" $2 "/"}' "$TEMP_FILE") | \
  while read -r file; do git rm -rf --cached "$file"; done

# 提交变更
echo "提交分支初始化变更..."
git add -u
git commit -m "初始化分支 $BRANCH_NAME: 根据 $TABLE_FILE 保留指定文件"

# 清理临时文件
rm -f "$TEMP_FILE"

echo "分支 $BRANCH_NAME 初始化完成!"