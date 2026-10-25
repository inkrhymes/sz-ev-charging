#!/usr/bin/env bash
# 恢复：从 backup.sh 的产物重建数据库（可指定目标库名），可选还原数据湖。
#   bash scripts/restore.sh [备份目录|latest] [--target-db 库名] [--lake]
# 默认恢复到原库 sz_ev；--target-db 指定别的库名时会先建库再导入（用于演练）。
# --lake 会把 lake.tar.gz 解压到湖目录的上一级（原湖目录先改名为 lake.bak.<时间戳>）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${MYSQL_CONTAINER:-sz-ev-mysql}"
DB="${MYSQL_DATABASE:-sz_ev}"
ROOT_PW="${MYSQL_ROOT_PASSWORD:-rootpass}"
BACKUP_ROOT="${BACKUP_ROOT:-/workspace/backups}"
LAKE="${SZ_EV_LAKE:-$ROOT/data/lake}"
[ -d "$LAKE" ] || LAKE=/workspace/sz-ev-charging/data/lake

SRC="${1:-latest}"; shift || true
TARGET="$DB"; DO_LAKE=0
while [ $# -gt 0 ]; do case "$1" in
  --target-db) TARGET="$2"; shift 2;;
  --lake) DO_LAKE=1; shift;;
  *) echo "未知参数 $1"; exit 2;; esac; done
[ "$SRC" = latest ] && SRC="$(ls -1d "$BACKUP_ROOT"/2*/ | sort | tail -1)"
SRC="${SRC%/}"
DUMP="$SRC/mysql_${DB}.sql.gz"
[ -f "$DUMP" ] || { echo "找不到 $DUMP"; exit 1; }
( cd "$SRC" && sha256sum -c --quiet SHA256SUMS )
echo "[restore] $DUMP -> 库 $TARGET"

MYSQL=(docker exec -i "$CONTAINER" mysql -uroot -p"$ROOT_PW")
"${MYSQL[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$TARGET\` DEFAULT CHARACTER SET utf8mb4" 2>/dev/null
# dump 带 CREATE DATABASE/USE 原库名；改写成目标库名。
# 视图的 DEFINER（ev@%）去掉，由执行恢复的账号做定义者，否则 ev 对演练库无权限会导致建视图失败。
# 导入错误不再吞掉，失败时脚本直接退出。
gunzip -c "$DUMP" \
  | sed -E "s/^(CREATE DATABASE[^\`]*\`)$DB(\`)/\1$TARGET\2/; s/^USE \`$DB\`;/USE \`$TARGET\`;/; s#/\*!50013 DEFINER=[^*]*\*/##" \
  | "${MYSQL[@]}" 2> >(grep -v 'Using a password' >&2)
echo "[restore] 数据库已恢复到 $TARGET"

if [ "$DO_LAKE" = 1 ]; then
  [ -f "$SRC/lake.tar.gz" ] || { echo "备份里没有 lake.tar.gz"; exit 1; }
  if [ -d "$LAKE" ]; then mv "$LAKE" "$LAKE.bak.$(date +%Y%m%dT%H%M%S)"; fi
  tar -C "$(dirname "$LAKE")" -xzf "$SRC/lake.tar.gz"
  echo "[restore] 数据湖已还原到 $LAKE"
fi
