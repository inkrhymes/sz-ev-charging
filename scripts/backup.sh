#!/usr/bin/env bash
# 备份：MySQL sz_ev（mysqldump | gzip）+ 数据湖快照（tar.gz，跳过 _tmp/_staging 临时目录）
# 输出到 $BACKUP_ROOT/<时间戳>/，只保留最近 $KEEP 份。
#   bash scripts/backup.sh            # 库 + 湖
#   bash scripts/backup.sh --db-only  # 只备份库
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${MYSQL_CONTAINER:-sz-ev-mysql}"
DB="${MYSQL_DATABASE:-sz_ev}"
ROOT_PW="${MYSQL_ROOT_PASSWORD:-rootpass}"
LAKE="${SZ_EV_LAKE:-$ROOT/data/lake}"
[ -d "$LAKE" ] || LAKE=/workspace/sz-ev-charging/data/lake
BACKUP_ROOT="${BACKUP_ROOT:-/workspace/backups}"
KEEP="${KEEP:-3}"
DB_ONLY=0; [ "${1:-}" = "--db-only" ] && DB_ONLY=1

TS="$(date +%Y%m%dT%H%M%S)"
OUT="$BACKUP_ROOT/$TS"
mkdir -p "$OUT"
echo "[backup] -> $OUT"

# 1) MySQL：单事务一致性快照，带存储过程/触发器
docker exec "$CONTAINER" mysqldump -uroot -p"$ROOT_PW" --single-transaction --quick \
  --routines --triggers --set-gtid-purged=OFF --databases "$DB" 2>/dev/null | gzip -6 > "$OUT/mysql_${DB}.sql.gz"
gzip -t "$OUT/mysql_${DB}.sql.gz"
# 行数清单（精确 COUNT），恢复后用来核对
docker exec "$CONTAINER" mysql -uroot -p"$ROOT_PW" -N -e \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='$DB' AND table_type='BASE TABLE' ORDER BY table_name" 2>/dev/null |
while read -r t; do
  n=$(docker exec "$CONTAINER" mysql -uroot -p"$ROOT_PW" -N -e "SELECT COUNT(*) FROM \`$DB\`.\`$t\`" 2>/dev/null)
  echo -e "$t\t$n"
done > "$OUT/mysql_rowcounts.tsv"

# 2) 数据湖：保留全部层（ods/dwd/ads/_runs），跳过临时目录
if [ "$DB_ONLY" = 0 ] && [ -d "$LAKE" ]; then
  tar -C "$(dirname "$LAKE")" -czf "$OUT/lake.tar.gz" \
    --exclude='_tmp' --exclude='_staging' "$(basename "$LAKE")"
fi

( cd "$OUT" && sha256sum ./* > SHA256SUMS )
{ echo "created=$TS"; echo "db=$DB"; echo "lake=$LAKE"; echo "git=$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo na)"; } > "$OUT/MANIFEST"
du -sh "$OUT"/* | sed 's/^/[backup] /'

# 3) 轮转：只保留最近 KEEP 份
ls -1d "$BACKUP_ROOT"/2*/ 2>/dev/null | sort | head -n -"$KEEP" | while read -r old; do
  echo "[backup] 删除旧备份 $old"; rm -rf -- "$old"; done
echo "[backup] 完成 $OUT"
