#!/usr/bin/env bash
# 恢复演练：把最近一份备份恢复到临时库 sz_ev_restore_test，逐表比对精确行数，然后删掉临时库。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${MYSQL_CONTAINER:-sz-ev-mysql}"; ROOT_PW="${MYSQL_ROOT_PASSWORD:-rootpass}"
BACKUP_ROOT="${BACKUP_ROOT:-/workspace/backups}"; SCRATCH=sz_ev_restore_test
SRC="${1:-$(ls -1d "$BACKUP_ROOT"/2*/ | sort | tail -1)}"; SRC="${SRC%/}"
bash "$ROOT/scripts/restore.sh" "$SRC" --target-db "$SCRATCH"
fail=0
while IFS=$'\t' read -r t n; do
  m=$(docker exec "$CONTAINER" mysql -uroot -p"$ROOT_PW" -N -e "SELECT COUNT(*) FROM \`$SCRATCH\`.\`$t\`" 2>/dev/null || echo missing)
  [ "$n" = "$m" ] && s=OK || { s=DIFF; fail=1; }
  printf '%-28s %10s %10s %s\n' "$t" "$n" "$m" "$s"
done < "$SRC/mysql_rowcounts.tsv"
docker exec "$CONTAINER" mysql -uroot -p"$ROOT_PW" -e "DROP DATABASE \`$SCRATCH\`" 2>/dev/null
tar -tzf "$SRC/lake.tar.gz" >/dev/null 2>&1 && echo "lake.tar.gz 可读，文件数 $(tar -tzf "$SRC/lake.tar.gz" | grep -vc '/$')" || { echo "lake.tar.gz 损坏"; fail=1; }
[ "$fail" = 0 ] && echo "[verify] 全部一致" || { echo "[verify] 有差异"; exit 1; }
