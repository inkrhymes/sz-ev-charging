#!/usr/bin/env bash
# 一键启动：MySQL -> 建表 -> 装载 demo 数据 -> Flask API + 大屏
set -euo pipefail
cd "$(dirname "$0")/.."
: "${MYSQL_ROOT_PASSWORD:=rootpass}" "${MYSQL_DATABASE:=sz_ev}" "${MYSQL_USER:=ev}" "${MYSQL_PASSWORD:=evpass}" "${MYSQL_PORT:=3306}"
export MYSQL_ROOT_PASSWORD MYSQL_DATABASE MYSQL_USER MYSQL_PASSWORD MYSQL_PORT

echo "[1/5] 启动 MySQL"
docker compose up -d mysql
until [ "$(docker inspect -f '{{.State.Health.Status}}' sz-ev-mysql)" = healthy ]; do sleep 2; done

echo "[2/5] 建表与指标视图（ods -> dwd -> ads）"
for f in sql/ods/*.sql sql/dwd/*.sql sql/ads/*.sql; do
  echo "  $f"
  docker exec -i sz-ev-mysql mysql -uroot -p"$MYSQL_ROOT_PASSWORD" --default-character-set=utf8mb4 "$MYSQL_DATABASE" < "$f"
done

echo "[3/5] 准备 demo CSV（缺失时由 ETL 重新生成）"
if [ ! -f data/demo/dwd/dwd_station_5min_clean.csv ]; then
  .venv/bin/python etl/load_ods.py && .venv/bin/python etl/clean_to_dwd.py
fi

echo "[4/5] 装载 CSV 到 MySQL 并回填 ads"
MYSQL_USER=root MYSQL_PASSWORD="$MYSQL_ROOT_PASSWORD" .venv/bin/python etl/load_mysql_from_demo.py

echo "[5/5] 启动 API 与大屏"
docker compose up -d --build api
echo "完成：http://localhost:${API_PORT:-5000}"
