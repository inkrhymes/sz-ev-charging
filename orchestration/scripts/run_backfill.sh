#!/usr/bin/env bash
# 补跑闭区间 [START, END]。优先用 Airflow 容器；容器没起时退回 run_day.py。
# 用法: orchestration/scripts/run_backfill.sh 2022-09-01 2022-09-07
set -euo pipefail
START=${1:?start ds}; END=${2:?end ds}
cd "$(dirname "$0")/../.."
if docker ps --format '{{.Names}}' | grep -qx sz-ev-airflow; then
  docker exec sz-ev-airflow airflow dags backfill -s "$START" -e "$END" --reset-dagruns -y sz_ev_daily
else
  echo "[backfill] Airflow 未运行，使用本地 run_day.py"
  python3 orchestration/run_day.py --start "$START" --end "$END"
fi
