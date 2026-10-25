#!/usr/bin/env bash
# 串行跑 ods → dwd → ads。用法：spark/run_range.sh --dt 2022-09-01 [--dry-run]
#                         spark/run_range.sh --start 2022-09-01 --end 2022-09-14 [--dry-run] [--from-lake-dwd]
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${SZ_PY:-/workspace/sz-venv-spark/bin/python}"
export JAVA_HOME="${JAVA_HOME:-/workspace/jdk}"; export PATH="$JAVA_HOME/bin:$PATH"
DATES=(); ADS=(); DWD=()
while [[ $# -gt 0 ]]; do case "$1" in
  --dt|--ds|--start|--end) DATES+=("$1" "$2"); shift 2;;
  --dry-run) ADS+=("$1"); shift;;
  --from-lake-dwd) DWD+=("$1"); shift;;
  *) echo "未知参数 $1"; exit 2;; esac; done
[[ ${#DWD[@]} -eq 0 ]] && "$PY" spark/jobs/ods_load.py "${DATES[@]}"
"$PY" spark/jobs/dwd_clean.py "${DATES[@]}" "${DWD[@]}"
"$PY" spark/jobs/ads_metrics.py "${DATES[@]}" "${ADS[@]}"
