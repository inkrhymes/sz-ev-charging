#!/usr/bin/env python3
"""调度入口（DAG 任务 publish）：把 Hive ads.* 中指定日期的结果回写 MySQL。

表：ads_city_overview / ads_zone_occupancy_hour / ads_baseline_naive / ads_zone_daily / dim_station / ads_quality_summary
连接：MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB（与 etl/load_mysql_from_demo.py 一致）
幂等：按日期范围 DELETE + INSERT（同一事务）；不分区表 REPLACE INTO。--dry-run 只导出 CSV。
用法：export_ads_mysql.py --ds 2022-09-01 [--dry-run]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.jobs.common import Perf, date_args, in_list, resolve_dates, spark_session  # noqa: E402
from spark.jobs.ads_metrics import export  # noqa: E402


def main() -> None:
    p = date_args(__doc__)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    dates = resolve_dates(a)
    spark = spark_session("export_ads_mysql")
    export(spark, Perf("export_ads_mysql", dates), dates, in_list(dates), a.dry_run)
    spark.stop()


if __name__ == "__main__":
    main()
