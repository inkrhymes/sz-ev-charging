#!/usr/bin/env python3
"""调度入口（DAG 任务 ads_build）：直接读取上游清洗分区，生成 ADS。

输入：$SZ_EV_LAKE/dwd/charge_5min_clean/dt=D/（注册为 Hive 外表 dwd.ext_charge_5min_clean，不重做清洗）
      $SZ_EV_LAKE/ods/station_info/（站点维表，装入 ods.station_info）
输出：Hive dwd.zone_hour / dwd.quality_station_day / ads.*（dt 分区，INSERT OVERWRITE 幂等）
      $SZ_EV_LAKE/ads/zone_hour/dt=D/   列：taz_id, ts_hour, occupancy_avg, busy_sum, piles_sum, station_cnt, center_lon, center_lat
      $SZ_EV_LAKE/ads/zone_daily/dt=D/  列同 MySQL ads_zone_daily（不含 dt，dt 在目录名）
用法：ads_build.py --ds 2022-09-01 | --start D1 --end D2
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.jobs.common import DEFAULT_DWD_LAKE, LAKE, Perf, date_args, in_list, resolve_dates, spark_session  # noqa: E402
from spark.jobs.dwd_clean import aggregate, ensure_dwd_tables  # noqa: E402
from spark.jobs.ads_metrics import build  # noqa: E402
from spark.jobs.ods_load import load_station_info  # noqa: E402


def main() -> None:
    p = date_args(__doc__)
    p.add_argument("--dwd-input", default=DEFAULT_DWD_LAKE)
    p.add_argument("--ads-output", default=str(LAKE / "ads"))
    p.add_argument("--station-info", default=None)
    a = p.parse_args()
    dates = resolve_dates(a)
    dwd_root = Path(a.dwd_input).resolve()
    have = [d for d in dates if (dwd_root / f"dt={d}").is_dir()]
    if not have:
        raise SystemExit(f"[ads_build] {dwd_root} 下没有所需分区 {dates[0]}..{dates[-1]}")
    if len(have) < len(dates):
        print(f"[ads_build] 跳过缺失分区 {sorted(set(dates) - set(have))[:5]}")
    dates, dl = have, in_list(have)

    spark = spark_session("ads_build")
    perf = Perf("ads_build", dates)

    t = time.time()
    spark.sql("CREATE DATABASE IF NOT EXISTS ods")
    spark.sql("""CREATE TABLE IF NOT EXISTS ods.station_info (
        station_id STRING, taz_id STRING, longitude DOUBLE, latitude DOUBLE,
        slow_count INT, fast_count INT, total_piles INT, rated_power_kw DOUBLE, src STRING) STORED AS PARQUET""")
    src = load_station_info(spark, a.station_info)
    spark.sql("INSERT OVERWRITE TABLE ods.station_info SELECT * FROM v_info")
    perf.step("station_info", spark.table("ods.station_info").count(), t, source=src)

    t = time.time()
    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS dwd.ext_charge_5min_clean (
        station_id INT, taz_id INT, ts TIMESTAMP, busy DOUBLE, idle DOUBLE, total_piles INT,
        s_price DOUBLE, e_price DOUBLE, duration DOUBLE, volume DOUBLE,
        is_row_missing TINYINT, is_stuck TINYINT, is_outlier TINYINT, occupancy DOUBLE, quality_flag STRING)
      PARTITIONED BY (dt STRING) STORED AS PARQUET LOCATION '{dwd_root}'""")
    spark.sql("MSCK REPAIR TABLE dwd.ext_charge_5min_clean")  # 注册湖内全部已有分区
    spark.sql("""CREATE OR REPLACE TEMP VIEW v_src AS
      SELECT CAST(station_id AS STRING) station_id, ts, busy, idle, total_piles, occupancy,
             s_price, e_price, duration, volume, is_row_missing, is_stuck, is_outlier, quality_flag, dt
      FROM dwd.ext_charge_5min_clean""")
    n = spark.sql(f"SELECT COUNT(*) FROM v_src WHERE dt IN ({dl})").first()[0]
    perf.step("register_ext_dwd", n, t,
              partitions_total=spark.sql("SHOW PARTITIONS dwd.ext_charge_5min_clean").count())

    ensure_dwd_tables(spark)
    aggregate(spark, perf, dates, dl, src="v_src")
    build(spark, perf, dates, dl, src="v_src")

    out = Path(a.ads_output)
    t = time.time()
    zh = spark.sql(f"""
      SELECT z.taz_id, z.ts_hour, z.occupancy_avg, z.busy_sum, z.piles_sum, z.station_cnt,
             c.lon AS center_lon, c.lat AS center_lat, z.dt
      FROM dwd.zone_hour z LEFT JOIN (SELECT taz_id, AVG(longitude) lon, AVG(latitude) lat
                                      FROM ods.station_info GROUP BY taz_id) c ON z.taz_id=c.taz_id
      WHERE z.dt IN ({dl})""")
    zh.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(str(out / "zone_hour"))
    perf.step("lake:ads/zone_hour", zh.count(), t, path=str(out / "zone_hour"))
    t = time.time()
    zd = spark.sql(f"SELECT taz_id, avg_occupancy, station_cnt, pile_cnt, center_lon, center_lat, dt "
                   f"FROM ads.zone_daily WHERE dt IN ({dl})")
    zd.repartition("dt").write.mode("overwrite").partitionBy("dt").parquet(str(out / "zone_daily"))
    perf.step("lake:ads/zone_daily", zd.count(), t, path=str(out / "zone_daily"))
    spark.stop()


if __name__ == "__main__":
    main()
