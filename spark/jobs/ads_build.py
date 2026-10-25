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
from spark.jobs.ods_load import ensure_station_info_table, load_station_info  # noqa: E402


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
    ensure_station_info_table(spark)
    src = load_station_info(spark, a.station_info)
    spark.sql("INSERT OVERWRITE TABLE ods.station_info SELECT * FROM v_info")
    perf.step("station_info", spark.table("ods.station_info").count(), t, source=src)

    t = time.time()
    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    # 用 Spark 数据源表（带 LOCATION 即为外表，登记在 Hive Metastore）：
    # 上游 ts 是不带时区的 Parquet 时间戳，Hive SerDe 的 TIMESTAMP 会按 UTC 解释而偏移 8 小时，数据源表会推断为 TIMESTAMP_NTZ，不会偏移。
    old = [r for r in spark.sql("SHOW TABLES IN dwd LIKE 'ext_charge_5min_clean'").collect()]
    if old and spark.sql("DESCRIBE FORMATTED dwd.ext_charge_5min_clean").where("col_name='Provider' AND lower(data_type)='parquet'").count() == 0:
        spark.sql("DROP TABLE dwd.ext_charge_5min_clean")  # 旧版 Hive SerDe 外表；EXTERNAL 删除不动数据
    spark.sql(f"""CREATE TABLE IF NOT EXISTS dwd.ext_charge_5min_clean
      USING parquet LOCATION '{dwd_root}'""")
    spark.sql("MSCK REPAIR TABLE dwd.ext_charge_5min_clean")  # 注册湖内全部已有分区
    spark.sql("""CREATE OR REPLACE TEMP VIEW v_src AS
      SELECT CAST(station_id AS STRING) station_id, CAST(ts AS TIMESTAMP) ts, busy, idle, total_piles, occupancy,
             s_price, e_price, duration, volume, is_row_missing, is_stuck, is_outlier, quality_flag, dt
      FROM dwd.ext_charge_5min_clean""")
    n = spark.sql(f"SELECT COUNT(*) FROM v_src WHERE dt IN ({dl})").first()[0]
    perf.step("register_ext_dwd", n, t,
              partitions_total=spark.sql("SHOW PARTITIONS dwd.ext_charge_5min_clean").count())

    ensure_dwd_tables(spark)
    aggregate(spark, perf, dates, dl, src="v_src")
    parts = [r[0].split("=", 1)[1] for r in spark.sql("SHOW PARTITIONS dwd.ext_charge_5min_clean").collect()]
    build(spark, perf, dates, dl, src="v_src", src_parts=parts)

    out = Path(a.ads_output)
    t = time.time()
    zh = spark.sql(f"""
      SELECT z.taz_id, CAST(z.ts_hour AS TIMESTAMP_NTZ) ts_hour, z.occupancy_avg, z.busy_sum, z.piles_sum, z.station_cnt,
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
