#!/usr/bin/env python3
"""ADS：由 DWD 计算指标 → Hive ads.*（dt 分区），再回写 MySQL ads_*（结构同 sql/ads/03_ads_tables.sql）。

- ads.city_overview          ↔ ads_city_overview
- ads.zone_occupancy_hour    ↔ ads_zone_occupancy_hour（load_kw_est = occupancy_avg × 片区额定功率之和）
- ads.baseline_naive         ↔ ads_baseline_naive（lag1 跨天：读取前一日最后一小时）
- ads.quality_summary        ↔ ads_quality_summary（对全部已有 dwd 分区汇总）
回写：MySQL 端按日期范围先 DELETE 再 INSERT（同一事务），重跑幂等；--dry-run 时只导出 CSV。
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.jobs.common import ROOT, Perf, date_args, in_list, resolve_dates, spark_session  # noqa: E402

HIGH_MISSING_THR = 0.30
EXPORT = ROOT / "data" / "lake" / "export" / "ads"


def build(spark, perf, dates, dl):
    spark.sql("CREATE DATABASE IF NOT EXISTS ads")
    spark.sql("""CREATE TABLE IF NOT EXISTS ads.city_overview (station_cnt BIGINT, zone_cnt BIGINT,
      avg_occupancy DOUBLE, peak_hour INT, stuck_cnt BIGINT, high_missing_cnt BIGINT)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""CREATE TABLE IF NOT EXISTS ads.zone_occupancy_hour (taz_id STRING, ts_hour TIMESTAMP,
      occupancy DOUBLE, load_kw_est DOUBLE) PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""CREATE TABLE IF NOT EXISTS ads.baseline_naive (taz_id STRING, ts_hour TIMESTAMP,
      y_true DOUBLE, y_hat_lag1 DOUBLE, abs_err DOUBLE) PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""CREATE TABLE IF NOT EXISTS ads.quality_summary (metric STRING, value DOUBLE, note STRING)
      STORED AS PARQUET""")

    t = time.time()
    spark.sql(f"""INSERT OVERWRITE TABLE ads.zone_occupancy_hour PARTITION (dt)
      SELECT taz_id, ts_hour, occupancy_avg, occupancy_avg * rated_kw_sum, dt
      FROM dwd.zone_hour WHERE dt IN ({dl})""")
    perf.step("zone_occupancy_hour",
              spark.sql(f"SELECT COUNT(*) FROM ads.zone_occupancy_hour WHERE dt IN ({dl})").first()[0], t)

    t = time.time()
    prev = (dt.date.fromisoformat(dates[0]) - dt.timedelta(days=1)).isoformat()
    spark.sql(f"""INSERT OVERWRITE TABLE ads.baseline_naive PARTITION (dt)
      SELECT taz_id, ts_hour, y_true, y_hat, ABS(y_true - y_hat), dt FROM (
        SELECT taz_id, ts_hour, occupancy AS y_true, dt,
               CASE WHEN LAG(ts_hour) OVER w = ts_hour - INTERVAL 1 HOUR THEN LAG(occupancy) OVER w END AS y_hat
        FROM ads.zone_occupancy_hour WHERE dt IN ({dl}, '{prev}')
        WINDOW w AS (PARTITION BY taz_id ORDER BY ts_hour)) x
      WHERE dt IN ({dl})""")
    perf.step("baseline_naive",
              spark.sql(f"SELECT COUNT(*) FROM ads.baseline_naive WHERE dt IN ({dl})").first()[0], t)

    t = time.time()
    spark.sql(f"""INSERT OVERWRITE TABLE ads.city_overview PARTITION (dt)
      WITH d AS (SELECT dt, COUNT(DISTINCT taz_id) zone_cnt, AVG(occupancy_avg) avg_occ
                 FROM dwd.zone_hour WHERE dt IN ({dl}) GROUP BY dt),
      p AS (SELECT dt, hr FROM (SELECT dt, HOUR(ts_hour) hr,
              ROW_NUMBER() OVER (PARTITION BY dt ORDER BY AVG(occupancy_avg) DESC) rn
              FROM dwd.zone_hour WHERE dt IN ({dl}) GROUP BY dt, HOUR(ts_hour)) x WHERE rn=1),
      q AS (SELECT dt, COUNT(*) station_cnt, SUM(stuck_flag) stuck_cnt,
                   SUM(CASE WHEN missing_rate > {HIGH_MISSING_THR} THEN 1 ELSE 0 END) hm
            FROM dwd.quality_station_day WHERE dt IN ({dl}) GROUP BY dt)
      SELECT q.station_cnt, d.zone_cnt, d.avg_occ, p.hr, q.stuck_cnt, q.hm, q.dt
      FROM q LEFT JOIN d ON q.dt=d.dt LEFT JOIN p ON q.dt=p.dt""")
    perf.step("city_overview",
              spark.sql(f"SELECT COUNT(*) FROM ads.city_overview WHERE dt IN ({dl})").first()[0], t)

    t = time.time()
    spark.sql("""CREATE TABLE IF NOT EXISTS ads.zone_daily (taz_id STRING, avg_occupancy DOUBLE,
      station_cnt BIGINT, pile_cnt BIGINT, center_lon DOUBLE, center_lat DOUBLE)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql(f"""INSERT OVERWRITE TABLE ads.zone_daily PARTITION (dt)
      WITH occ AS (SELECT dt, taz_id, AVG(occupancy_avg) avg_occ FROM dwd.zone_hour
                   WHERE dt IN ({dl}) GROUP BY dt, taz_id),
      st AS (SELECT q.dt, q.taz_id, COUNT(*) station_cnt, SUM(i.total_piles) pile_cnt
             FROM dwd.quality_station_day q JOIN ods.station_info i ON q.station_id=i.station_id
             WHERE q.dt IN ({dl}) GROUP BY q.dt, q.taz_id),
      ctr AS (SELECT taz_id, AVG(longitude) lon, AVG(latitude) lat FROM ods.station_info GROUP BY taz_id)
      SELECT st.taz_id, occ.avg_occ, st.station_cnt, st.pile_cnt, ctr.lon, ctr.lat, st.dt
      FROM st LEFT JOIN occ ON st.dt=occ.dt AND st.taz_id=occ.taz_id
      LEFT JOIN ctr ON st.taz_id=ctr.taz_id""")
    perf.step("zone_daily", spark.sql(f"SELECT COUNT(*) FROM ads.zone_daily WHERE dt IN ({dl})").first()[0], t)

    t = time.time()
    spark.sql(f"""INSERT OVERWRITE TABLE ads.quality_summary
      WITH s AS (SELECT station_id, SUM(rows) r, SUM(missing_rows) m, MAX(stuck_flag) st
                 FROM dwd.quality_station_day GROUP BY station_id)
      SELECT 'station_cnt', CAST(COUNT(*) AS DOUBLE), '已入仓站点数' FROM s
      UNION ALL SELECT 'row_missing_rate', SUM(m)/SUM(r), '整行缺数比例（按行加权，全部已入仓分区）' FROM s
      UNION ALL SELECT 'stuck_station_cnt', CAST(SUM(st) AS DOUBLE), '至少一天读数不动的站' FROM s
      UNION ALL SELECT 'high_missing_station_cnt', CAST(SUM(CASE WHEN m/r > {HIGH_MISSING_THR} THEN 1 ELSE 0 END) AS DOUBLE),
                       '缺数比例>30% 的站' FROM s""")
    perf.step("quality_summary", spark.table("ads.quality_summary").count(), t)


EXPORTS = {
    "ads_city_overview": ("SELECT CAST(dt AS DATE) dt, station_cnt, zone_cnt, ROUND(avg_occupancy,6) avg_occupancy, "
                          "peak_hour, stuck_cnt, high_missing_cnt FROM ads.city_overview WHERE dt IN ({dl})", "dt"),
    "ads_zone_occupancy_hour": ("SELECT taz_id, ts_hour, ROUND(occupancy,6) occupancy, ROUND(load_kw_est,2) load_kw_est "
                                "FROM ads.zone_occupancy_hour WHERE dt IN ({dl})", "DATE(ts_hour)"),
    "ads_baseline_naive": ("SELECT taz_id, ts_hour, ROUND(y_true,6) y_true, ROUND(y_hat_lag1,6) y_hat_lag1, "
                           "ROUND(abs_err,6) abs_err FROM ads.baseline_naive WHERE dt IN ({dl})", "DATE(ts_hour)"),
    "ads_zone_daily": ("SELECT CAST(dt AS DATE) dt, taz_id, ROUND(avg_occupancy,6) avg_occupancy, station_cnt, pile_cnt, "
                       "ROUND(center_lon,6) center_lon, ROUND(center_lat,6) center_lat FROM ads.zone_daily WHERE dt IN ({dl})", "dt"),
    "dim_station": ("SELECT station_id, taz_id, ROUND(longitude,6) longitude, ROUND(latitude,6) latitude, slow_count, "
                    "fast_count, total_piles, rated_power_kw FROM ods.station_info", None),
    "ads_quality_summary": ("SELECT metric, ROUND(value,6) value, note FROM ads.quality_summary", None),
}


def export(spark, perf, dates, dl, dry_run: bool):
    conn = None
    if not dry_run:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DB", os.getenv("MYSQL_DATABASE", "sz_ev")),
                               charset="utf8mb4", autocommit=False)
    for table, (sql, date_expr) in EXPORTS.items():
        t = time.time()
        sdf = spark.sql(sql.format(dl=dl))
        cols = sdf.columns
        rows = [tuple(r) for r in sdf.collect()]  # ads 结果量小；避免 toPandas（py3.13 下 pyspark 3.5 依赖 distutils）
        if dry_run:
            EXPORT.mkdir(parents=True, exist_ok=True)
            out = EXPORT / f"{table}_{dates[0]}_{dates[-1]}.csv"
            with out.open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f); w.writerow(cols); w.writerows(rows)
            perf.step(f"export:{table}(dry-run)", len(rows), t, file=str(out.relative_to(ROOT)))
            continue
        with conn.cursor() as cur:
            if date_expr:
                cur.execute(f"DELETE FROM {table} WHERE {date_expr} BETWEEN %s AND %s", (dates[0], dates[-1]))
                verb = "INSERT"
            else:
                verb = "REPLACE"
            ph = ",".join(["%s"] * len(cols))
            for i in range(0, len(rows), 5000):
                cur.executemany(f"{verb} INTO {table} ({','.join(cols)}) VALUES ({ph})", rows[i:i + 5000])
        conn.commit()
        perf.step(f"mysql:{table}", len(rows), t)
    if conn:
        conn.close()


def main() -> None:
    p = date_args(__doc__)
    p.add_argument("--dry-run", action="store_true", help="不连 MySQL，导出 CSV 到 data/lake/export/ads/")
    p.add_argument("--skip-export", action="store_true", help="只算 Hive ads 表")
    a = p.parse_args()
    dates = resolve_dates(a)
    dl = in_list(dates)
    spark = spark_session("ads_metrics")
    perf = Perf("ads_metrics", dates)
    build(spark, perf, dates, dl)
    if not a.skip_export:
        export(spark, perf, dates, dl, a.dry_run)
    spark.stop()


if __name__ == "__main__":
    main()
