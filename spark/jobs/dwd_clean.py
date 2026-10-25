#!/usr/bin/env python3
"""DWD：清洗打标，口径沿用 etl/clean_to_dwd.py，粒度改为「站 × 天」以支持按天增量。

- is_row_missing：busy 与 idle 同时为空
- is_stuck：当天非缺失 busy 有效点 ≥12 且只有一个取值
- is_outlier：按站×天对 busy 做 1.5×IQR（有效点 ≥8 且 IQR>0）
- occupancy = busy / total_piles（非缺失且 total_piles>0）
产出：dwd.station_5min_clean / dwd.zone_hour / dwd.quality_station_day（均按 dt 分区）
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.jobs.common import DEFAULT_DWD_LAKE, Perf, date_args, in_list, resolve_dates, spark_session  # noqa: E402

HIGH_MISSING_THR = 0.30


def main() -> None:
    p = date_args(__doc__)
    p.add_argument("--from-lake-dwd", action="store_true",
                   help="跳过清洗，直接读外部已清洗分区（默认 data/lake/dwd/charge_5min_clean）")
    p.add_argument("--dwd-input", default=DEFAULT_DWD_LAKE)
    a = p.parse_args()
    dates = resolve_dates(a)
    dl = in_list(dates)
    spark = spark_session("dwd_clean")
    perf = Perf("dwd_clean", dates)
    spark.sql("CREATE DATABASE IF NOT EXISTS dwd")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS dwd.station_5min_clean (
        station_id STRING, ts TIMESTAMP, busy DOUBLE, idle DOUBLE, total_piles INT, occupancy DOUBLE,
        s_price DOUBLE, e_price DOUBLE, duration DOUBLE, volume DOUBLE,
        is_row_missing TINYINT, is_stuck TINYINT, is_outlier TINYINT, quality_flag STRING)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS dwd.zone_hour (
        taz_id STRING, ts_hour TIMESTAMP, occupancy_avg DOUBLE, busy_sum DOUBLE, piles_sum BIGINT,
        station_cnt BIGINT, rated_kw_sum DOUBLE)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS dwd.quality_station_day (
        station_id STRING, taz_id STRING, rows BIGINT, missing_rows BIGINT, missing_rate DOUBLE,
        stuck_flag TINYINT, outlier_rows BIGINT, max_missing_run BIGINT, ticket_suggest STRING)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")

    t = time.time()
    if a.from_lake_dwd:
        load_external_dwd(spark, a.dwd_input, dates)
    else:
        clean(spark, dl)
    n = spark.sql(f"SELECT COUNT(*) FROM dwd.station_5min_clean WHERE dt IN ({dl})").first()[0]
    perf.step("station_5min_clean" + ("(external)" if a.from_lake_dwd else ""), n, t)
    aggregate(spark, perf, dates, dl)
    spark.stop()


DWD_COLS = {"station_id": "string", "ts": "timestamp", "busy": "double", "idle": "double",
            "total_piles": "int", "occupancy": "double", "s_price": "double", "e_price": "double",
            "duration": "double", "volume": "double", "is_row_missing": "tinyint",
            "is_stuck": "tinyint", "is_outlier": "tinyint", "quality_flag": "string"}


def load_external_dwd(spark, root: str, dates: list[str]) -> None:
    from pyspark.sql import functions as F
    r = Path(root).resolve()
    paths = [str(r / f"dt={d}") for d in dates if (r / f"dt={d}").is_dir()]
    if not paths:
        raise SystemExit(f"[dwd] {r} 下没有所需分区")
    raw = spark.read.option("basePath", str(r)).option("mergeSchema", "true").parquet(*paths)
    sel = [F.col(c).cast(t).alias(c) if c in raw.columns else F.lit(None).cast(t).alias(c)
           for c, t in DWD_COLS.items()]
    raw.select(*sel, F.col("dt").cast("string").alias("dt")).createOrReplaceTempView("v_ext")
    spark.sql("INSERT OVERWRITE TABLE dwd.station_5min_clean PARTITION (dt) SELECT * FROM v_ext")


def clean(spark, dl: str) -> None:
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW v_base AS
      SELECT o.station_id, o.ts, o.busy, o.idle, i.total_piles, o.s_price, o.e_price, o.duration, o.volume,
             CASE WHEN o.busy IS NULL AND o.idle IS NULL THEN 1 ELSE 0 END AS is_row_missing, o.dt
      FROM ods.charge_5min o LEFT JOIN ods.station_info i ON o.station_id = i.station_id
      WHERE o.dt IN ({dl})""")
    spark.sql("""
      CREATE OR REPLACE TEMP VIEW v_stat AS
      SELECT station_id, dt,
             SUM(CASE WHEN is_row_missing=0 AND busy IS NOT NULL THEN 1 ELSE 0 END) AS n_valid,
             COUNT(DISTINCT CASE WHEN is_row_missing=0 THEN busy END) AS n_distinct,
             percentile(CASE WHEN is_row_missing=0 THEN busy END, 0.25) AS q1,
             percentile(CASE WHEN is_row_missing=0 THEN busy END, 0.75) AS q3
      FROM v_base GROUP BY station_id, dt""")
    spark.sql("""
      INSERT OVERWRITE TABLE dwd.station_5min_clean PARTITION (dt)
      SELECT b.station_id, b.ts, b.busy, b.idle, b.total_piles,
             CASE WHEN b.is_row_missing=0 AND b.total_piles>0 AND b.busy IS NOT NULL
                  THEN b.busy / b.total_piles END AS occupancy,
             b.s_price, b.e_price, b.duration, b.volume,
             CAST(b.is_row_missing AS TINYINT),
             CAST(CASE WHEN s.n_valid>=12 AND s.n_distinct=1 THEN 1 ELSE 0 END AS TINYINT) AS is_stuck,
             CAST(CASE WHEN s.n_valid>=8 AND (s.q3-s.q1)>0 AND b.busy IS NOT NULL AND
                    (b.busy < s.q1-1.5*(s.q3-s.q1) OR b.busy > s.q3+1.5*(s.q3-s.q1)) THEN 1 ELSE 0 END AS TINYINT) AS is_outlier,
             CASE WHEN b.is_row_missing=1 THEN 'dropped_src' ELSE 'ok' END AS quality_flag,
             b.dt
      FROM v_base b JOIN v_stat s ON b.station_id=s.station_id AND b.dt=s.dt""")


def aggregate(spark, perf, dates, dl) -> None:
    t = time.time()
    spark.sql(f"""
      INSERT OVERWRITE TABLE dwd.zone_hour PARTITION (dt)
      SELECT i.taz_id, date_trunc('HOUR', c.ts) AS ts_hour, AVG(c.occupancy), SUM(c.busy),
             SUM(c.total_piles), COUNT(DISTINCT c.station_id), SUM(i.rated_power_kw), c.dt
      FROM dwd.station_5min_clean c JOIN ods.station_info i ON c.station_id=i.station_id
      WHERE c.dt IN ({dl}) AND c.is_row_missing=0
      GROUP BY i.taz_id, date_trunc('HOUR', c.ts), c.dt""")
    n = spark.sql(f"SELECT COUNT(*) FROM dwd.zone_hour WHERE dt IN ({dl})").first()[0]
    perf.step("zone_hour", n, t)

    t = time.time()
    # 最长连续缺失：gaps-and-islands
    spark.sql(f"""
      INSERT OVERWRITE TABLE dwd.quality_station_day PARTITION (dt)
      WITH r AS (
        SELECT station_id, dt, is_row_missing, is_stuck, is_outlier,
               ROW_NUMBER() OVER (PARTITION BY station_id, dt ORDER BY ts)
             - ROW_NUMBER() OVER (PARTITION BY station_id, dt, is_row_missing ORDER BY ts) AS grp
        FROM dwd.station_5min_clean WHERE dt IN ({dl})),
      runs AS (SELECT station_id, dt, MAX(n) AS max_run FROM (
                 SELECT station_id, dt, grp, COUNT(*) n FROM r WHERE is_row_missing=1 GROUP BY station_id, dt, grp) x
               GROUP BY station_id, dt),
      agg AS (SELECT station_id, dt, COUNT(*) rows, SUM(is_row_missing) missing_rows,
                     MAX(is_stuck) stuck_flag, SUM(is_outlier) outlier_rows
              FROM r GROUP BY station_id, dt)
      SELECT a.station_id, i.taz_id, a.rows, a.missing_rows, a.missing_rows / a.rows,
             CAST(a.stuck_flag AS TINYINT), a.outlier_rows, COALESCE(u.max_run, 0),
             CASE WHEN a.stuck_flag=1 THEN 'repair'
                  WHEN a.missing_rows / a.rows > {HIGH_MISSING_THR} THEN 'inspect' ELSE 'none' END,
             a.dt
      FROM agg a LEFT JOIN runs u ON a.station_id=u.station_id AND a.dt=u.dt
      LEFT JOIN ods.station_info i ON a.station_id=i.station_id""")
    n = spark.sql(f"SELECT COUNT(*) FROM dwd.quality_station_day WHERE dt IN ({dl})").first()[0]
    perf.step("quality_station_day", n, t)


if __name__ == "__main__":
    main()
