#!/usr/bin/env python3
"""ODS：按天 Parquet 分区 → Hive 表 ods.charge_5min（dt 分区）+ ods.station_info。

输入路径可配置（--input / 环境变量 SZ_ODS_INPUT，默认 data/full），
目录需为 <input>/dt=YYYY-MM-DD/*.parquet。INSERT OVERWRITE PARTITION 保证重跑幂等。
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from spark.jobs.common import DEFAULT_ODS_INPUT, STATION_INFO_DEMO, STATION_INFO_LAKE, STATION_INFO_URBANEV, Perf, date_args, resolve_dates, spark_session  # noqa: E402

COLS = {"station_id": "string", "ts": "timestamp", "busy": "double", "idle": "double",
        "s_price": "double", "e_price": "double", "duration": "double", "volume": "double"}


def load_station_info(spark, explicit: str | None) -> str:
    """统一成 station_id/taz_id/longitude/latitude/slow_count/fast_count/total_piles/rated_power_kw。

    UrbanEV 原表列：station_id,longitude,latitude,slow_count,fast_count,charge_count,TAZID（无额定功率，置空）。
    demo 表列：station_id,taz_id,lon_gcj,lat_gcj,...,total_piles,rated_power_kw。
    """
    cands = [Path(explicit)] if explicit else [STATION_INFO_LAKE, STATION_INFO_URBANEV, STATION_INFO_DEMO]
    for c in cands:
        if c.is_dir() and any(c.glob("*.csv")):
            path = str(c)
        elif c.is_file():
            path = str(c)
        else:
            continue
        df = spark.read.option("header", True).csv(path)
        cols = set(df.columns)
        def pick(*names):
            for n in names:
                if n in cols:
                    return f"`{n}`"
            return "NULL"
        df.createOrReplaceTempView("v_info_raw")
        spark.sql(f"""CREATE OR REPLACE TEMP VIEW v_info AS SELECT
            CAST(station_id AS STRING) station_id,
            CAST({pick('TAZID', 'taz_id')} AS STRING) taz_id,
            CAST({pick('longitude', 'lon_gcj', 'lon')} AS DOUBLE) longitude,
            CAST({pick('latitude', 'lat_gcj', 'lat')} AS DOUBLE) latitude,
            CAST({pick('slow_count')} AS INT) slow_count,
            CAST({pick('fast_count')} AS INT) fast_count,
            CAST({pick('charge_count', 'total_piles')} AS INT) total_piles,
            CAST({pick('rated_power_kw')} AS DOUBLE) rated_power_kw,
            '{c.name}' src
          FROM v_info_raw""")
        return path
    raise SystemExit("[ods] 找不到站点维表")


def main() -> None:
    p = date_args(__doc__)
    p.add_argument("--station-info", default=None,
                   help="站点维表 CSV；默认依次尝试 data/lake/ods/station_info/*.csv → UrbanEV station_information.csv → demo")
    p.add_argument("--input", default=DEFAULT_ODS_INPUT, help="分区根目录（含 dt=YYYY-MM-DD 子目录）")
    a = p.parse_args()
    dates = resolve_dates(a)
    root = Path(a.input).resolve()
    paths = [str(root / f"dt={d}") for d in dates if (root / f"dt={d}").is_dir()]
    missing = [d for d in dates if not (root / f"dt={d}").is_dir()]
    if missing:
        print(f"[ods] 跳过缺失分区 {len(missing)} 个: {missing[:5]}...")
    if not paths:
        raise SystemExit(f"[ods] {root} 下没有所需分区")

    spark = spark_session("ods_load")
    perf = Perf("ods_load", dates)
    spark.sql("CREATE DATABASE IF NOT EXISTS ods")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS ods.charge_5min (
        station_id STRING, ts TIMESTAMP, busy DOUBLE, idle DOUBLE,
        s_price DOUBLE, e_price DOUBLE, duration DOUBLE, volume DOUBLE,
        src_path STRING, load_at TIMESTAMP)
      PARTITIONED BY (dt STRING) STORED AS PARQUET""")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS ods.station_info (
        station_id STRING, taz_id STRING, longitude DOUBLE, latitude DOUBLE,
        slow_count INT, fast_count INT, total_piles INT, rated_power_kw DOUBLE, src STRING) STORED AS PARQUET""")

    t = time.time()
    src = load_station_info(spark, a.station_info)
    spark.sql("INSERT OVERWRITE TABLE ods.station_info SELECT * FROM v_info")
    perf.step("station_info", spark.table("ods.station_info").count(), t, source=src)

    t = time.time()
    from pyspark.sql import functions as F
    raw = spark.read.option("basePath", str(root)).option("mergeSchema", "true").parquet(*paths)
    sel = []
    for c, typ in COLS.items():
        sel.append(F.col(c).cast(typ).alias(c) if c in raw.columns else F.lit(None).cast(typ).alias(c))
    df = raw.select(*sel, F.input_file_name().alias("src_path"), F.current_timestamp().alias("load_at"),
                    F.col("dt").cast("string").alias("dt"))
    df.createOrReplaceTempView("v_raw")
    spark.sql("INSERT OVERWRITE TABLE ods.charge_5min PARTITION (dt) SELECT * FROM v_raw")
    n = spark.sql(f"SELECT COUNT(*) c FROM ods.charge_5min WHERE dt IN ({','.join(repr(d) for d in dates)})").first()[0]
    perf.step("charge_5min", n, t, input=str(root), partitions=len(paths))
    spark.stop()


if __name__ == "__main__":
    main()
