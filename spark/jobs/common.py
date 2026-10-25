"""公共：SparkSession、日期参数、计时与行数记录。"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPARK_DIR = ROOT / "spark"
WAREHOUSE = Path(os.environ.get("SZ_WAREHOUSE", ROOT / "data" / "warehouse"))
METASTORE = Path(os.environ.get("SZ_METASTORE", SPARK_DIR / "_metastore"))
DEFAULT_ODS_INPUT = os.environ.get("SZ_ODS_INPUT", str(ROOT / "data" / "lake" / "ods" / "charge_5min"))
STATION_INFO_LAKE = ROOT / "data" / "lake" / "ods" / "station_info"
STATION_INFO_URBANEV = Path(os.environ.get(
    "SZ_STATION_INFO_RAW",
    "/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/20220901-20230228_station-raw/station_information.csv"))
STATION_INFO_DEMO = ROOT / "data" / "demo" / "ods" / "ods_station_info.csv.gz"
PERF_LOG = SPARK_DIR / "_runs" / "perf.jsonl"


def date_args(desc: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=desc)
    p.add_argument("--dt", "--ds", dest="dt", help="单日增量，YYYY-MM-DD（--ds 为调度约定别名）")
    p.add_argument("--start", help="补跑起始日（含）")
    p.add_argument("--end", help="补跑结束日（含）")
    return p


def resolve_dates(a) -> list[str]:
    if a.dt:
        return [a.dt]
    if a.start and a.end:
        s, e = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
        if e < s:
            raise SystemExit("--end 早于 --start")
        return [(s + dt.timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
    raise SystemExit("需要 --dt 或 --start/--end")


def spark_session(app: str):
    os.environ.setdefault("SPARK_CONF_DIR", str(SPARK_DIR / "conf"))
    from pyspark.sql import SparkSession

    METASTORE.mkdir(parents=True, exist_ok=True)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    b = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SZ_SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SZ_DRIVER_MEM", "2g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE))
        .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={METASTORE / 'metastore_db'};create=true")
        .config("spark.hadoop.hive.exec.dynamic.partition", "true")
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={METASTORE}")
        .enableHiveSupport()
    )
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


class Perf:
    """记录每一步耗时与行数，写入 spark/_runs/perf.jsonl。"""

    def __init__(self, job: str, dates: list[str]):
        self.job, self.dates, self.t0 = job, dates, time.time()

    def step(self, name: str, rows: int, t_start: float, **extra):
        rec = {
            "job": self.job, "step": name, "rows": int(rows),
            "seconds": round(time.time() - t_start, 2),
            "dates": f"{self.dates[0]}..{self.dates[-1]}", "n_days": len(self.dates),
            "at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **extra,
        }
        PERF_LOG.parent.mkdir(parents=True, exist_ok=True)
        with PERF_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print(f"[{self.job}] {name}: rows={rows} {rec['seconds']}s")


def in_list(dates: list[str]) -> str:
    return ",".join(f"'{d}'" for d in dates)
DEFAULT_DWD_LAKE = os.environ.get("SZ_DWD_LAKE", str(ROOT / "data" / "lake" / "dwd" / "charge_5min_clean"))
