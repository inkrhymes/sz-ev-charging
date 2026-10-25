"""调度任务的薄包装：每个任务只负责“按 ds 调用对应负责人的脚本”+ 质检闸门。

真实步骤（存在时优先调用）:
  ods_ingest / dwd_build : python etl/full_pipeline.py --start ds --end ds --layer ods|dwd   （小紫）
  ads_build              : python spark/jobs/ads_build.py --ds ds                           （小黑，占位）
  publish                : python spark/jobs/export_ads_mysql.py --ds ds                    （小黑，占位）
脚本不存在时退回演示实现：从 data/demo 的 CSV 按天切出分区，写到 orchestration/_work/lake/。
Airflow DAG 与 run_day.py 共用本模块，保证两条路径行为一致。
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from quality import engine  # noqa: E402

DEMO = ROOT / "data" / "demo"
REAL_LAKE = ROOT / "data" / "lake"
DEMO_LAKE = Path(os.getenv("DEMO_LAKE", ROOT / "orchestration" / "_work" / "lake"))

FULL_PIPELINE = ROOT / "etl" / "full_pipeline.py"
SPARK_ADS = ROOT / "spark" / "jobs" / "ads_build.py"
SPARK_EXPORT = ROOT / "spark" / "jobs" / "export_ads_mysql.py"

PART = {
    "ods": "ods/charge_5min",
    "dwd": "dwd/charge_5min_clean",
    "ads": "ads/zone_hour",
}


def _use_real(layer: str) -> bool:
    if os.getenv("ORCH_FORCE_DEMO") == "1":
        return False
    return {"ods": FULL_PIPELINE, "dwd": FULL_PIPELINE, "ads": SPARK_ADS}[layer].exists()


def part_dir(layer: str, ds: str) -> Path:
    lake = REAL_LAKE if _use_real(layer) else DEMO_LAKE
    return lake / PART[layer] / f"dt={ds}"


def _run(cmd: list[str]) -> None:
    print("[run]", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def _write_part(df: pd.DataFrame, layer: str, ds: str) -> Path:
    d = part_dir(layer, ds)
    d.mkdir(parents=True, exist_ok=True)
    for f in d.glob("*"):  # 覆盖写分区，保证幂等
        f.unlink()
    df.to_parquet(d / "part-0.parquet", index=False)
    return d


def _slice_demo(csv: Path, ds: str, ts_col: str = "ts") -> pd.DataFrame:
    df = pd.read_csv(csv)
    return df[df[ts_col].astype(str).str.startswith(ds)].reset_index(drop=True)


# ---------- 构建任务 ----------
def ods_ingest(ds: str) -> None:
    if _use_real("ods"):
        return _run([sys.executable, str(FULL_PIPELINE), "--start", ds, "--end", ds, "--layer", "ods"])
    src = Path(os.getenv("DEMO_ODS_CSV", DEMO / "ods" / "ods_station_raw_5min.csv"))
    _write_part(_slice_demo(src, ds), "ods", ds)


def dwd_build(ds: str) -> None:
    if _use_real("dwd"):
        return _run([sys.executable, str(FULL_PIPELINE), "--start", ds, "--end", ds, "--layer", "dwd"])
    # 演示：dwd 明细由现有 etl/clean_to_dwd.py 产出（全窗口），这里只按 ds 切分区
    _write_part(_slice_demo(DEMO / "dwd" / "dwd_station_5min_clean.csv", ds), "dwd", ds)


def ads_build(ds: str) -> None:
    if _use_real("ads"):
        return _run([sys.executable, str(SPARK_ADS), "--ds", ds])
    # 演示：片区小时汇总（口径同 dwd_zone_hour）
    dwd = engine.read_frame(part_dir("dwd", ds))
    info = pd.read_csv(DEMO / "dwd" / "dwd_quality_station.csv")[["station_id", "taz_id"]]
    d = dwd if "taz_id" in dwd.columns else dwd.merge(info, on="station_id", how="left")
    d["ts_hour"] = pd.to_datetime(d["ts"]).dt.floor("h").astype(str)
    g = d.groupby(["taz_id", "ts_hour"]).agg(
        busy_sum=("busy", "sum"), piles_sum=("total_piles", "sum"),
        occupancy_avg=("occupancy", "mean"), station_cnt=("station_id", "nunique")).reset_index()
    g["occupancy_avg"] = g["occupancy_avg"].round(4)
    _write_part(g, "ads", ds)


def publish(ds: str) -> None:
    if SPARK_EXPORT.exists() and os.getenv("ORCH_FORCE_DEMO") != "1":
        return _run([sys.executable, str(SPARK_EXPORT), "--ds", ds])
    n = len(engine.read_frame(part_dir("ads", ds)))
    print(f"[publish] demo: ads {ds} {n} 行已就绪（写回 MySQL 由小黑的作业接管）")


# ---------- 质检闸门 ----------
def _prev_ds(ds: str) -> str:
    return (pd.Timestamp(ds) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def dq(layer: str, ds: str, write_db: bool = True, path: Path | None = None) -> list:
    path = path or part_dir(layer, ds)
    df = engine.read_frame(path)
    prev_df = engine.read_frame(part_dir(layer, _prev_ds(ds)))
    return engine.check(layer, ds, df, prev_df if not prev_df.empty else None, write_db=write_db)


CHAIN = [
    ("ods_ingest", ods_ingest),
    ("dq_ods", lambda ds, **kw: dq("ods", ds, **kw)),
    ("dwd_build", dwd_build),
    ("dq_dwd", lambda ds, **kw: dq("dwd", ds, **kw)),
    ("ads_build", ads_build),
    ("dq_ads", lambda ds, **kw: dq("ads", ds, **kw)),
    ("publish", publish),
]


def run_chain(ds: str, write_db: bool = True) -> list[tuple[str, str, float]]:
    """顺序执行整条链路；任何任务异常（含 DQBlockError）立即停止，下游标记 skipped。"""
    timings, failed = [], False
    for name, fn in CHAIN:
        if failed:
            timings.append((name, "skipped", 0.0))
            continue
        t0 = time.perf_counter()
        try:
            fn(ds, write_db=write_db) if name.startswith("dq_") else fn(ds)
            timings.append((name, "success", time.perf_counter() - t0))
        except Exception as e:
            print(f"[fail] {name}: {e}", flush=True)
            timings.append((name, "failed", time.perf_counter() - t0))
            failed = True
    return timings
