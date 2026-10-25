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
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from quality import engine  # noqa: E402
from orchestration import run_log  # noqa: E402

DEMO = ROOT / "data" / "demo"
REAL_LAKE = Path(os.getenv("SZ_EV_LAKE", ROOT / "data" / "lake"))
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


# ---------- publish 之后的扩展任务 ----------
# 子进程退出码约定：0 成功；2 输入不足 → 跳过（不算失败）；3 可重试（如写 MySQL 失败）；其他 → 失败
WEATHER_JOIN = ROOT / "spark" / "jobs" / "weather_join.py"
SITE_SUGGEST = ROOT / "spark" / "jobs" / "site_suggest.py"


class TaskSkipped(Exception):
    """输入不足等情况，本步不执行；Airflow 里转成 AirflowSkipException。"""


class TaskRetryable(Exception):
    """可重试的失败（退出码 3），Airflow 按 retries 重试。"""


def _child_env() -> dict:
    env = dict(os.environ)
    env.setdefault("SZ_EV_LAKE", str(REAL_LAKE))  # MYSQL_* 原样透传
    return env


def _run_rc(cmd: list[str], name: str) -> None:
    print("[run]", " ".join(cmd), flush=True)
    rc = subprocess.run(cmd, cwd=ROOT, env=_child_env()).returncode
    if rc == 0:
        return
    if rc == 2:
        raise TaskSkipped(f"{name}：输入不足（退出码 2），本步跳过")
    if rc == 3:
        raise TaskRetryable(f"{name}：可重试错误（退出码 3），如写 MySQL 失败")
    raise RuntimeError(f"{name}：退出码 {rc}")


def forecast_daily(ds: str) -> None:
    _run_rc([sys.executable, "-m", "forecast.daily", "--ds", ds], "forecast_daily")


def forecast_evaluate(ds: str) -> None:
    _run_rc([sys.executable, "-m", "forecast.evaluate", "--ds", ds], "forecast_evaluate")


def _optional_script(path: Path, name: str):
    def fn(ds: str) -> None:
        if not path.exists():
            rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
            raise TaskSkipped(f"{name}：{rel} 尚未提供，本步跳过")
        _run_rc([sys.executable, str(path), "--ds", ds], name)
    fn.__name__ = name
    return fn


weather_join = _optional_script(WEATHER_JOIN, "weather_join")
site_suggest = _optional_script(SITE_SUGGEST, "site_suggest")

# publish 成功后执行；彼此独立，一个失败/跳过不影响其他
AFTER_PUBLISH = [
    ("forecast_daily", forecast_daily),
    ("forecast_evaluate", forecast_evaluate),   # 评估 ds 当天（前一天的预测）；今天预测跳过照评，预测失败则跳过（同 DAG none_failed）
    ("weather_join", weather_join),
    ("site_suggest", site_suggest),
]


def recorded(name: str, fn, source: str = "airflow"):
    """包一层运行记录：开始/结束/状态/拦截原因写 ops_task_run，异常照常抛出（Airflow 据此判失败）。"""
    # 只接 ds/write_db：Airflow 遇到 **kwargs 会把整个 context 塞进来
    def wrapper(ds: str, write_db: bool = True):
        start = datetime.now()
        try:
            out = fn(ds, write_db=write_db) if name.startswith("dq_") else fn(ds)
        except TaskSkipped as e:
            if write_db:
                run_log.record(name, ds, "skipped", start, datetime.now(), str(e), source)
            raise
        except Exception as e:
            if write_db:
                state = "blocked" if isinstance(e, engine.DQBlockError) else "failed"
                run_log.record(name, ds, state, start, datetime.now(), run_log.reason_from_exc(name, ds, e), source)
            raise
        if write_db:
            run_log.record(name, ds, "success", start, datetime.now(), None, source)
        return out
    wrapper.__name__ = name
    return wrapper


def run_chain(ds: str, write_db: bool = True, source: str = "run_day") -> list[tuple[str, str, float]]:
    """顺序执行整条链路；任何任务异常（含 DQBlockError）立即停止，下游标记 skipped。
    write_db=True 时每个任务的运行情况写入 ops_task_run（被跳过的下游也写一行 skipped）。"""
    timings, failed = [], False
    for name, fn in CHAIN:
        if failed:
            timings.append((name, "skipped", 0.0))
            if write_db:
                now = datetime.now()
                run_log.record(name, ds, "skipped", now, now, "上游任务未通过，本步未执行", source)
            continue
        t0 = time.perf_counter()
        try:
            recorded(name, fn, source)(ds, write_db=write_db)
            timings.append((name, "success", time.perf_counter() - t0))
        except Exception as e:
            print(f"[fail] {name}: {e}", flush=True)
            timings.append((name, "failed", time.perf_counter() - t0))
            failed = True
    for name, fn in AFTER_PUBLISH:
        if name == "forecast_evaluate" and any(n == "forecast_daily" and st == "failed" for n, st, _ in timings):
            failed_daily = True
        else:
            failed_daily = False
        if failed or failed_daily:
            timings.append((name, "skipped", 0.0))
            if write_db:
                now = datetime.now()
                run_log.record(name, ds, "skipped", now, now, "上游任务未通过，本步未执行", source)
            continue
        t0 = time.perf_counter()
        try:
            recorded(name, fn, source)(ds, write_db=write_db)
            timings.append((name, "success", time.perf_counter() - t0))
        except TaskSkipped as e:
            print(f"[skip] {e}", flush=True)
            timings.append((name, "skipped", time.perf_counter() - t0))
        except Exception as e:  # 扩展任务失败只记 failed，不影响同一天其他扩展任务
            print(f"[fail] {name}: {e}", flush=True)
            timings.append((name, "failed", time.perf_counter() - t0))
    return timings
