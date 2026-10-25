#!/usr/bin/env python3
"""UrbanEV Shenzhen full-data ETL -> date-partitioned Parquet lake (Spark/Hive readable).

Layout (hive-style, partition column `dt` NOT stored inside files):
  data/lake/ods/charge_5min/dt=YYYY-MM-DD/part-0.parquet
  data/lake/dwd/charge_5min_clean/dt=YYYY-MM-DD/part-0.parquet
  data/lake/dwd/quality_station/run=<start>_<end>/part-0.parquet
  data/lake/_staging/<start>_<end>_<pid>/<station>.parquet   (intermediate, per station)

Steps
  1 stage : read each station CSV (parallel, --workers), filter [start,end],
            apply demo cleaning rules per station over the window
            (row-missing / stuck / IQR outlier / occupancy), write staging.
  2 split : for windows of --batch-days days, read staging and overwrite each
            dt partition (idempotent: a rerun replaces the partition dir).
  3 report: per-station quality summary + docs/quality_report_full.md,
            metrics appended to docs/full_load_metrics.md.

Peak memory ~= batch_days * n_stations * 288 rows (7 days, 1682 stations
~= 3.4M rows ~= 0.6 GB) plus workers * one station file.
"""
from __future__ import annotations

import argparse, os, shutil, time, datetime as _dt
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SRC = Path("/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/20220901-20230228_station-raw")
LAKE = ROOT / "data" / "lake"
HIGH_MISSING_THR = 0.30
RAW_COLS = ["busy", "idle", "s_price", "e_price", "fast_busy", "fast_idle",
            "slow_busy", "slow_idle", "duration", "volume"]

ODS_SCHEMA = pa.schema([("station_id", pa.int32()), ("ts", pa.timestamp("us"))]
                       + [(c, pa.float64()) for c in RAW_COLS])
DWD_SCHEMA = pa.schema([
    ("station_id", pa.int32()), ("taz_id", pa.int32()), ("ts", pa.timestamp("us")),
    ("busy", pa.float64()), ("idle", pa.float64()), ("total_piles", pa.int32()),
    ("s_price", pa.float64()), ("e_price", pa.float64()),
    ("duration", pa.float64()), ("volume", pa.float64()),
    ("is_row_missing", pa.int8()), ("is_stuck", pa.int8()), ("is_outlier", pa.int8()),
    ("occupancy", pa.float64()), ("quality_flag", pa.string())])


def _stage_one(args):
    path, start, end, piles, taz, out = args
    sid = int(Path(path).stem)
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["time"]); df = df.drop(columns="time")
    df = df[(df.ts >= start) & (df.ts < end)].reset_index(drop=True)
    if df.empty:
        return sid, 0, None
    miss = df["busy"].isna() & df["idle"].isna()
    valid = df.loc[~miss, "busy"]
    stuck = int(len(valid) >= 12 and valid.nunique() == 1)
    out_mask = np.zeros(len(df), bool)
    x = valid.dropna()
    if len(x) >= 8:
        q1, q3 = x.quantile(.25), x.quantile(.75); iqr = q3 - q1
        if iqr > 0:
            b = df["busy"]; out_mask = (b.notna() & ((b < q1 - 1.5*iqr) | (b > q3 + 1.5*iqr))).to_numpy()
    df.insert(0, "station_id", sid)
    df["taz_id"] = taz
    df["total_piles"] = piles
    df["is_row_missing"] = miss.astype("int8")
    df["is_stuck"] = np.int8(stuck)
    df["is_outlier"] = out_mask.astype("int8")
    ok = (~miss) & df["busy"].notna() & (piles or 0) > 0
    df["occupancy"] = np.where(ok, df["busy"] / (piles if piles else np.nan), np.nan)
    df["quality_flag"] = np.where(miss, "dropped_src", np.where(out_mask, "outlier", "ok"))
    # max consecutive missing run
    m = miss.to_numpy().astype(int); run = best = 0
    for v in m:
        run = run + 1 if v else 0; best = max(best, run)
    df.to_parquet(out / f"{sid}.parquet", index=False)
    stat = dict(station_id=sid, taz_id=taz, rows=len(df), missing_rows=int(m.sum()),
                missing_rate=round(float(m.mean()), 6), stuck_flag=stuck,
                outlier_rows=int(out_mask.sum()), gap_days_max=int(round(best*5/1440)))
    return sid, len(df), stat


def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-09-01")
    ap.add_argument("--end", default="2023-02-28", help="inclusive date")
    ap.add_argument("--stations", default="all", help="'all' or comma list or N (first N)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-days", type=int, default=7)
    ap.add_argument("--layer", choices=["ods", "dwd", "all"], default="all", help="which partitions to (re)write")
    ap.add_argument("--keep-staging", action="store_true")
    ap.add_argument("--ds", help="single day YYYY-MM-DD (overrides --start/--end)")
    a = ap.parse_args()
    if a.ds: a.start = a.end = a.ds
    start = pd.Timestamp(a.start); end = pd.Timestamp(a.end) + pd.Timedelta(days=1)
    tag = f"{a.start}_{a.end}"
    files = sorted((SRC / "charge_5min").glob("*.csv"), key=lambda p: int(p.stem))
    if a.stations.isdigit(): files = files[:int(a.stations)]
    elif a.stations != "all":
        keep = set(a.stations.split(",")); files = [f for f in files if f.stem in keep]
    info = pd.read_csv(SRC / "station_information.csv")
    piles = dict(zip(info.station_id.astype(int), info.charge_count.astype(int)))
    taz = dict(zip(info.station_id.astype(int), info.TAZID.astype(int)))
    stg = LAKE / "_staging" / f"{tag}_{os.getpid()}"  # per-process: concurrent runs on the shared box must not clobber each other
    shutil.rmtree(stg, ignore_errors=True); stg.mkdir(parents=True)
    m = {}
    src_bytes = sum(f.stat().st_size for f in files)

    t = time.time()
    jobs = [(str(f), start, end, piles.get(int(f.stem)), taz.get(int(f.stem)), stg) for f in files]
    stats = []; rows = 0
    with ProcessPoolExecutor(a.workers) as ex:
        for sid, n, st in ex.map(_stage_one, jobs, chunksize=8):
            rows += n
            if st: stats.append(st)
    m["stage"] = dict(rows=rows, secs=time.time()-t, bytes=dir_size(stg), files=len(files), src_bytes=src_bytes)
    print("stage", m["stage"], flush=True)

    t = time.time(); ods_rows = dwd_rows = 0; parts = 0
    ods_root = LAKE / "ods" / "charge_5min"; dwd_root = LAKE / "dwd" / "charge_5min_clean"
    stg_files = sorted(stg.glob("*.parquet"))
    d = start
    while d < end:
        w_end = min(d + pd.Timedelta(days=a.batch_days), end)
        flt = [("ts", ">=", d.to_pydatetime()), ("ts", "<", w_end.to_pydatetime())]
        tables = [pq.read_table(f, filters=flt) for f in stg_files]
        df = pa.concat_tables(tables, promote_options="default").to_pandas()
        df["dt"] = df.ts.dt.strftime("%Y-%m-%d")
        for dt, g in df.groupby("dt"):
            g = g.sort_values(["station_id", "ts"])
            g = g.assign(ts=g.ts.astype("datetime64[us]"))
            for root, sch in [x for x in ((ods_root, ODS_SCHEMA), (dwd_root, DWD_SCHEMA)) if a.layer == "all" or x[0].parent.name == a.layer]:
                pdir = root / f"dt={dt}"; tmp = root / f".tmp_dt={dt}"
                shutil.rmtree(tmp, ignore_errors=True); tmp.mkdir(parents=True)
                tbl = pa.Table.from_pandas(g[sch.names], schema=sch, preserve_index=False)
                pq.write_table(tbl, tmp / "part-0.parquet", compression="snappy")
                shutil.rmtree(pdir, ignore_errors=True); tmp.rename(pdir)
            ods_rows += len(g); dwd_rows += len(g); parts += 1
        del df, tables
        print("split window", d.date(), "->", w_end.date(), flush=True)
        d = w_end
    if ods_rows != rows:
        raise SystemExit(f"row mismatch: staged {rows} vs written {ods_rows}")
    m["split"] = dict(ods_rows=ods_rows, dwd_rows=dwd_rows, partitions=parts, secs=time.time()-t,
                      ods_bytes=dir_size(ods_root), dwd_bytes=dir_size(dwd_root))
    print("split", m["split"], flush=True)

    t = time.time()
    q = pd.DataFrame(stats).sort_values("station_id")
    q["ticket_suggest"] = np.where(q.stuck_flag == 1, "repair",
                                   np.where(q.missing_rate > HIGH_MISSING_THR, "inspect", "none"))
    qdir = LAKE / "dwd" / "quality_station" / f"run={tag}"; qdir.mkdir(parents=True, exist_ok=True)
    q.to_parquet(qdir / "part-0.parquet", index=False)
    write_quality(q, a, tag)
    m["report"] = dict(secs=time.time()-t)
    if not a.keep_staging: shutil.rmtree(stg, ignore_errors=True)
    write_metrics(m, a, tag)


def write_quality(q, a, tag):
    tot = q.rows.sum(); miss = q.missing_rows.sum()
    tazs = q.taz_id.nunique()
    lines = [f"# 全量数据质检报告（{a.start} ~ {a.end}）", "",
             "由 `etl/full_pipeline.py` 生成，规则与演示子集一致（`etl/clean_to_dwd.py`）：整行缺失 = busy 与 idle 均为空；"
             "卡死 = 窗口内有效 busy 只有一个取值（≥12 点）；异常 = busy 超出站内 IQR 1.5 倍边界；"
             f"缺失率 > {HIGH_MISSING_THR:.0%} 建议巡检，卡死建议维修。", "",
             "| 指标 | 数值 |", "|---|---|",
             f"| 站点数 | {len(q)} |", f"| 片区数（TAZ） | {tazs} |",
             f"| 总行数 | {tot:,} |", f"| 缺失行数 | {miss:,} |",
             f"| 整体缺失率 | {miss/tot:.2%} |",
             f"| 异常点行数 | {q.outlier_rows.sum():,} ({q.outlier_rows.sum()/tot:.2%}) |",
             f"| 卡死站点 | {int(q.stuck_flag.sum())} |",
             f"| 高缺失站点（>{HIGH_MISSING_THR:.0%}） | {int((q.missing_rate>HIGH_MISSING_THR).sum())} |",
             f"| 全部缺失的站点 | {int((q.missing_rate>=1).sum())} |",
             f"| 建议工单：维修 / 巡检 | {int((q.ticket_suggest=='repair').sum())} / {int((q.ticket_suggest=='inspect').sum())} |",
             "", "## 缺失率最高的 15 个站点", "",
             "| station_id | taz_id | missing_rate | stuck | gap_days_max | ticket |", "|---|---|---|---|---|---|"]
    for r in q.sort_values("missing_rate", ascending=False).head(15).itertuples():
        lines.append(f"| {r.station_id} | {r.taz_id} | {r.missing_rate:.2%} | {r.stuck_flag} | {r.gap_days_max} | {r.ticket_suggest} |")
    lines += ["", f"站点明细：`data/lake/dwd/quality_station/run={tag}/part-0.parquet`（不入库）。", ""]
    (ROOT / "docs" / "quality_report_full.md").write_text("\n".join(lines), encoding="utf-8")


def write_metrics(m, a, tag):
    p = ROOT / "docs" / "full_load_metrics.md"
    mb = lambda b: f"{b/1024**2:,.1f} MB"
    if not p.exists():
        p.write_text("# 全量装载记录\n\n每次运行 `etl/full_pipeline.py` 追加一节。耗时为墙钟时间，机器 8 核 / 15 GB（与其他服务共用）。\n", encoding="utf-8")
    s, sp = m["stage"], m["split"]
    sec = [f"\n## {tag}（stations={a.stations}, workers={a.workers}, batch_days={a.batch_days}，"
           f"{_dt.datetime.now():%Y-%m-%d %H:%M} UTC+8）", "",
           "| 步骤 | 行数 | 存储 | 耗时 |", "|---|---|---|---|",
           f"| 源 CSV 读取+清洗规则（{s['files']} 个站文件，{mb(s['src_bytes'])}） | {s['rows']:,} | 中间件 {mb(s['bytes'])} | {s['secs']:.1f} s |",
           f"| 写 ODS 分区（{sp['partitions']} 个 dt） | {sp['ods_rows']:,} | {mb(sp['ods_bytes'])}（全湖累计） | {sp['secs']:.1f} s（含 DWD） |",
           f"| 写 DWD 分区 | {sp['dwd_rows']:,} | {mb(sp['dwd_bytes'])}（全湖累计） | 同上 |",
           f"| 质检汇总+报告 | — | — | {m['report']['secs']:.1f} s |", ""]
    with p.open("a", encoding="utf-8") as f: f.write("\n".join(sec))


if __name__ == "__main__":
    main()
