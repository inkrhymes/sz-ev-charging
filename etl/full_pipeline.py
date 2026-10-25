#!/usr/bin/env python3
"""UrbanEV Shenzhen full-data ETL -> date-partitioned Parquet lake (Spark/Hive readable).

Layout (hive-style, partition column `dt` NOT stored inside files):
  data/lake/ods/charge_5min/dt=YYYY-MM-DD/part-0.parquet
  data/lake/dwd/charge_5min_clean/dt=YYYY-MM-DD/part-0.parquet
  data/lake/ods/station_info/part-0.parquet   (station_information.csv as Parquet)
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

Staging is written per station-group and per day (stg/dt=X/gNNN.parquet), so
the split step reads ~34 small files per dt in parallel. Peak memory ~= workers *
group_size station files (4 * 20 * 52k rows); measured peak ~2.2 GB total.
(old note: ~= 3.4M rows ~= 0.6 GB) plus workers * one station file.
"""
from __future__ import annotations

import argparse, json, os, shutil, time, datetime as _dt
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SRC = Path("/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/20220901-20230228_station-raw")
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))  # override to share one lake across clones
HIGH_MISSING_THR = 0.30
REPAIR_STUCK_DAYS = 30      # cumulative stuck days -> repair ticket
WARN_STUCK_STATIONS = 221   # daily warning: stuck stations > p95 of 181 days
WARN_NEW_STUCK = 63         # daily warning: newly stuck stations > p95
LOOKBACK_DAYS = 7  # stuck / IQR-outlier window: the day plus 6 previous days
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
    # Flags use a FIXED trailing window per day (LOOKBACK_DAYS incl. the day itself),
    # read straight from the source CSV, so results do not depend on --start/--end.
    df = df[(df.ts >= start - pd.Timedelta(days=LOOKBACK_DAYS - 1)) & (df.ts < end)].reset_index(drop=True)
    miss_all = (df["busy"].isna() & df["idle"].isna()).to_numpy()
    day = df.ts.dt.normalize()
    days = pd.date_range(max(start, day.min()), end - pd.Timedelta(days=1), freq="D")
    busy = df["busy"].to_numpy(float)
    vb = np.where(miss_all, np.nan, busy)
    stuck_day, fence = {}, {}
    dvals = day.to_numpy()
    for d in days:
        w = (dvals >= np.datetime64(d - pd.Timedelta(days=LOOKBACK_DAYS - 1))) & (dvals <= np.datetime64(d))
        x = vb[w]; x = x[~np.isnan(x)]
        stuck_day[d] = int(len(x) >= 12 and np.unique(x).size == 1)
        if len(x) >= 8:
            q1, q3 = np.quantile(x, [.25, .75]); iqr = q3 - q1
            fence[d] = (q1 - 1.5*iqr, q3 + 1.5*iqr) if iqr > 0 else None
        else:
            fence[d] = None
    keep = (df.ts >= start).to_numpy()
    df = df[keep].reset_index(drop=True); dvals = dvals[keep]; busy = busy[keep]
    if df.empty:
        return sid, 0, None
    miss = pd.Series(miss_all[keep])
    dser = pd.Series(dvals)
    stuck_arr = dser.map(lambda d: stuck_day[pd.Timestamp(d)]).to_numpy()
    lo = dser.map(lambda d: fence[pd.Timestamp(d)][0] if fence[pd.Timestamp(d)] else np.nan).to_numpy()
    hi = dser.map(lambda d: fence[pd.Timestamp(d)][1] if fence[pd.Timestamp(d)] else np.nan).to_numpy()
    with np.errstate(invalid="ignore"):
        out_mask = ~np.isnan(busy) & ~np.isnan(lo) & ((busy < lo) | (busy > hi))
    stuck_days = int(sum(stuck_day.values()))
    stuck = int(stuck_days > 0)
    df.insert(0, "station_id", sid)
    df["taz_id"] = taz
    df["total_piles"] = piles
    df["is_row_missing"] = miss.astype("int8")
    df["is_stuck"] = stuck_arr.astype("int8")
    df["is_outlier"] = out_mask.astype("int8")
    ok = (~miss) & df["busy"].notna() & (piles or 0) > 0
    df["occupancy"] = np.where(ok, df["busy"] / (piles if piles else np.nan), np.nan)
    df["quality_flag"] = np.where(miss, "dropped_src", np.where(out_mask, "outlier", "ok"))
    # max consecutive missing run
    m = miss.to_numpy().astype(int); run = best = 0
    for v in m:
        run = run + 1 if v else 0; best = max(best, run)
    stat = dict(station_id=sid, taz_id=taz, rows=len(df), missing_rows=int(m.sum()),
                missing_rate=round(float(m.mean()), 6), stuck_flag=stuck, stuck_days=stuck_days,
                outlier_rows=int(out_mask.sum()), gap_days_max=int(round(best*5/1440)),
                stuck_dates=[d.strftime("%Y-%m-%d") for d, v in stuck_day.items() if v])
    return sid, df, stat


def _stage_group(args):
    """Process a group of stations; write staging split by day: stg/dt=X/g<k>.parquet.
    Lets the split step read only ~n_groups small files per dt instead of every station file."""
    k, jobs, out = args
    frames, stats = [], []
    for j in jobs:
        sid, df, st = _stage_one(j)
        if st: frames.append(df); stats.append(st)
    if not frames:
        return 0, stats
    df = pd.concat(frames, ignore_index=True); del frames
    df["ts"] = df["ts"].astype("datetime64[us]")
    for dt, g in df.groupby(df.ts.dt.strftime("%Y-%m-%d")):
        d = out / f"dt={dt}"; d.mkdir(exist_ok=True)
        g.to_parquet(d / f"g{k:03d}.parquet", index=False)
    return len(df), stats


def _split_day(args):
    dt, stg, ods_root, dwd_root, layer = args
    g = pq.read_table(stg / f"dt={dt}").to_pandas()
    g = g.sort_values(["station_id", "ts"], kind="stable")
    for root, sch in [x for x in ((ods_root, ODS_SCHEMA), (dwd_root, DWD_SCHEMA)) if layer == "all" or x[0].parent.name == layer]:
        pdir = root / f"dt={dt}"; tmp = root / f".tmp_dt={dt}"
        shutil.rmtree(tmp, ignore_errors=True); tmp.mkdir(parents=True)
        tbl = pa.Table.from_pandas(g[sch.names], schema=sch, preserve_index=False)
        pq.write_table(tbl, tmp / "part-0.parquet", compression="snappy")
        shutil.rmtree(pdir, ignore_errors=True); tmp.rename(pdir)
    return dt, len(g)


def dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2022-09-01")
    ap.add_argument("--end", default="2023-02-28", help="inclusive date")
    ap.add_argument("--stations", default="all", help="'all' or comma list or N (first N)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-days", type=int, default=7, help="(unused since v3; kept for CLI compat)")
    ap.add_argument("--group-size", type=int, default=20, help="stations per staging task (memory ~ group_size * 52k rows per worker)")
    ap.add_argument("--layer", choices=["ods", "dwd", "all"], default="all", help="which partitions to (re)write")
    ap.add_argument("--write-report", action="store_true", help="also rewrite docs/full_load_metrics.md and docs/quality_report_full.md")
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
    si = LAKE / "ods" / "station_info"; si.mkdir(parents=True, exist_ok=True)
    info.rename(columns={"TAZID": "taz_id"}).astype({"station_id": "int32", "taz_id": "int32"}).to_parquet(si / "part-0.parquet", index=False)
    stg = LAKE / "_staging" / f"{tag}_{os.getpid()}"  # per-process: concurrent runs on the shared box must not clobber each other
    shutil.rmtree(stg, ignore_errors=True); stg.mkdir(parents=True)
    m = {}
    src_bytes = sum(f.stat().st_size for f in files)

    t = time.time()
    jobs = [(str(f), start, end, piles.get(int(f.stem)), taz.get(int(f.stem)), stg) for f in files]
    gs = max(1, a.group_size)
    groups = [(k, jobs[i:i+gs], stg) for k, i in enumerate(range(0, len(jobs), gs))]
    stats = []; rows = 0
    with ProcessPoolExecutor(a.workers) as ex:
        for n, sts in ex.map(_stage_group, groups):
            rows += n; stats += sts
    m["stage"] = dict(rows=rows, secs=time.time()-t, bytes=dir_size(stg), files=len(files), src_bytes=src_bytes)
    print("stage", m["stage"], flush=True)

    t = time.time(); ods_rows = dwd_rows = 0; parts = 0
    ods_root = LAKE / "ods" / "charge_5min"; dwd_root = LAKE / "dwd" / "charge_5min_clean"
    dts = sorted(p.name[3:] for p in stg.glob("dt=*"))
    with ProcessPoolExecutor(a.workers) as ex:
        for dt, n in ex.map(_split_day, [(dt, stg, ods_root, dwd_root, a.layer) for dt in dts]):
            ods_rows += n; dwd_rows += n; parts += 1
    if ods_rows != rows:
        raise SystemExit(f"row mismatch: staged {rows} vs written {ods_rows}")
    m["split"] = dict(ods_rows=ods_rows, dwd_rows=dwd_rows, partitions=parts, secs=time.time()-t,
                      ods_bytes=dir_size(ods_root), dwd_bytes=dir_size(dwd_root))
    print("split", m["split"], flush=True)

    t = time.time()
    q = pd.DataFrame(stats).sort_values("station_id")
    q["ticket_suggest"] = np.where(q.stuck_days >= REPAIR_STUCK_DAYS, "repair",
                                   np.where(q.missing_rate > HIGH_MISSING_THR, "inspect", "none"))
    qdir = LAKE / "dwd" / "quality_station" / f"run={tag}"; qdir.mkdir(parents=True, exist_ok=True)
    q.drop(columns="stuck_dates").to_parquet(qdir / "part-0.parquet", index=False)
    if a.write_report:
        write_quality(q, a, tag)
    m["report"] = dict(secs=time.time()-t)
    if not a.keep_staging: shutil.rmtree(stg, ignore_errors=True)
    run = dict(args=vars(a), finished=_dt.datetime.now().isoformat(timespec="seconds"), metrics=m,
               quality=dict(stations=len(q), rows=int(q.rows.sum()), missing_rows=int(q.missing_rows.sum()),
                            outlier_rows=int(q.outlier_rows.sum()), stuck_stations=int(q.stuck_flag.sum()),
                            stuck_station_days=int(q.stuck_days.sum()),
                            repair=int((q.ticket_suggest == "repair").sum()), inspect=int((q.ticket_suggest == "inspect").sum())))
    rd = LAKE / "_runs"; rd.mkdir(parents=True, exist_ok=True)
    (rd / f"{tag}_{_dt.datetime.now():%Y%m%dT%H%M%S}_{os.getpid()}.json").write_text(json.dumps(run, indent=2, default=str))
    print("quality", run["quality"], flush=True)
    if a.write_report:
        write_metrics(m, a, tag)


def daily_stuck(q):
    days = {}
    for sid, ds in zip(q.station_id, q.stuck_dates):
        for d in ds: days.setdefault(d, set()).add(sid)
    alld = sorted(days); rows = []; prev = None
    for d in pd.date_range(min(alld), max(alld)).strftime("%Y-%m-%d") if alld else []:
        cur = days.get(d, set())
        # first day has no "yesterday": new_stuck undefined (excluded from p95 and warning)
        rows.append(dict(dt=d, stuck=len(cur), new_stuck=pd.NA if prev is None else len(cur - prev))); prev = cur
    df = pd.DataFrame(rows, columns=["dt", "stuck", "new_stuck"])
    df["new_stuck"] = df["new_stuck"].astype("Int64")
    return df


def write_quality(q, a, tag):
    tot = q.rows.sum(); miss = q.missing_rows.sum()
    dd = daily_stuck(q)
    warn = dd[(dd.stuck > WARN_STUCK_STATIONS) | (dd.new_stuck.fillna(-1) > WARN_NEW_STUCK)]
    sd = q.stuck_days
    lines = [f"# 全量数据质检报告（{a.start} ~ {a.end}）", "",
             "由 `etl/full_pipeline.py --write-report` 生成。规则：",
             "- 整行缺失：busy 与 idle 均为空。",
             "- 卡死：某站某天连同前 6 天（共 7 天）的有效 busy 只有一个取值（≥12 点），按站按天标记，与运行区间无关。",
             "- 异常：busy 超出该站同一 7 天窗口的 IQR 1.5 倍边界。",
             f"- 单日卡死只做**预警**，不开工单：当天卡死站数 > {WARN_STUCK_STATIONS} 或新增卡死站数 > {WARN_NEW_STUCK}（两个阈值取 181 天的 P95）。",
             "  - 新增卡死口径：180 对相邻两天，统计「当天 is_stuck 且前一天不是」的站数，首日（无前一天）不计；P95 = 63.25，取 63。",
             f"- 累计卡死 ≥ {REPAIR_STUCK_DAYS} 天：建议**维修**工单；缺失率 > {HIGH_MISSING_THR:.0%}（且未到维修）：建议**巡检**。", "",
             "| 指标 | 数值 |", "|---|---|",
             f"| 站点数 | {len(q)} |", f"| 片区数（TAZ） | {q.taz_id.nunique()} |",
             f"| 总行数 | {tot:,} |", f"| 缺失行数 | {miss:,} |",
             f"| 整体缺失率 | {miss/tot:.2%} |",
             f"| 异常点行数 | {q.outlier_rows.sum():,} ({q.outlier_rows.sum()/tot:.2%}) |",
             f"| 至少一天卡死的站点 | {int((sd >= 1).sum())} |", f"| 卡死站·天 | {int(sd.sum()):,} |",
             f"| 触发单日预警的天数 | {len(warn)} / {len(dd)} |",
             f"| 高缺失站点（>{HIGH_MISSING_THR:.0%}） | {int((q.missing_rate>HIGH_MISSING_THR).sum())} |",
             f"| 全部缺失的站点 | {int((q.missing_rate>=1).sum())} |",
             f"| 建议工单：维修 / 巡检 | {int((q.ticket_suggest=='repair').sum())} / {int((q.ticket_suggest=='inspect').sum())} |",
             "", "## 累计卡死天数分布", "", "| 累计卡死天数 | 站点数 |", "|---|---|"]
    for k in (1, 7, 30, 90):
        lines.append(f"| ≥ {k} | {int((sd >= k).sum())} |")
    lines.append(f"| = {len(dd) or 181}（全程） | {int((sd >= (len(dd) or 181)).sum())} |")
    if len(dd):
        lines += ["", "## 每日卡死站数", "",
                  f"P50 / P95 / 最大：当天卡死 {dd.stuck.quantile(.5):.0f} / {dd.stuck.quantile(.95):.0f} / {dd.stuck.max()}；"
                  f"新增 {dd.new_stuck.quantile(.5):.0f} / {dd.new_stuck.quantile(.95):.0f} / {dd.new_stuck.max()}。"]
        if len(warn):
            lines += ["", "触发预警的日期：" + "、".join(f"{r.dt}（{r.stuck}/{'–' if pd.isna(r.new_stuck) else r.new_stuck}）" for r in warn.itertuples()) + "（括号内为 当天卡死/新增）。"]
    lines += ["", "## 缺失率最高的 15 个站点", "",
              "| station_id | taz_id | missing_rate | stuck_days | gap_days_max | ticket |", "|---|---|---|---|---|---|"]
    for r in q.sort_values("missing_rate", ascending=False).head(15).itertuples():
        lines.append(f"| {r.station_id} | {r.taz_id} | {r.missing_rate:.2%} | {r.stuck_days} | {r.gap_days_max} | {r.ticket_suggest} |")
    lines += ["", f"站点明细：`data/lake/dwd/quality_station/run={tag}/part-0.parquet`（不入库）。", ""]
    (ROOT / "docs" / "quality_report_full.md").write_text("\n".join(lines), encoding="utf-8")


def write_metrics(m, a, tag):
    p = ROOT / "docs" / "full_load_metrics.md"
    mb = lambda b: f"{b/1024**2:,.1f} MB"
    if not p.exists():
        p.write_text("# 全量装载记录\n\n每次运行 `etl/full_pipeline.py` 追加一节。耗时为墙钟时间，机器 8 核 / 15 GB（与其他服务共用）。\n", encoding="utf-8")
    s, sp = m["stage"], m["split"]
    sec = [f"\n## {tag}（stations={a.stations}, workers={a.workers}, group_size={a.group_size}，"
           f"{_dt.datetime.now():%Y-%m-%d %H:%M} UTC+8）", "",
           "| 步骤 | 行数 | 存储 | 耗时 |", "|---|---|---|---|",
           f"| 源 CSV 读取+清洗规则（{s['files']} 个站文件，{mb(s['src_bytes'])}） | {s['rows']:,} | 中间件 {mb(s['bytes'])} | {s['secs']:.1f} s |",
           f"| 写 ODS 分区（{sp['partitions']} 个 dt） | {sp['ods_rows']:,} | {mb(sp['ods_bytes'])}（全湖累计） | {sp['secs']:.1f} s（含 DWD） |",
           f"| 写 DWD 分区 | {sp['dwd_rows']:,} | {mb(sp['dwd_bytes'])}（全湖累计） | 同上 |",
           f"| 质检汇总+报告 | — | — | {m['report']['secs']:.1f} s |", ""]
    with p.open("a", encoding="utf-8") as f: f.write("\n".join(sec))


if __name__ == "__main__":
    main()
