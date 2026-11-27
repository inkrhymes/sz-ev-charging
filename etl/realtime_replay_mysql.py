#!/usr/bin/env python3
"""轻量级演示实时回放：按 15 分钟时间段聚合 demo 清洗数据并写入 MySQL rt_zone_occupancy。

用途：答辩演示或低配笔记本环境，无需启动 Kafka / Spark 集群即可驱动「实时占用」大屏每 5 秒刷新。
聚合口径与 stream/zone_occupancy_stream.py 保持一致（坏标记丢弃并计数，occupancy = busy_sum / piles_sum）。

用法：
  python etl/realtime_replay_mysql.py --date 2022-09-02 --interval 3
  python etl/realtime_replay_mysql.py --dry-run --limit 3
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLEAN = ROOT / "data" / "demo" / "dwd" / "dwd_station_5min_clean.csv"
DEFAULT_STATIONS = ROOT / "data" / "demo" / "demo_station_list.csv"

UPSERT_SQL = (
    "INSERT INTO rt_zone_occupancy (taz_id, window_start, window_end, occupancy, busy_sum, piles_sum, station_cnt,"
    " msg_cnt, outlier_cnt, dropped_src_cnt, ingest_ts_max, write_ts, latency_ms, batch_id)"
    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) AS new"
    " ON DUPLICATE KEY UPDATE window_start=new.window_start, occupancy=new.occupancy, busy_sum=new.busy_sum,"
    " piles_sum=new.piles_sum, station_cnt=new.station_cnt, msg_cnt=new.msg_cnt, outlier_cnt=new.outlier_cnt,"
    " dropped_src_cnt=new.dropped_src_cnt, ingest_ts_max=new.ingest_ts_max,"
    " write_ts=new.write_ts, latency_ms=new.latency_ms, batch_id=new.batch_id"
)


def _open_csv(path: Path):
    if path.is_file():
        return path.open(encoding="utf-8")
    gz = Path(f"{path}.gz")
    if gz.is_file():
        return gzip.open(gz, "rt", encoding="utf-8")
    raise FileNotFoundError(f"找不到 {path} 或 {gz}")


def load_station_taz_map(station_list_path: Path = DEFAULT_STATIONS) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with station_list_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            sid = (r.get("sid") or r.get("station_id") or "").strip()
            taz = (r.get("TAZID") or r.get("taz_id") or "").strip()
            if sid and taz:
                mapping[sid] = taz
    return mapping


def _parse_ts(raw: str) -> dt.datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    s = s.replace("T", " ")[:19]
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _bad_flags() -> set[str]:
    raw = os.getenv("SZ_RT_BAD_FLAGS", "outlier,dropped_src")
    return {x.strip().lower() for x in raw.split(",") if x.strip()}


def iter_demo_windows(
    clean_path: Path = DEFAULT_CLEAN,
    station_list_path: Path = DEFAULT_STATIONS,
    date_filter: str | None = None,
    window_minutes: int = 15,
) -> Iterator[tuple[dt.datetime, dt.datetime, list[dict]]]:
    """将 demo 5 分钟清洗 CSV 按 15 分钟时间段聚合，按时间升序产出每个时间段各片区指标。"""
    taz_of = load_station_taz_map(station_list_path)
    bad = _bad_flags()
    step = max(5, int(window_minutes))

    # buckets[(window_start, taz_id)] = stats
    buckets: dict[tuple[dt.datetime, str], dict] = {}
    with _open_csv(clean_path) as f:
        for r in csv.DictReader(f):
            ts_raw = r.get("ts") or ""
            if date_filter and not ts_raw.startswith(date_filter):
                continue
            sid = (r.get("station_id") or "").strip()
            taz = taz_of.get(sid)
            if not taz:
                continue
            ts = _parse_ts(ts_raw)
            if ts is None:
                continue
            minute = (ts.minute // step) * step
            ws = ts.replace(minute=minute, second=0, microsecond=0)
            key = (ws, taz)
            st = buckets.get(key)
            if st is None:
                st = {
                    "busy_sum": 0,
                    "piles_sum": 0,
                    "stations": set(),
                    "msg_cnt": 0,
                    "outlier_cnt": 0,
                    "dropped_src_cnt": 0,
                }
                buckets[key] = st

            flag = (r.get("quality_flag") or "").strip().lower()
            is_outlier = (r.get("is_outlier") or "").strip() in {"1", "1.0", "true"}
            if flag == "dropped_src":
                st["dropped_src_cnt"] += 1
                continue
            if flag == "outlier" or (is_outlier and "outlier" in bad):
                st["outlier_cnt"] += 1
                continue
            if bad and flag in bad:
                continue

            busy_s = (r.get("busy") or "").strip()
            piles_s = (r.get("total_piles") or "").strip()
            if not busy_s or not piles_s:
                continue
            try:
                busy = int(round(float(busy_s)))
                piles = int(round(float(piles_s)))
            except ValueError:
                continue
            if piles <= 0:
                continue

            st["busy_sum"] += busy
            st["piles_sum"] += piles
            st["stations"].add(sid)
            st["msg_cnt"] += 1

    by_window: dict[dt.datetime, list[dict]] = defaultdict(list)
    for (ws, taz), st in buckets.items():
        if st["piles_sum"] <= 0:
            continue
        we = ws + dt.timedelta(minutes=step)
        by_window[ws].append(
            {
                "taz_id": taz,
                "window_start": ws,
                "window_end": we,
                "occupancy": round(st["busy_sum"] / st["piles_sum"], 6),
                "busy_sum": st["busy_sum"],
                "piles_sum": st["piles_sum"],
                "station_cnt": len(st["stations"]),
                "msg_cnt": st["msg_cnt"],
                "outlier_cnt": st["outlier_cnt"],
                "dropped_src_cnt": st["dropped_src_cnt"],
            }
        )

    for ws in sorted(by_window):
        we = ws + dt.timedelta(minutes=step)
        recs = sorted(by_window[ws], key=lambda x: int(x["taz_id"]) if x["taz_id"].isdigit() else 0)
        yield ws, we, recs


def to_db_rows(records: list[dict], batch_id: int, now: dt.datetime | None = None, latency_ms: int = 320) -> list[tuple]:
    now = now or dt.datetime.now()
    ingest_ts = now - dt.timedelta(milliseconds=max(0, int(latency_ms)))
    rows = []
    for r in records:
        rows.append(
            (
                str(r["taz_id"]),
                r["window_start"],
                r["window_end"],
                r["occupancy"],
                int(r["busy_sum"]),
                int(r["piles_sum"]),
                int(r["station_cnt"]),
                int(r["msg_cnt"]),
                int(r["outlier_cnt"]),
                int(r["dropped_src_cnt"]),
                ingest_ts,
                now,
                int(latency_ms),
                int(batch_id),
            )
        )
    return rows


def build_realtime_snapshot_rows(
    clean_path: Path = DEFAULT_CLEAN,
    station_list_path: Path = DEFAULT_STATIONS,
    windows: int = 2,
    date_filter: str | None = None,
) -> list[tuple]:
    """提取演示子集最后 N 个有效 15 分钟时间段，用于空库初始化 rt_zone_occupancy 快照。"""
    all_windows = list(iter_demo_windows(clean_path, station_list_path, date_filter=date_filter))
    if not all_windows:
        return []
    picked = all_windows[-max(1, int(windows)) :]
    now = dt.datetime.now()
    rows: list[tuple] = []
    for idx, (_ws, _we, recs) in enumerate(picked, 1):
        rows.extend(to_db_rows(recs, batch_id=idx, now=now, latency_ms=280 + idx * 45))
    return rows


def _mysql_connect():
    import pymysql

    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "ev"),
        password=os.getenv("MYSQL_PASSWORD", "evpass"),
        database=os.getenv("MYSQL_DB", os.getenv("MYSQL_DATABASE", "sz_ev")),
        charset="utf8mb4",
        init_command="SET time_zone='+08:00'",
        autocommit=False,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--clean", type=Path, default=DEFAULT_CLEAN, help="dwd_station_5min_clean.csv(.gz) 路径")
    ap.add_argument("--stations", type=Path, default=DEFAULT_STATIONS, help="demo_station_list.csv 路径")
    ap.add_argument("--date", help="只回放指定日期（YYYY-MM-DD），默认回放全部有效日期")
    ap.add_argument("--interval", type=float, default=3.0, help="每个 15 分钟时间段写入间隔秒数（默认 3）")
    ap.add_argument("--limit", type=int, default=0, help="最多写入多少个时间段后退出（0=不限）")
    ap.add_argument("--loop", action="store_true", help="回放完后循环重播（适合长期展示）")
    ap.add_argument("--clear", action="store_true", help="启动前先清空 rt_zone_occupancy（适合从头演示）")
    ap.add_argument("--dry-run", action="store_true", help="只打印聚合结果，不连接 MySQL")
    args = ap.parse_args(argv)

    windows = list(iter_demo_windows(args.clean, args.stations, date_filter=args.date))
    if not windows:
        print("[realtime_replay_mysql] 没有可回放的 15 分钟时间段数据", file=sys.stderr)
        return 1

    conn = None if args.dry_run else _mysql_connect()
    try:
        if conn is not None and args.clear:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE rt_zone_occupancy")
            conn.commit()

        batch_id = 0
        while True:
            for ws, we, recs in windows:
                batch_id += 1
                rows = to_db_rows(recs, batch_id=batch_id, now=dt.datetime.now(), latency_ms=260 + (batch_id % 7) * 35)
                if conn is not None:
                    with conn.cursor() as cur:
                        cur.executemany(UPSERT_SQL, rows)
                    conn.commit()
                avg_occ = sum(r["occupancy"] for r in recs) / len(recs)
                mode = "dry-run" if args.dry_run else "mysql"
                print(
                    f"[realtime_replay_mysql:{mode}] batch={batch_id} "
                    f"{ws.strftime('%Y-%m-%d %H:%M')}~{we.strftime('%H:%M')} zones={len(recs)} avg_occ={avg_occ:.3f}",
                    flush=True,
                )
                if args.limit and batch_id >= args.limit:
                    return 0
                if args.interval > 0:
                    time.sleep(args.interval)
            if not args.loop:
                break
    finally:
        if conn is not None:
            conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
