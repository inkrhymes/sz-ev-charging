#!/usr/bin/env python3
"""不起 Spark / 不依赖 Hive metastore：直接读 ads Parquet，回写 MySQL（与 export_ads_mysql.py 同表同列）。

来源：
  $SZ_WAREHOUSE/ads.db/{zone_occupancy_hour,baseline_naive,city_overview,zone_daily}/dt=D/*.parquet
  $SZ_WAREHOUSE/ads.db/quality_summary/*.parquet（整表快照）
  $SZ_EV_LAKE/ods/station_info/*.parquet → dim_station
  $SZ_EV_LAKE/dwd/quality_station/run=*（累计卡死/工单建议，口径同 ads_metrics.quality_station_path）
注意：Hive 表 ts_hour 是 Spark 写的 INT96（UTC 存储），这里按 SZ_SPARK_TZ（默认 Asia/Shanghai）还原成本地时间。
用法：
  export_ads_parquet.py --start 2022-09-01 --end 2023-02-28 [--exclude 2022-09-02 ...] [--dry-run]
幂等：每张分区表只 DELETE 本次要写的日期（被 --exclude 的日期原样保留），再批量 INSERT，单表一个事务。
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))
WAREHOUSE = Path(os.environ.get("SZ_WAREHOUSE", ROOT / "data" / "warehouse"))
SPARK_TZ = os.environ.get("SZ_SPARK_TZ", "Asia/Shanghai")
REPAIR_STUCK_DAYS = 30

# MySQL 表 -> (Hive 目录, 列, 日期删除表达式, 舍入)
PART_TABLES = {
    "ads_city_overview": ("city_overview", ["dt", "station_cnt", "zone_cnt", "avg_occupancy", "peak_hour", "stuck_cnt",
                                            "high_missing_cnt"], "dt", {"avg_occupancy": 6}),
    "ads_zone_occupancy_hour": ("zone_occupancy_hour", ["taz_id", "ts_hour", "occupancy", "load_kw_est"], "DATE(ts_hour)",
                                {"occupancy": 6, "load_kw_est": 2}),
    "ads_baseline_naive": ("baseline_naive", ["taz_id", "ts_hour", "y_true", "y_hat_lag1", "abs_err"], "DATE(ts_hour)",
                           {"y_true": 6, "y_hat_lag1": 6, "abs_err": 6}),
    "ads_zone_daily": ("zone_daily", ["dt", "taz_id", "avg_occupancy", "station_cnt", "pile_cnt", "center_lon",
                                      "center_lat"], "dt", {"avg_occupancy": 6, "center_lon": 6, "center_lat": 6}),
}


def dates_from(a) -> list[str]:
    if a.ds:
        ds = [a.ds]
    else:
        s, e = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
        ds = [(s + dt.timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
    ex = set(a.exclude or [])
    return [d for d in ds if d not in ex]


def read_part(table: str, cols: list[str], rnd: dict, d: str):
    p = WAREHOUSE / "ads.db" / table / f"dt={d}"
    files = sorted(p.glob("*.parquet"))
    if not files:
        return None
    df = pq.ParquetDataset([str(f) for f in files]).read().to_pandas()
    if "ts_hour" in df:
        df["ts_hour"] = (df["ts_hour"].dt.tz_localize("UTC").dt.tz_convert(SPARK_TZ).dt.tz_localize(None)
                         .dt.strftime("%Y-%m-%d %H:%M:%S"))
    if "dt" in cols:
        df["dt"] = d
    for c, n in rnd.items():
        df[c] = df[c].round(n)
    df = df[cols].astype(object).where(df[cols].notna(), None)
    return df


def quality_station_path() -> str | None:
    """同 ads_metrics.quality_station_path（复制一份，避免导入 pyspark）。"""
    env = os.environ.get("SZ_QUALITY_STATION")
    if env:
        return env if Path(env).exists() else None
    best, span = None, -1
    for d in (LAKE / "dwd" / "quality_station").glob("run=*_*"):
        try:
            a, b = d.name[4:].split("_")
            n = (dt.date.fromisoformat(b) - dt.date.fromisoformat(a)).days
        except ValueError:
            continue
        if n > span and any(d.glob("*.parquet")):
            best, span = str(d), n
    return best


def quality_rows() -> list[tuple]:
    files = sorted((WAREHOUSE / "ads.db" / "quality_summary").glob("*.parquet"))
    rows = {}
    for f in files:
        for r in pq.read_table(f).to_pylist():
            rows[r["metric"]] = (r["metric"], None if r["value"] is None else round(r["value"], 6), r["note"])
    # 旧快照（c1b9b10 之前构建）里 stuck_station_cnt 是“全程卡死”口径：改名，避免与新口径混淆
    if "stuck_whole_window_cnt" not in rows and "stuck_station_cnt" in rows:
        _, v, _ = rows.pop("stuck_station_cnt")
        rows["stuck_whole_window_cnt"] = ("stuck_whole_window_cnt", v, "整个已入仓窗口内读数从没变过的站（全程卡死）")
    qs = quality_station_path()
    if qs:
        q = pq.ParquetDataset(qs).read(columns=["stuck_days", "ticket_suggest"]).to_pandas()
        T = REPAIR_STUCK_DAYS
        for m, v, note in [
            ("stuck_station_cnt", (q.stuck_days >= T).sum(), f"累计卡死≥{T}天的站（口径同 dq_alert，读 dwd/quality_station.stuck_days）"),
            ("stuck_any_day_cnt", (q.stuck_days > 0).sum(), "至少有一天卡死的站"),
            ("repair_ticket_cnt", (q.ticket_suggest == "repair").sum(), "建议维修工单的站（dwd/quality_station.ticket_suggest=repair）"),
            ("inspect_ticket_cnt", (q.ticket_suggest == "inspect").sum(), "建议巡检工单的站（ticket_suggest=inspect）"),
        ]:
            rows[m] = (m, float(v), note)
    return list(rows.values())


def station_rows() -> list[tuple]:
    t = pq.ParquetDataset(str(LAKE / "ods" / "station_info")).read().to_pandas()
    pick = lambda *ns: next((t[n] for n in ns if n in t), None)  # noqa: E731
    out = []
    rp = pick("rated_power_kw")
    for i in range(len(t)):
        g = lambda s: None if s is None or s.iloc[i] != s.iloc[i] else s.iloc[i]  # noqa: E731
        out.append((str(t.station_id.iloc[i]), str(g(pick("taz_id", "TAZID"))), round(float(g(pick("longitude"))), 6),
                    round(float(g(pick("latitude"))), 6), g(pick("slow_count")), g(pick("fast_count")),
                    g(pick("charge_count", "total_piles")), None if rp is None else g(rp)))
    return [tuple(None if v is None else (v.item() if hasattr(v, "item") else v) for v in r) for r in out]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ds", "--dt", dest="ds")
    p.add_argument("--start"); p.add_argument("--end")
    p.add_argument("--exclude", action="append", help="跳过的日期，可重复")
    p.add_argument("--dry-run", action="store_true", help="只读 Parquet 统计行数，不连 MySQL")
    a = p.parse_args()
    if not a.ds and not (a.start and a.end):
        raise SystemExit("需要 --ds 或 --start/--end")
    dates = dates_from(a)
    conn = None
    if not a.dry_run:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DB", os.getenv("MYSQL_DATABASE", "sz_ev")),
                               charset="utf8mb4", autocommit=False, init_command="SET time_zone='+08:00'")
    missing = {}
    for mt, (ht, cols, dexpr, rnd) in PART_TABLES.items():
        t0, n = time.time(), 0
        cur = conn.cursor() if conn else None
        for i in range(0, len(dates), 31):
            chunk = dates[i:i + 31]
            if cur:
                cur.execute(f"DELETE FROM {mt} WHERE {dexpr} IN ({','.join(['%s'] * len(chunk))})", chunk)
            for d in chunk:
                df = read_part(ht, cols, rnd, d)
                if df is None:
                    missing.setdefault(mt, []).append(d); continue
                rows = list(df.itertuples(index=False, name=None))
                if cur:
                    ph = ",".join(["%s"] * len(cols))
                    for j in range(0, len(rows), 5000):
                        cur.executemany(f"INSERT INTO {mt} ({','.join(cols)}) VALUES ({ph})", rows[j:j + 5000])
                n += len(rows)
        if conn:
            conn.commit()
        print(f"[export_ads_parquet] {mt}: rows={n} days={len(dates) - len(missing.get(mt, []))} {time.time() - t0:.2f}s", flush=True)
    for mt, rows, cols, mode in [
        ("dim_station", station_rows(), ["station_id", "taz_id", "longitude", "latitude", "slow_count", "fast_count",
                                         "total_piles", "rated_power_kw"], "REPLACE"),
        ("ads_quality_summary", quality_rows(), ["metric", "value", "note"], "SNAPSHOT"),
    ]:
        t0 = time.time()
        if conn:
            with conn.cursor() as cur:
                if mode == "SNAPSHOT":
                    cur.execute(f"DELETE FROM {mt}")
                verb = "REPLACE" if mode == "REPLACE" else "INSERT"
                cur.executemany(f"{verb} INTO {mt} ({','.join(cols)}) VALUES ({','.join(['%s'] * len(cols))})", rows)
            conn.commit()
        print(f"[export_ads_parquet] {mt}: rows={len(rows)} {time.time() - t0:.2f}s", flush=True)
        if mt == "ads_quality_summary":
            for r in rows:
                print("   ", r[0], r[1])
    for mt, ds in missing.items():
        print(f"[export_ads_parquet] WARN {mt} 缺分区 {len(ds)} 天: {ds[:10]}{'...' if len(ds) > 10 else ''}")
    if conn:
        conn.close()


if __name__ == "__main__":
    main()
