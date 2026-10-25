#!/usr/bin/env python3
"""不起 Spark / 不依赖 Hive metastore：直接读 ads Parquet，回写 MySQL（与 export_ads_mysql.py 同表同列）。

来源：
  $SZ_WAREHOUSE/ads.db/{zone_occupancy_hour,baseline_naive,city_overview,zone_daily}/dt=D/*.parquet
  $SZ_WAREHOUSE/ads.db/quality_summary/*.parquet（整表快照）
  $SZ_EV_LAKE/ods/station_info/*.parquet → dim_station
  $SZ_EV_LAKE/dwd/quality_station/run=*（累计卡死/工单建议，口径同 ads_metrics.quality_station_path）
注意：Hive 表 ts_hour 是 Spark 写的 INT96（UTC 存储），这里按 SZ_SPARK_TZ（默认 Asia/Shanghai）还原成本地时间。
load_kw_est / power_coverage 在这里用 pandas 按 ods/station_info 重算（口径同 ads_metrics.ZONE_POWER_SQL），不取 Parquet 里的旧值。
用法：
  export_ads_parquet.py --start 2022-09-01 --end 2023-02-28 [--exclude 2022-09-02 ...] [--dry-run]
  export_ads_parquet.py --only quality_summary --only dim_station        # 只刷这两张小表
  export_ads_parquet.py --only load_kw --start ... --end ... --exclude 2022-09-02
  export_ads_parquet.py --only site_suggest      # 小紫的 ads/zone_site_suggest Parquet → ads_zone_site_suggest（全量替换）
      # 只 UPDATE ads_zone_occupancy_hour 的 load_kw_est、power_coverage 两列，其他列/其他表不动
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
    "ads_zone_occupancy_hour": ("zone_occupancy_hour", ["taz_id", "ts_hour", "occupancy", "load_kw_est", "power_coverage"],
                                "DATE(ts_hour)", {"occupancy": 6, "load_kw_est": 2, "power_coverage": 6}),
    "ads_baseline_naive": ("baseline_naive", ["taz_id", "ts_hour", "y_true", "y_hat_lag1", "abs_err"], "DATE(ts_hour)",
                           {"y_true": 6, "y_hat_lag1": 6, "abs_err": 6}),
    "ads_zone_daily": ("zone_daily", ["dt", "taz_id", "avg_occupancy", "station_cnt", "pile_cnt", "center_lon",
                                      "center_lat"], "dt", {"avg_occupancy": 6, "center_lon": 6, "center_lat": 6}),
}


DIM_COLS = ["station_id", "taz_id", "longitude", "latitude", "slow_count", "fast_count", "total_piles", "rated_power_kw",
            "rated_power_kw_total", "dc_pile_cnt", "ac_pile_cnt", "pile_cnt_from_power"]
# 表结构缺列时补上（同 sql/migrations/20261025_station_power.sql）
SCHEMA_ADD = {
    "dim_station": [("rated_power_kw_total", "DECIMAL(12,2) NULL"), ("dc_pile_cnt", "INT NULL"), ("ac_pile_cnt", "INT NULL"),
                    ("pile_cnt_from_power", "INT NULL")],
    "ads_zone_occupancy_hour": [("power_coverage", "DECIMAL(7,6) NULL")],
}


def station_info():
    return pq.ParquetDataset(str(LAKE / "ods" / "station_info")).read().to_pandas()


def zone_power(t) -> "pd.DataFrame":
    """片区功率，口径同 ads_metrics.ZONE_POWER_SQL：
    kw_sum = 片区内 rated_power_kw_total 非空站的功率之和（全空 → NaN/NULL）；
    power_coverage = 有功率数据站的桩数 / 片区全部桩数（桩数 = charge_count/total_piles）。"""
    import pandas as pd
    taz = next(c for c in ("taz_id", "TAZID") if c in t)
    piles_c = next((c for c in ("charge_count", "total_piles") if c in t), None)
    kw = t["rated_power_kw_total"] if "rated_power_kw_total" in t else pd.Series(float("nan"), index=t.index)
    piles = t[piles_c].fillna(0).astype(float) if piles_c else pd.Series(0.0, index=t.index)
    d = pd.DataFrame({"taz_id": t[taz].astype(str), "kw": kw.astype(float), "piles": piles,
                      "piles_kw": piles.where(kw.notna(), 0.0)})
    g = d.groupby("taz_id")
    out = pd.DataFrame({"kw_sum": g["kw"].sum(min_count=1), "piles": g["piles"].sum(), "piles_kw": g["piles_kw"].sum()})
    out["power_coverage"] = (out.piles_kw / out.piles.where(out.piles > 0)).astype(float)
    return out[["kw_sum", "power_coverage"]].reset_index()


_ZP = None


def _zone_power_cached():
    global _ZP
    if _ZP is None:
        _ZP = zone_power(station_info())
    return _ZP


def apply_zone_power(df, zp):
    """df 含 taz_id/occupancy（未舍入的原始 double）→ 填 load_kw_est、power_coverage。"""
    df = df.drop(columns=[c for c in ("load_kw_est", "power_coverage") if c in df]).merge(zp, on="taz_id", how="left")
    df["load_kw_est"] = df["occupancy"] * df["kw_sum"]
    return df.drop(columns=["kw_sum"])


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
    if "load_kw_est" in cols:
        df["taz_id"] = df["taz_id"].astype(str)
        df = apply_zone_power(df, _zone_power_cached())
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
    t = station_info()
    pick = lambda *ns: next((t[n] for n in ns if n in t), None)  # noqa: E731
    out = []
    rp = pick("rated_power_kw", "rated_power_kw_total")  # 旧列 rated_power_kw 没有时用站总额定功率
    for i in range(len(t)):
        g = lambda s: None if s is None or s.iloc[i] != s.iloc[i] else s.iloc[i]  # noqa: E731
        r2 = lambda v: None if v is None else round(float(v), 2)  # noqa: E731
        out.append((str(t.station_id.iloc[i]), str(g(pick("taz_id", "TAZID"))), round(float(g(pick("longitude"))), 6),
                    round(float(g(pick("latitude"))), 6), g(pick("slow_count")), g(pick("fast_count")),
                    g(pick("charge_count", "total_piles")), r2(g(rp)), r2(g(pick("rated_power_kw_total"))),
                    g(pick("dc_pile_cnt")), g(pick("ac_pile_cnt")), g(pick("pile_cnt_from_power"))))
    return [tuple(None if v is None else (v.item() if hasattr(v, "item") else v) for v in r) for r in out]


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for t, cols in SCHEMA_ADD.items():
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s", (t,))
            have = {r[0].lower() for r in cur.fetchall()}
            add = [f"ADD COLUMN {c} {ty}" for c, ty in cols if c not in have]
            if add:
                cur.execute(f"ALTER TABLE {t} {', '.join(add)}")
                print(f"[export_ads_parquet] ALTER {t}: {', '.join(add)}", flush=True)
    conn.commit()


def update_load_kw(conn, dates: list[str], missing: dict) -> None:
    """只 UPDATE ads_zone_occupancy_hour.load_kw_est / power_coverage（按主键 taz_id+ts_hour），一天一个事务。"""
    ht, cols, _, rnd = PART_TABLES["ads_zone_occupancy_hour"]
    t0, n, changed = time.time(), 0, 0
    cur = conn.cursor() if conn else None
    if cur:
        cur.execute("CREATE TEMPORARY TABLE IF NOT EXISTS tmp_zoh_kw (taz_id VARCHAR(32) NOT NULL, ts_hour DATETIME NOT NULL, "
                    "load_kw_est DECIMAL(12,2) NULL, power_coverage DECIMAL(7,6) NULL, PRIMARY KEY (taz_id, ts_hour))")
    for d in dates:
        df = read_part(ht, cols, rnd, d)
        if df is None:
            missing.setdefault("ads_zone_occupancy_hour(load_kw)", []).append(d); continue
        rows = list(df[["taz_id", "ts_hour", "load_kw_est", "power_coverage"]].itertuples(index=False, name=None))
        n += len(rows)
        if not cur:
            continue
        cur.execute("DELETE FROM tmp_zoh_kw")
        for j in range(0, len(rows), 5000):
            cur.executemany("INSERT INTO tmp_zoh_kw VALUES (%s,%s,%s,%s)", rows[j:j + 5000])
        changed += cur.execute("UPDATE ads_zone_occupancy_hour z JOIN tmp_zoh_kw k ON z.taz_id=k.taz_id AND z.ts_hour=k.ts_hour "
                               "SET z.load_kw_est=k.load_kw_est, z.power_coverage=k.power_coverage")
        conn.commit()
    print(f"[export_ads_parquet] ads_zone_occupancy_hour load_kw_est/power_coverage: computed={n} updated={changed} "
          f"days={len(dates) - len(missing.get('ads_zone_occupancy_hour(load_kw)', []))} {time.time() - t0:.2f}s", flush=True)


# ---- 选址/加桩建议：$SZ_EV_LAKE/ads/zone_site_suggest → ads_zone_site_suggest（全量替换）----
SITE_COLS = ["run_dt", "taz_id", "center_lon", "center_lat", "station_cnt", "piles", "rated_kw", "avg_occupancy",
             "peak_occupancy", "busy_hours_ratio", "demand_score", "supply_score", "gap_score", "suggest_add_piles",
             "rank", "function_tag", "reason"]
# Parquet 里可能的别名 → 表列（schema 未定，宽松映射）
SITE_ALIAS = {"dt": "run_dt", "run_date": "run_dt", "ds": "run_dt", "lon": "center_lon", "lat": "center_lat",
              "longitude": "center_lon", "latitude": "center_lat", "station_count": "station_cnt",
              "pile_cnt": "piles", "total_piles": "piles", "piles_sum": "piles", "rated_kw_total": "rated_kw",
              "rated_power_kw_total": "rated_kw", "occupancy_avg": "avg_occupancy", "occupancy_peak": "peak_occupancy",
              "add_piles": "suggest_add_piles", "suggest_piles": "suggest_add_piles", "priority": "rank",
              "zone_rank": "rank", "tag": "function_tag", "func_tag": "function_tag", "reason_text": "reason"}
SITE_INT = {"station_cnt", "piles", "suggest_add_piles", "rank"}
SITE_RND = {"center_lon": 6, "center_lat": 6, "rated_kw": 2, "avg_occupancy": 6, "peak_occupancy": 6,
            "busy_hours_ratio": 6, "demand_score": 4, "supply_score": 4, "gap_score": 4}


def site_suggest_frame(path=None):
    """读 Parquet（目录或文件，支持 hive 分区如 run_dt=…），按 SITE_COLS 宽松映射。返回 (DataFrame, 缺失列, 多余列)。"""
    import pandas as pd
    path = Path(path or LAKE / "ads" / "zone_site_suggest")
    if not path.exists():
        raise SystemExit(f"找不到 {path}（小紫的 zone_site_suggest 还没生成？）")
    raw = pd.read_parquet(path)
    ren, extras = {}, []
    for c in raw.columns:
        k = str(c).strip().lower()
        t = k if k in SITE_COLS else SITE_ALIAS.get(k)
        if t and t not in ren.values():
            ren[c] = t
        else:
            extras.append(str(c))
    df = raw.rename(columns=ren)[list(ren.values())]
    missing = [c for c in SITE_COLS if c not in df.columns]
    for c in missing:
        df[c] = None
    df = df[SITE_COLS].copy()
    if df["run_dt"].isna().all():
        df["run_dt"] = dt.date.today()
    df["run_dt"] = pd.to_datetime(df["run_dt"].astype(str)).dt.date
    df["taz_id"] = df["taz_id"].astype(str)
    if df["rank"].isna().all() and df["gap_score"].notna().any():   # 没给排名就按缺口分倒序排
        df["rank"] = df.groupby("run_dt")["gap_score"].rank(ascending=False, method="first")
    return df, missing, extras


def site_suggest_rows(path=None) -> list[tuple]:
    import pandas as pd
    df, missing, extras = site_suggest_frame(path)
    if missing:
        print(f"[export_ads_parquet] site_suggest: Parquet 缺列 {missing}，写 NULL", flush=True)
    if extras:
        print(f"[export_ads_parquet] site_suggest: 未识别的列 {extras}，已忽略", flush=True)

    def cell(c, v):
        if v is None or (not isinstance(v, str) and pd.isna(v)):
            return None
        if c in SITE_INT:
            return int(round(float(v)))
        if c in SITE_RND:
            return round(float(v), SITE_RND[c])
        return v
    return [tuple(cell(c, v) for c, v in zip(SITE_COLS, r)) for r in df.itertuples(index=False, name=None)]


def load_site_suggest(conn, path=None) -> int:
    rows = site_suggest_rows(path)
    if conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ads_zone_site_suggest")
            cols = ",".join(f"`{c}`" for c in SITE_COLS)
            cur.executemany(f"INSERT INTO ads_zone_site_suggest ({cols}) VALUES ({','.join(['%s'] * len(SITE_COLS))})", rows)
        conn.commit()
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ds", "--dt", dest="ds")
    p.add_argument("--start"); p.add_argument("--end")
    p.add_argument("--exclude", action="append", help="跳过的日期，可重复")
    p.add_argument("--site-path", help="site_suggest 的 Parquet 路径（默认 $SZ_EV_LAKE/ads/zone_site_suggest）")
    p.add_argument("--dry-run", action="store_true", help="只读 Parquet 统计行数，不连 MySQL")
    p.add_argument("--only", action="append", choices=["quality_summary", "dim_station", "load_kw", "site_suggest"],
                   help="只做这几项（可重复）；不给则全量导出")
    a = p.parse_args()
    only = set(a.only or [])
    need_dates = not only or "load_kw" in only
    if need_dates and not a.ds and not (a.start and a.end):
        raise SystemExit("需要 --ds 或 --start/--end")
    dates = dates_from(a) if need_dates else []
    conn = None
    if not a.dry_run:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DB", os.getenv("MYSQL_DATABASE", "sz_ev")),
                               charset="utf8mb4", autocommit=False, init_command="SET time_zone='+08:00'")
        ensure_schema(conn)
    missing = {}
    if "site_suggest" in only:
        t0 = time.time()
        n = load_site_suggest(conn, a.site_path)
        print(f"[export_ads_parquet] ads_zone_site_suggest: rows={n} (全量替换) {time.time() - t0:.2f}s", flush=True)
    if "load_kw" in only:
        update_load_kw(conn, dates, missing)
    for mt, (ht, cols, dexpr, rnd) in (PART_TABLES.items() if not only else []):
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
    for key, mt, rows_fn, cols, mode in [
        ("dim_station", "dim_station", station_rows, DIM_COLS, "REPLACE"),
        ("quality_summary", "ads_quality_summary", quality_rows, ["metric", "value", "note"], "SNAPSHOT"),
    ]:
        if only and key not in only:
            continue
        rows = rows_fn()
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
