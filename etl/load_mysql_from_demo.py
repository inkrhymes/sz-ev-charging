#!/usr/bin/env python3
"""Load data/demo ods+dwd CSVs into MySQL and backfill all ads/ops/rt demo tables (小黑)."""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import math
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pymysql

try:
    from etl.realtime_replay_mysql import UPSERT_SQL as RT_UPSERT_SQL
    from etl.realtime_replay_mysql import build_realtime_snapshot_rows
    from etl.zone_profile import (
        ADD_GAP,
        ADD_P90,
        BUSY_OCC,
        EVENING_H,
        MIN_PILES,
        NEAR_M,
        TARGET_OCC,
        WATCH_GAP,
        nearest_district,
    )
except ImportError:  # 直接 python etl/load_mysql_from_demo.py 执行时 sys.path[0] 为 etl/
    from realtime_replay_mysql import UPSERT_SQL as RT_UPSERT_SQL
    from realtime_replay_mysql import build_realtime_snapshot_rows
    from zone_profile import (
        ADD_GAP,
        ADD_P90,
        BUSY_OCC,
        EVENING_H,
        MIN_PILES,
        NEAR_M,
        TARGET_OCC,
        WATCH_GAP,
        nearest_district,
    )

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
ODS = DEMO / "ods"
DWD = DEMO / "dwd"
FORECAST_PRED = ROOT / "forecast" / "predictions.csv"

CFG = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "ev"),
    password=os.getenv("MYSQL_PASSWORD", "evpass"),
    database=os.getenv("MYSQL_DB", os.getenv("MYSQL_DATABASE", "sz_ev")),
    charset="utf8mb4",
    local_infile=True,
)


DEMO_LIST = ROOT / "data" / "demo" / "demo_station_list.csv"


def conn():
    return pymysql.connect(**CFG)


def _open_csv(path: Path):
    if path.is_file():
        return path.open(encoding="utf-8")
    gz = Path(f"{path}.gz")
    if gz.is_file():
        return gzip.open(gz, "rt", encoding="utf-8")
    raise FileNotFoundError(path)


def load_csv(cur, table: str, path: Path, cols: list[str]):
    with _open_csv(path) as f:
        r = csv.DictReader(f)
        rows = []
        for row in r:
            vals = []
            for c in cols:
                v = row.get(c)
                vals.append(None if v in ("", None) else v)
            rows.append(vals)
            if len(rows) >= 2000:
                cur.executemany(
                    f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})",
                    rows,
                )
                rows = []
        if rows:
            cur.executemany(
                f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})",
                rows,
            )


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def build_site_suggest_rows(
    zone_hour_path: Path = DWD / "dwd_zone_hour.csv",
    station_list_path: Path = DEMO_LIST,
    run_dt: str | None = None,
) -> list[tuple]:
    """用与 etl/zone_profile.py 一致的经验贝叶斯 + 缺口分算法，基于 demo 数据计算选址补桩建议。"""
    zh = pd.read_csv(zone_hour_path, dtype={"taz_id": str})
    if zh.empty:
        return []
    zh["ts"] = pd.to_datetime(zh["ts_hour"])
    zh["hour"] = zh["ts"].dt.hour
    zh["occupancy"] = pd.to_numeric(zh["occupancy_avg"], errors="coerce")
    zh = zh.dropna(subset=["occupancy"])
    if zh.empty:
        return []

    ev = zh[zh["hour"].isin(EVENING_H)]
    occ = zh.groupby("taz_id")["occupancy"].mean().rename("occ_avg").to_frame()
    occ["occ_evening_p90"] = ev.groupby("taz_id")["occupancy"].quantile(0.9)
    occ["occ_evening_p90"] = occ["occ_evening_p90"].fillna(occ["occ_avg"])
    hp = zh.groupby(["taz_id", "hour"])["occupancy"].mean().unstack()
    occ["peak_hour_occ"] = hp.max(axis=1)
    occ["busy_hours_ratio"] = zh.assign(b=zh["occupancy"] >= BUSY_OCC).groupby("taz_id")["b"].mean()

    si = pd.read_csv(station_list_path, dtype={"sid": str, "TAZID": str})
    si = si.rename(columns={"sid": "station_id", "TAZID": "taz_id"})
    si["longitude"] = pd.to_numeric(si["longitude"], errors="coerce")
    si["latitude"] = pd.to_numeric(si["latitude"], errors="coerce")
    si["charge_count"] = pd.to_numeric(si["charge_count"], errors="coerce").fillna(0).astype(int)

    coords = list(zip(si["longitude"], si["latitude"], si["charge_count"]))
    near_piles = []
    for lon1, lat1, _ in coords:
        total = 0
        for lon2, lat2, cnt in coords:
            if pd.notna(lon1) and pd.notna(lat1) and pd.notna(lon2) and pd.notna(lat2):
                if _haversine_m(lon1, lat1, lon2, lat2) <= NEAR_M:
                    total += cnt
        near_piles.append(float(total))
    si["piles_within_2km"] = near_piles

    st = si.groupby("taz_id").agg(
        station_cnt=("station_id", "size"),
        pile_cnt=("charge_count", "sum"),
        center_lon=("longitude", "mean"),
        center_lat=("latitude", "mean"),
        piles_within_2km=("piles_within_2km", "mean"),
    )
    z = occ.join(st, how="inner").reset_index()
    if z.empty:
        return []

    z["district"] = nearest_district(z["center_lon"], z["center_lat"])

    for c in ("occ_avg", "occ_evening_p90", "peak_hour_occ"):
        n = z["pile_cnt"].clip(lower=1).astype(float)
        m = float(np.average(z[c], weights=n))
        tau2 = float(np.average((z[c] - m) ** 2, weights=n)) - float(np.mean(m * (1 - m) / n))
        k = m * (1 - m) / tau2 - 1 if tau2 > 0 else float(n.median())
        k = max(k, 0.0)
        z[c + "_shr"] = (n * z[c] + k * m) / (n + k)

    pct = lambda s: s.rank(pct=True) * 100.0
    small = z["pile_cnt"] < MIN_PILES
    z["demand_score"] = (0.6 * pct(z["occ_evening_p90_shr"]) + 0.4 * pct(z["occ_avg_shr"])).round(1)
    z["supply_score"] = pct(z["piles_within_2km"]).round(1)
    z["gap_score"] = (0.7 * z["demand_score"] + 0.3 * (100.0 - z["supply_score"])).round(1)

    need = np.ceil(z["pile_cnt"] * z["occ_evening_p90_shr"] / TARGET_OCC - z["pile_cnt"]).clip(lower=0)
    add = (z["gap_score"] >= ADD_GAP) & (z["occ_evening_p90_shr"] >= ADD_P90) & (~small)
    z["suggest_add_piles"] = np.where(add, np.maximum(need, 1), 0).astype(int)

    reasons = []
    for r in z.itertuples():
        base = (
            f"晚高峰（18–23 点）忙的时候占用率到 {r.occ_evening_p90:.0%}，全天平均 {r.occ_avg:.0%}，"
            f"{r.busy_hours_ratio:.0%} 的小时占用率超过 {BUSY_OCC:.0%}；区内 {r.pile_cnt} 根桩，"
            f"周边 2 公里平均 {r.piles_within_2km:.0f} 根（按桩数校正后晚高峰约 {r.occ_evening_p90_shr:.0%}）"
        )
        if r.pile_cnt < MIN_PILES:
            reasons.append(base + f"。桩数太少，样本不够（少于 {MIN_PILES} 根），先观察。")
        elif r.suggest_add_piles > 0:
            reasons.append(base + f"。晚上经常排满、周边桩也不多，建议加 {r.suggest_add_piles} 根桩（加完后晚高峰占用率约降到 {TARGET_OCC:.0%}）。")
        elif r.gap_score >= WATCH_GAP or r.occ_evening_p90_shr >= ADD_P90:
            reasons.append(base + "。有时比较紧张，先观察，看后面几个月会不会更忙。")
        else:
            reasons.append(base + "。桩够用，维持现状。")
    z["reason"] = reasons

    z = z.sort_values(["gap_score", "demand_score"], ascending=False).reset_index(drop=True)
    z["rank"] = np.arange(1, len(z) + 1, dtype=int)
    run_dt = run_dt or str(zh["ts"].max().date())

    rows: list[tuple] = []
    for r in z.itertuples():
        rows.append(
            (
                run_dt,
                str(r.taz_id),
                round(float(r.center_lon), 6),
                round(float(r.center_lat), 6),
                int(r.station_cnt),
                int(r.pile_cnt),
                None,
                round(float(r.occ_avg), 4),
                round(float(r.peak_hour_occ), 4),
                round(float(r.busy_hours_ratio), 4),
                round(float(r.demand_score), 4),
                round(float(r.supply_score), 4),
                round(float(r.gap_score), 4),
                int(r.suggest_add_piles),
                int(r.rank),
                str(r.district),
                r.reason,
            )
        )
    return rows


def build_forecast_rows(
    pred_path: Path = FORECAST_PRED,
    model_version: str = "lgbm_demo_v1",
) -> tuple[list[tuple], list[tuple]]:
    """读取仓库自带 forecast/predictions.csv，生成 ads_forecast_zone_hour 与 ads_forecast_metric_daily 行。"""
    if not pred_path.is_file():
        return [], []

    zone_hour_rows: list[tuple] = []
    by_day: dict[str, list[tuple[str, str, float, float, float]]] = defaultdict(list)
    truth_lookup: dict[tuple[str, str], float] = {}
    min_ts: str | None = None

    with pred_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            taz = (r.get("taz_id") or "").strip()
            ts = (r.get("ts") or "").strip()
            if not taz or not ts:
                continue
            try:
                y = float(r["y"])
                yhat_lgbm = float(r["y_hat_lgbm"])
                yhat_lag1 = float(r["y_hat_lag1"])
            except (KeyError, TypeError, ValueError):
                continue
            if min_ts is None or ts < min_ts:
                min_ts = ts
            truth_lookup[(taz, ts)] = y
            by_day[ts[:10]].append((taz, ts, y, yhat_lgbm, yhat_lag1))

    if not by_day or min_ts is None:
        return [], []

    trained_until = (dt.datetime.strptime(min_ts[:19], "%Y-%m-%d %H:%M:%S") - dt.timedelta(hours=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    for ds in sorted(by_day):
        for taz, ts, _y, yhat_lgbm, yhat_lag1 in by_day[ds]:
            zone_hour_rows.append(
                (taz, ts[:19], round(yhat_lgbm, 6), round(yhat_lag1, 6), model_version, trained_until)
            )

    metric_daily_rows: list[tuple] = []
    for ds in sorted(by_day):
        items = by_day[ds]
        n = len(items)
        ae_lgbm = [abs(y - yl) for _, _, y, yl, _ in items]
        se_lgbm = [(y - yl) ** 2 for _, _, y, yl, _ in items]
        ae_lag1 = [abs(y - y1) for _, _, y, _, y1 in items]
        se_lag1 = [(y - y1) ** 2 for _, _, y, _, y1 in items]
        ae_lag24 = []
        for taz, ts, y, _, _ in items:
            prev_ts = (dt.datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S") - dt.timedelta(hours=24)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if (taz, prev_ts) in truth_lookup:
                ae_lag24.append(abs(y - truth_lookup[(taz, prev_ts)]))

        mae_lgbm = round(sum(ae_lgbm) / n, 6)
        rmse_lgbm = round(math.sqrt(sum(se_lgbm) / n), 6)
        mae_lag1 = round(sum(ae_lag1) / n, 6)
        rmse_lag1 = round(math.sqrt(sum(se_lag1) / n), 6)
        mae_lag24 = round(sum(ae_lag24) / len(ae_lag24), 6) if ae_lag24 else None
        drift = 1 if mae_lgbm > mae_lag1 else 0
        metric_daily_rows.append((ds, n, mae_lgbm, rmse_lgbm, mae_lag1, rmse_lag1, mae_lag24, model_version, drift))

    return zone_hour_rows, metric_daily_rows


def build_demo_dq_alerts(
    quality_path: Path = DWD / "dwd_quality_station.csv",
    latest_ds: str = "2022-09-14",
) -> list[tuple]:
    """根据 dwd_quality_station.csv 实测异常站生成 dq_alert 记录。"""
    alerts: list[tuple] = []
    with quality_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            sid = str(r["station_id"]).strip()
            taz = (r.get("taz_id") or "").strip() or None
            mr = float(r.get("missing_rate") or 0)
            stuck = r.get("stuck_flag") in {"1", "1.0", 1}
            gap = int(float(r.get("gap_days_max") or 0))
            if stuck:
                alerts.append(
                    (
                        latest_ds,
                        "dwd",
                        "dwd_stuck_station",
                        "block",
                        sid,
                        taz,
                        1.0,
                        0.0,
                        f"站 {sid} 读数长期不变，疑似设备卡死",
                        "open",
                        None,
                    )
                )
            elif mr > 0.30:
                alerts.append(
                    (
                        latest_ds,
                        "dwd",
                        "dwd_high_missing_station",
                        "warn",
                        sid,
                        taz,
                        round(mr, 4),
                        0.30,
                        f"站 {sid} 缺失率 {mr:.1%}（最长断档 {gap} 天）",
                        "open",
                        None,
                    )
                )
            elif gap >= 1:
                alerts.append(
                    (
                        latest_ds,
                        "dwd",
                        "dwd_long_gap_station",
                        "warn",
                        sid,
                        taz,
                        float(gap),
                        1.0,
                        f"站 {sid} 存在连续 {gap} 天缺数",
                        "open",
                        None,
                    )
                )
    return alerts


def build_demo_ops_run_rows(
    zone_hour_path: Path = DWD / "dwd_zone_hour.csv",
    quality_path: Path = DWD / "dwd_quality_station.csv",
) -> tuple[list[tuple], list[tuple]]:
    """基于演示子集真实日期和质量统计，构建 ops_task_run 与 dq_run_log 记录。"""
    day_zones: dict[str, set[str]] = defaultdict(set)
    day_rows: dict[str, int] = defaultdict(int)
    with zone_hour_path.open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            ds = (r.get("ts_hour") or "")[:10]
            if ds:
                day_zones[ds].add(str(r.get("taz_id") or ""))
                day_rows[ds] += 1

    with quality_path.open(encoding="utf-8") as f:
        q_rows = list(csv.DictReader(f))
    station_cnt = len(q_rows)
    stuck_cnt = sum(1 for r in q_rows if r.get("stuck_flag") in {"1", "1.0", 1})
    high_miss_cnt = sum(1 for r in q_rows if float(r.get("missing_rate") or 0) > 0.30)

    dates = sorted(day_zones)
    ops_rows: list[tuple] = []
    dq_rows: list[tuple] = []
    dag_id = "sz_ev_daily"
    base_durs = [
        ("ods_ingest", 4.2),
        ("dq_ods", 1.1),
        ("dwd_build", 6.8),
        ("dq_dwd", 1.4),
        ("ads_build", 5.5),
        ("dq_ads", 0.9),
        ("publish", 2.3),
        ("forecast_daily", 3.1),
        ("forecast_evaluate", 1.2),
        ("site_suggest", 1.8),
        ("cache_flush", 0.4),
    ]

    for idx, ds in enumerate(dates):
        run_day = dt.datetime.strptime(ds, "%Y-%m-%d") + dt.timedelta(days=1, hours=2)
        cur_ts = run_day
        run_id = f"demo_{ds.replace('-', '')}"
        z_cnt = len(day_zones[ds])
        r_cnt = day_rows[ds]

        for step_idx, (task_id, base_s) in enumerate(base_durs):
            dur = round(base_s + ((idx * 3 + step_idx) % 5) * 0.17, 3)
            end_ts = cur_ts + dt.timedelta(seconds=dur)
            ops_rows.append(
                (
                    dag_id,
                    ds,
                    task_id,
                    "success",
                    cur_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    end_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    dur,
                    None,
                    "run_day",
                )
            )
            cur_ts = end_ts + dt.timedelta(milliseconds=250)

        dq_rows.extend(
            [
                (run_id, ds, "ods", "ods_station_count", "row_count", "block", 1, str(station_cnt), ">=20", f"原始层共接入 {station_cnt} 座演示站"),
                (run_id, ds, "dwd", "dwd_stuck_station_warn", "threshold", "warn", 0 if stuck_cnt else 1, str(stuck_cnt), "==0", f"检测到 {stuck_cnt} 座读数卡死站，已转告警"),
                (run_id, ds, "dwd", "dwd_high_missing_warn", "threshold", "warn", 0 if high_miss_cnt else 1, str(high_miss_cnt), "==0", f"检测到 {high_miss_cnt} 座高缺失站，已转巡检"),
                (run_id, ds, "ads", "ads_zone_hour_nonempty", "row_count", "block", 1, f"{z_cnt} zones / {r_cnt} rows", ">0", f"汇总层生成 {z_cnt} 个片区共 {r_cnt} 条小时记录"),
            ]
        )

    return ops_rows, dq_rows


def build_demo_audit_logs(latest_ds: str = "2022-09-14") -> list[tuple]:
    """构建 sys_op_log 初始演示审计记录，便于答辩开箱展示系统管理与审计链路。"""
    return [
        ("demo_admin", "admin", "login", "auth", "demo_admin", "管理员 demo_admin 登录系统", "ok", "127.0.0.1", f"{latest_ds} 09:00:00"),
        ("demo_admin", "admin", "ticket_create", "ticket", "WO-0001", "由告警 AL-0001 派发维修工单 WO-0001（站点 1334）", "ok", "127.0.0.1", f"{latest_ds} 09:05:00"),
        ("demo_admin", "admin", "ticket_update", "ticket", "WO-0001", "工单 WO-0001（站点 1334）状态调整为「处理中」", "ok", "127.0.0.1", f"{latest_ds} 09:12:00"),
        ("访客", "readonly", "auth_denied", "ticket", "WO-0001", "只读访客尝试修改工单状态，已被权限拦截", "denied", "127.0.0.1", f"{latest_ds} 09:20:00"),
        ("demo_admin", "admin", "alert_update", "alert", "AL-0002", "告警 AL-0002 状态变更为「处理中」", "ok", "127.0.0.1", f"{latest_ds} 09:28:00"),
        ("demo_admin", "admin", "export", "export", "audit_log.csv", "导出操作审计日志 CSV（共 5 条）", "ok", "127.0.0.1", f"{latest_ds} 09:35:00"),
    ]


def _table_exists(cur, table: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (table,))
    return cur.fetchone() is not None


def main():
    c = conn()
    cur = c.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in [
        "ads_ticket",
        "ads_baseline_naive",
        "ads_zone_occupancy_hour",
        "ads_zone_daily",
        "ads_zone_site_suggest",
        "ads_forecast_zone_hour",
        "ads_forecast_metric_daily",
        "ads_city_overview",
        "ads_quality_summary",
        "rt_zone_occupancy",
        "ops_task_run",
        "sys_op_log",
        "dq_run_log",
        "dq_alert",
        "dim_station",
        "dwd_zone_hour",
        "dwd_quality_station",
        "dwd_station_5min_clean",
        "ods_station_raw_5min",
        "ods_station_info",
    ]:
        if _table_exists(cur, t):
            cur.execute(f"TRUNCATE TABLE {t}")

    info_path = ODS / "ods_station_info.csv"
    if not info_path.exists() and not Path(f"{info_path}.gz").exists():
        # fresh clone: ods/ is gitignored; derive station info from the tracked demo list
        src = DEMO_LIST
        ODS.mkdir(parents=True, exist_ok=True)
        with src.open(encoding="utf-8") as fi, info_path.open("w", encoding="utf-8", newline="") as fo:
            w = csv.writer(fo)
            w.writerow(["station_id", "taz_id", "lon_gcj", "lat_gcj", "lon_wgs", "lat_wgs", "total_piles", "rated_power_kw"])
            for r in csv.DictReader(fi):
                w.writerow([r["sid"], r["TAZID"], r["longitude"], r["latitude"], "", "", r.get("charge_count", ""), ""])
    load_csv(
        cur,
        "ods_station_info",
        info_path,
        [
            "station_id",
            "taz_id",
            "lon_gcj",
            "lat_gcj",
            "lon_wgs",
            "lat_wgs",
            "total_piles",
            "rated_power_kw",
        ],
    )
    # raw optional (large) — skip if missing
    raw = ODS / "ods_station_raw_5min.csv"
    if raw.exists() and raw.stat().st_size < 50_000_000:
        load_csv(
            cur,
            "ods_station_raw_5min",
            raw,
            [
                "station_id",
                "ts",
                "busy",
                "idle",
                "s_price",
                "e_price",
                "fast_busy",
                "fast_idle",
                "slow_busy",
                "slow_idle",
                "duration",
                "volume",
                "src_file",
                "load_at",
            ],
        )

    load_csv(
        cur,
        "dwd_station_5min_clean",
        DWD / "dwd_station_5min_clean.csv",
        [
            "station_id",
            "ts",
            "busy",
            "idle",
            "total_piles",
            "s_price",
            "e_price",
            "duration",
            "volume",
            "is_row_missing",
            "is_stuck",
            "is_outlier",
            "occupancy",
            "quality_flag",
        ],
    )
    load_csv(
        cur,
        "dwd_quality_station",
        DWD / "dwd_quality_station.csv",
        ["station_id", "missing_rate", "stuck_flag", "gap_days_max", "ticket_suggest"],
    )
    # dwd_quality may not have taz in table — CSV has taz_id; ignore extra
    load_csv(
        cur,
        "dwd_zone_hour",
        DWD / "dwd_zone_hour.csv",
        ["taz_id", "ts_hour", "occupancy_avg", "busy_sum", "piles_sum", "station_cnt"],
    )

    # dim_station & ads_zone_daily（支撑 MySQL 地图与选址基础视图）
    if _table_exists(cur, "dim_station"):
        cur.execute(
            """
            INSERT INTO dim_station (station_id, taz_id, longitude, latitude, total_piles, rated_power_kw)
            SELECT station_id, taz_id, COALESCE(lon_wgs, lon_gcj), COALESCE(lat_wgs, lat_gcj), total_piles, rated_power_kw
            FROM ods_station_info
            ON DUPLICATE KEY UPDATE
              taz_id=VALUES(taz_id), longitude=VALUES(longitude), latitude=VALUES(latitude),
              total_piles=VALUES(total_piles), rated_power_kw=VALUES(rated_power_kw)
            """
        )
    if _table_exists(cur, "ads_zone_daily") and _table_exists(cur, "dim_station"):
        cur.execute(
            """
            INSERT INTO ads_zone_daily (dt, taz_id, avg_occupancy, station_cnt, pile_cnt, center_lon, center_lat)
            SELECT
              DATE(z.ts_hour) AS dt,
              z.taz_id,
              ROUND(AVG(z.occupancy_avg), 6) AS avg_occupancy,
              MAX(s.station_cnt) AS station_cnt,
              MAX(s.pile_cnt) AS pile_cnt,
              MAX(s.center_lon) AS center_lon,
              MAX(s.center_lat) AS center_lat
            FROM dwd_zone_hour z
            LEFT JOIN (
              SELECT taz_id, COUNT(*) AS station_cnt, SUM(total_piles) AS pile_cnt,
                     ROUND(AVG(longitude), 6) AS center_lon, ROUND(AVG(latitude), 6) AS center_lat
              FROM dim_station
              GROUP BY taz_id
            ) s ON s.taz_id = z.taz_id
            GROUP BY DATE(z.ts_hour), z.taz_id
            ON DUPLICATE KEY UPDATE
              avg_occupancy=VALUES(avg_occupancy), station_cnt=VALUES(station_cnt), pile_cnt=VALUES(pile_cnt),
              center_lon=VALUES(center_lon), center_lat=VALUES(center_lat)
            """
        )

    # quality summary from report numbers + live counts
    cur.execute(
        """
        REPLACE INTO ads_quality_summary (metric, value, note) VALUES
        ('demo_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station), '演示子集'),
        ('row_missing_rate', 0.2642, '14天窗实测 docs/quality_report.md'),
        ('stuck_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1), '窗内实测'),
        ('high_missing_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30), 'missing>30%')
        """
    )
    cur.execute(
        """
        INSERT INTO ads_zone_occupancy_hour (taz_id, ts_hour, occupancy, load_kw_est)
        SELECT taz_id, ts_hour, occupancy_avg, ROUND(busy_sum * 7, 2)
        FROM dwd_zone_hour
        ON DUPLICATE KEY UPDATE occupancy=VALUES(occupancy), load_kw_est=VALUES(load_kw_est)
        """
    )
    cur.execute(
        """
        INSERT INTO ads_baseline_naive (taz_id, ts_hour, y_true, y_hat_lag1, abs_err)
        SELECT
          taz_id,
          ts_hour,
          occupancy_avg AS y_true,
          COALESCE(LAG(occupancy_avg) OVER (PARTITION BY taz_id ORDER BY ts_hour), occupancy_avg) AS y_hat_lag1,
          ABS(occupancy_avg - COALESCE(LAG(occupancy_avg) OVER (PARTITION BY taz_id ORDER BY ts_hour), occupancy_avg)) AS abs_err
        FROM dwd_zone_hour
        ON DUPLICATE KEY UPDATE y_true=VALUES(y_true), y_hat_lag1=VALUES(y_hat_lag1), abs_err=VALUES(abs_err)
        """
    )
    cur.execute(
        """
        INSERT INTO ads_city_overview (dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt)
        SELECT
          d.dt,
          (SELECT COUNT(*) FROM dwd_quality_station),
          d.zone_cnt,
          d.avg_occupancy,
          p.peak_hour,
          (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1),
          (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30)
        FROM (
          SELECT DATE(ts_hour) AS dt,
                 COUNT(DISTINCT taz_id) AS zone_cnt,
                 AVG(occupancy_avg) AS avg_occupancy
          FROM dwd_zone_hour
          GROUP BY DATE(ts_hour)
        ) d
        LEFT JOIN (
          SELECT dt, peak_hour
          FROM (
            SELECT DATE(ts_hour) AS dt,
                   HOUR(ts_hour) AS peak_hour,
                   AVG(occupancy_avg) AS occ,
                   ROW_NUMBER() OVER (PARTITION BY DATE(ts_hour) ORDER BY AVG(occupancy_avg) DESC) AS rn
            FROM dwd_zone_hour
            GROUP BY DATE(ts_hour), HOUR(ts_hour)
          ) x
          WHERE rn = 1
        ) p ON p.dt = d.dt
        ON DUPLICATE KEY UPDATE
          station_cnt=VALUES(station_cnt), zone_cnt=VALUES(zone_cnt),
          avg_occupancy=VALUES(avg_occupancy), peak_hour=VALUES(peak_hour),
          stuck_cnt=VALUES(stuck_cnt), high_missing_cnt=VALUES(high_missing_cnt)
        """
    )
    cur.execute(
        """
        INSERT INTO ads_ticket (station_id, type, status)
        SELECT station_id,
               CASE WHEN stuck_flag=1 THEN 'repair' ELSE 'inspect' END,
               'open'
        FROM dwd_quality_station
        WHERE stuck_flag=1 OR missing_rate>0.30
        """
    )

    # 选址补桩建议（与 etl/zone_profile.py 同一套经验贝叶斯 + 缺口分公式）
    if _table_exists(cur, "ads_zone_site_suggest"):
        site_rows = build_site_suggest_rows()
        if site_rows:
            cur.executemany(
                """
                INSERT INTO ads_zone_site_suggest (
                  run_dt, taz_id, center_lon, center_lat, station_cnt, piles, rated_kw,
                  avg_occupancy, peak_occupancy, busy_hours_ratio,
                  demand_score, supply_score, gap_score, suggest_add_piles, `rank`, function_tag, reason
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                site_rows,
            )

    # 实时占用初始快照（最后 2 个 15 分钟时间段）
    if _table_exists(cur, "rt_zone_occupancy"):
        rt_rows = build_realtime_snapshot_rows()
        if rt_rows:
            cur.executemany(RT_UPSERT_SQL, rt_rows)

    # 预测曲线与每日预测误差（源自 forecast/predictions.csv）
    if _table_exists(cur, "ads_forecast_zone_hour") and _table_exists(cur, "ads_forecast_metric_daily"):
        fc_hour_rows, fc_daily_rows = build_forecast_rows()
        if fc_hour_rows:
            cur.executemany(
                """
                INSERT INTO ads_forecast_zone_hour (taz_id, ts_hour, y_hat_lgbm, y_hat_lag1, model_version, trained_until)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE y_hat_lgbm=VALUES(y_hat_lgbm), y_hat_lag1=VALUES(y_hat_lag1)
                """,
                fc_hour_rows,
            )
        if fc_daily_rows:
            cur.executemany(
                """
                INSERT INTO ads_forecast_metric_daily
                  (ds, n, mae_lgbm, rmse_lgbm, mae_lag1, rmse_lag1, mae_lag24, model_version, drift)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  n=VALUES(n), mae_lgbm=VALUES(mae_lgbm), rmse_lgbm=VALUES(rmse_lgbm),
                  mae_lag1=VALUES(mae_lag1), rmse_lag1=VALUES(rmse_lag1),
                  mae_lag24=VALUES(mae_lag24), model_version=VALUES(model_version), drift=VALUES(drift)
                """,
                fc_daily_rows,
            )

    # 调度运行记录、质检日志与质检告警
    ops_rows, dq_log_rows = build_demo_ops_run_rows()
    if _table_exists(cur, "ops_task_run") and ops_rows:
        cur.executemany(
            """
            INSERT INTO ops_task_run (dag_id, ds, task_id, state, start_ts, end_ts, duration_s, block_reason, source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            ops_rows,
        )
    if _table_exists(cur, "dq_run_log") and dq_log_rows:
        cur.executemany(
            """
            INSERT INTO dq_run_log
              (run_id, ds, layer, rule_name, rule_type, severity, passed, actual, expected, message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            dq_log_rows,
        )
    if _table_exists(cur, "dq_alert"):
        latest_ds = ops_rows[-1][1] if ops_rows else "2022-09-14"
        alert_rows = build_demo_dq_alerts(latest_ds=latest_ds)
        if alert_rows:
            cur.executemany(
                """
                INSERT INTO dq_alert
                  (ds, layer, rule_name, severity, station_id, taz_id, metric_value, threshold, detail, status, note)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                alert_rows,
            )
    if _table_exists(cur, "sys_op_log"):
        latest_ds = ops_rows[-1][1] if ops_rows else "2022-09-14"
        cur.executemany(
            """
            INSERT INTO sys_op_log (actor, role, action, target_type, target_id, detail, result, ip, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            build_demo_audit_logs(latest_ds=latest_ds),
        )

    c.commit()
    cur.close()
    c.close()
    print("OK loaded demo CSVs → MySQL ads/ops/rt/site/forecast backfilled")


if __name__ == "__main__":
    main()
