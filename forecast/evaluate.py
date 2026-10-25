"""每日预测监控：把 ds 当天的预测（daily.py 在 ds-1 产出）和实际占用率对比，写 MAE/RMSE，判断模型是否变差。

python -m forecast.evaluate --ds 2023-02-02 [--no-db]
输入 : $SZ_EV_LAKE/ads/forecast_zone_hour/dt=<ds>/ 与 $SZ_EV_LAKE/ads/zone_hour/dt=<ds>/（以及 dt=<ds-1> 算 lag24 参考）
输出 : $SZ_EV_LAKE/ads/forecast_metric_daily/dt=<ds>/part-0.parquet、MySQL ads_forecast_metric_daily（按 ds 先删后插）；
       触发漂移时往 dq_alert 插一条 rule_name=forecast_drift（同一天重跑先删掉当天仍为 open 的旧条再插）
漂移规则: ① LightGBM MAE 连续 3 天比日前朴素基线(y_hat_lag1)差；② 当天 LightGBM MAE > 前 28 天 MAE 中位数 × 1.5（至少 7 天历史）
退出码: 0 正常（含触发告警）；2 缺预测或实际值；3 写 MySQL 失败
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from forecast.daily import InputMissing, lake  # noqa: E402

CONSEC_DAYS = 3
MEDIAN_DAYS = 28
MEDIAN_FACTOR = 1.5
MIN_HIST = 7
RULE = "forecast_drift"


def _m(y, p):
    m = ~(np.isnan(y) | np.isnan(p))
    if not m.any():
        return None, None, 0
    e = y[m] - p[m]
    return round(float(np.mean(np.abs(e))), 6), round(float(np.sqrt(np.mean(e ** 2))), 6), int(m.sum())


def compute(ds: str, lake_dir: Path) -> dict:
    fp = lake_dir / "ads" / "forecast_zone_hour" / f"dt={ds}"
    ap = lake_dir / "ads" / "zone_hour" / f"dt={ds}"
    if not fp.exists() or not ap.exists():
        raise InputMissing(f"缺少 {fp if not fp.exists() else ap}")
    f = pd.read_parquet(fp)
    a = pd.read_parquet(ap, columns=["taz_id", "ts_hour", "occupancy_avg"])
    for d in (f, a):
        d["taz_id"] = d["taz_id"].astype(str)
        d["ts_hour"] = pd.to_datetime(d["ts_hour"])
    j = f.merge(a, on=["taz_id", "ts_hour"], how="inner").dropna(subset=["occupancy_avg", "y_hat_lgbm"])
    pp = lake_dir / "ads" / "zone_hour" / f"dt={(pd.Timestamp(ds) - pd.Timedelta(days=1)):%Y-%m-%d}"
    j["y_lag24"] = np.nan
    if pp.exists():
        p = pd.read_parquet(pp, columns=["taz_id", "ts_hour", "occupancy_avg"]).rename(columns={"occupancy_avg": "y_lag24"})
        p["taz_id"] = p["taz_id"].astype(str)
        p["ts_hour"] = pd.to_datetime(p["ts_hour"]) + pd.Timedelta(days=1)
        j = j.drop(columns="y_lag24").merge(p, on=["taz_id", "ts_hour"], how="left")
    y = j["occupancy_avg"].to_numpy(float)
    mae_l, rmse_l, n = _m(y, j["y_hat_lgbm"].to_numpy(float))
    mae_b, rmse_b, _ = _m(y, j["y_hat_lag1"].to_numpy(float))
    mae_s, _, _ = _m(y, j["y_lag24"].to_numpy(float))
    return {"ds": ds, "n": n, "mae_lgbm": mae_l, "rmse_lgbm": rmse_l, "mae_lag1": mae_b, "rmse_lag1": rmse_b,
            "mae_lag24": mae_s, "model_version": str(f["model_version"].iloc[0]) if len(f) else None}


def history(lake_dir: Path, ds: str) -> pd.DataFrame:
    base = lake_dir / "ads" / "forecast_metric_daily"
    parts = [pd.read_parquet(p) for p in sorted(base.glob("dt=*")) if p.name[3:] < ds]
    return pd.concat(parts, ignore_index=True).sort_values("ds") if parts else pd.DataFrame(columns=["ds", "mae_lgbm", "mae_lag1"])


def drift(m: dict, hist: pd.DataFrame) -> list[dict]:
    """返回触发的告警（可能 0~2 条），每条含 metric_value / threshold / detail（大白话）。"""
    out = []
    if m["mae_lgbm"] is None:
        return out
    seq = [*zip(hist["mae_lgbm"], hist["mae_lag1"])][-(CONSEC_DAYS - 1):] + [(m["mae_lgbm"], m["mae_lag1"])]
    if len(seq) == CONSEC_DAYS and all(l is not None and b is not None and l > b for l, b in seq):
        out.append({"metric_value": m["mae_lgbm"], "threshold": m["mae_lag1"],
                    "detail": f"预测模型连续 {CONSEC_DAYS} 天不如朴素基线：{m['ds']} 模型平均误差 {m['mae_lgbm']:.4f}，"
                              f"基线 {m['mae_lag1']:.4f}。建议检查数据是否异常或重新调参。"})
    h = pd.to_numeric(hist["mae_lgbm"], errors="coerce").dropna().tail(MEDIAN_DAYS)
    if len(h) >= MIN_HIST:
        thr = round(float(h.median()) * MEDIAN_FACTOR, 6)
        if m["mae_lgbm"] > thr:
            out.append({"metric_value": m["mae_lgbm"], "threshold": thr,
                        "detail": f"预测误差突然变大：{m['ds']} 模型平均误差 {m['mae_lgbm']:.4f}，超过近 {len(h)} 天中位数的 "
                                  f"{MEDIAN_FACTOR} 倍（{thr:.4f}）。可能是节假日、断传或数据口径变化。"})
    return out


def write_mysql(m: dict, alerts: list[dict], conn=None) -> None:
    from forecast import db
    own = conn is None
    conn = conn or db.connect()
    try:
        db.ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ads_forecast_metric_daily WHERE ds=%s", (m["ds"],))
            cur.execute("INSERT INTO ads_forecast_metric_daily (ds, n, mae_lgbm, rmse_lgbm, mae_lag1, rmse_lag1, mae_lag24, "
                        "model_version, drift) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (m["ds"], m["n"], m["mae_lgbm"], m["rmse_lgbm"], m["mae_lag1"], m["rmse_lag1"], m["mae_lag24"],
                         m["model_version"], int(bool(alerts))))
            cur.execute("DELETE FROM dq_alert WHERE ds=%s AND rule_name=%s AND status='open'", (m["ds"], RULE))
            for al in alerts:
                cur.execute("INSERT INTO dq_alert (ds, layer, rule_name, severity, metric_value, threshold, detail) "
                            "VALUES (%s,'ads',%s,'warn',%s,%s,%s)",
                            (m["ds"], RULE, al["metric_value"], al["threshold"], al["detail"][:500]))
        conn.commit()
    finally:
        if own:
            conn.close()


def run(ds: str, write_db: bool = True, lake_dir: Path | None = None) -> dict:
    lake_dir = lake_dir or lake()
    m = compute(ds, lake_dir)
    alerts = drift(m, history(lake_dir, ds))
    od = lake_dir / "ads" / "forecast_metric_daily" / f"dt={ds}"
    od.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{**m, "drift": int(bool(alerts))}]).to_parquet(od / "part-0.parquet", index=False)
    if write_db:
        write_mysql(m, alerts)
    return {**m, "alerts": [a["detail"] for a in alerts]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ds", required=True, help="被评估的日期（预测目标日）")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args(argv)
    try:
        r = run(a.ds, not a.no_db)
    except InputMissing as e:
        print(f"[forecast.evaluate] 输入不足：{e}", file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001
        if "pymysql" in type(e).__module__:
            print(f"[forecast.evaluate] 写 MySQL 失败：{e}", file=sys.stderr)
            return 3
        raise
    print(json.dumps(r, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
