"""每日生产预测：用截至 ds 当天 23:00 的片区小时历史训练 LightGBM，递推预测 ds+1 的 24 个小时。

python -m forecast.daily --ds 2023-02-01 [--no-db] [--train-days 56] [--no-holiday]
输入 : $SZ_EV_LAKE/ads/zone_hour/dt=*/（taz_id, ts_hour, occupancy_avg），默认 data/lake
输出 : $SZ_EV_LAKE/ads/forecast_zone_hour/dt=<ds+1>/part-0.parquet  +  MySQL ads_forecast_zone_hour（按天先删后插）
退出码: 0 成功；2 输入不足（ds 当天或历史缺分区）；3 写 MySQL 失败（Parquet 已写好）
"""
from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from forecast.calendar_cn import holiday_frame  # noqa: E402

LAGS = (1, 2, 3, 24, 168)
ROLLS = (3, 24, 168)
HIST_DAYS = 28 + 1  # same_hour_mean_4w 需要往前 4 周
BASE_FEATURES = ["hour", "dayofweek", "is_weekend", *[f"lag_{k}" for k in LAGS],
                 *[f"roll_mean_{w}" for w in ROLLS], "diff_1", "same_hour_mean_4w", "taz_id"]
HOLIDAY_FEATURES = ["is_holiday", "is_adjusted_workday", "is_offday"]
# 外部特征插口：name -> fn(ts: pd.Series[datetime], taz: pd.Series[str]) -> pd.DataFrame（同索引）。
# 小紫的逐小时天气接入后在这里注册（如 temp / rain_mm），训练和预测会自动带上。
EXTRA_FEATURES: dict = {}
PARAMS = dict(n_estimators=300, learning_rate=0.05, num_leaves=63, min_child_samples=50, subsample=0.8,
              subsample_freq=1, colsample_bytree=0.9, random_state=42, verbose=-1, n_jobs=2)


class InputMissing(Exception):
    pass


def lake() -> Path:
    return Path(os.getenv("SZ_EV_LAKE", ROOT / "data" / "lake"))


def load_wide(ds: str, days: int, lake_dir: Path | None = None) -> pd.DataFrame:
    """读 [ds-days+1, ds] 的 ads/zone_hour → 宽表（index=整点, columns=taz_id, 值=占用率）。"""
    base = (lake_dir or lake()) / "ads" / "zone_hour"
    end = pd.Timestamp(ds)
    if not (base / f"dt={ds}").exists():
        raise InputMissing(f"缺少 {base}/dt={ds}")
    frames = []
    for d in pd.date_range(end - pd.Timedelta(days=days - 1), end):
        p = base / f"dt={d:%Y-%m-%d}"
        if p.exists():
            frames.append(pd.read_parquet(p, columns=["taz_id", "ts_hour", "occupancy_avg"]))
    if len(frames) < 14:
        raise InputMissing(f"历史分区只有 {len(frames)} 天，至少要 14 天")
    df = pd.concat(frames, ignore_index=True)
    df["taz_id"] = df["taz_id"].astype(str)
    df["ts_hour"] = pd.to_datetime(df["ts_hour"])
    w = df.groupby(["ts_hour", "taz_id"])["occupancy_avg"].mean().unstack("taz_id")
    zones = sorted(df["taz_id"].unique(), key=lambda z: (not z.isdigit(), int(z) if z.isdigit() else 0, z))
    w = w.reindex(columns=zones)  # 全部片区都保留；整段没有占用率的片区预测值为空
    idx = pd.date_range(end - pd.Timedelta(days=days - 1), end + pd.Timedelta(hours=23), freq="h")
    return w.reindex(idx).astype("float64")


def features_wide(w: pd.DataFrame, rows=None) -> pd.DataFrame:
    """宽表上一次性算所有片区的滞后特征（全部基于过去值），rows 指定只要哪些时刻（预测时只算最后一行）。"""
    sel = w.index if rows is None else rows
    cols = {}
    for k in LAGS:
        cols[f"lag_{k}"] = w.shift(k)
    past = w.shift(1)
    for r in ROLLS:
        cols[f"roll_mean_{r}"] = past.rolling(r, min_periods=1).mean()
    cols["diff_1"] = w.shift(1) - w.shift(2)
    sh = [w.shift(168 * k) for k in range(1, 5)]
    cols["same_hour_mean_4w"] = sum(s.fillna(0) for s in sh) / sum(s.notna().astype("float64") for s in sh).replace(0, np.nan)
    cols["y"] = w
    nt, nz = len(sel), w.shape[1]
    f = pd.DataFrame({"ts": np.repeat(np.asarray(sel), nz), "taz_id": np.tile(np.asarray(w.columns, dtype=object), nt)})
    for k, v in cols.items():
        f[k] = v.loc[sel].to_numpy().reshape(-1)
    f["hour"] = f["ts"].dt.hour.astype("int8")
    f["dayofweek"] = f["ts"].dt.day_of_week.astype("int8")
    f["is_weekend"] = (f["dayofweek"] >= 5).astype("int8")
    return f


def add_extra(f: pd.DataFrame, holiday: bool) -> list[str]:
    feats = list(BASE_FEATURES)
    if holiday:
        f[HOLIDAY_FEATURES] = holiday_frame(f["ts"])
        feats += HOLIDAY_FEATURES
    for name, fn in EXTRA_FEATURES.items():
        ext = fn(f["ts"], f["taz_id"])
        f[list(ext.columns)] = ext
        feats += list(ext.columns)
    return feats


def run(ds: str, train_days: int = 56, holiday: bool = True, write_db: bool = True, lake_dir: Path | None = None,
        out_dir: Path | None = None) -> dict:
    import lightgbm as lgb
    t0 = time.time()
    w = load_wide(ds, train_days + HIST_DAYS, lake_dir)
    zones = list(w.columns)
    cats = pd.CategoricalDtype(zones)
    end = pd.Timestamp(ds) + pd.Timedelta(hours=23)
    train_start = end - pd.Timedelta(days=train_days) + pd.Timedelta(hours=1)
    f = features_wide(w.loc[:end], w.loc[train_start:end].index)
    f = f.dropna(subset=["y", "lag_1"])
    f["taz_id"] = f["taz_id"].astype(cats)
    feats = add_extra(f, holiday)
    model = lgb.LGBMRegressor(**PARAMS)
    model.fit(f[feats], f["y"], categorical_feature=["taz_id"])
    n_train = len(f)
    del f
    # 递推：逐小时预测 ds+1，把预测值写回宽表，下一小时的 lag 就用上一步的预测
    last_obs = w.loc[:end].ffill().iloc[-1]
    ww = w.loc[:end].copy()
    preds = []
    for h in range(1, 25):
        ts = end + pd.Timedelta(hours=h)
        ww.loc[ts] = np.nan
        fx = features_wide(ww.iloc[-(168 * 4 + 1):], pd.DatetimeIndex([ts]))
        fx["taz_id"] = fx["taz_id"].astype(cats)
        add_extra(fx, holiday)
        yh = np.clip(model.predict(fx[feats]), 0, 1)
        ww.loc[ts, fx["taz_id"].astype(str).values] = yh
        preds.append(pd.DataFrame({"taz_id": fx["taz_id"].astype(str).values, "ts_hour": ts, "y_hat_lgbm": yh}))
    p = pd.concat(preds, ignore_index=True)
    p["y_hat_lag1"] = p["taz_id"].map(last_obs).astype(float)
    # 最近 7 天一个有效值都没有的片区（断传/无数据）不出预测，避免模型对着全空特征瞎猜
    recent = w.loc[end - pd.Timedelta(days=7):end].notna().any()
    p.loc[~p["taz_id"].map(recent).fillna(False).astype(bool), ["y_hat_lgbm", "y_hat_lag1"]] = np.nan
    version = f"lgbm-{'hol-' if holiday else ''}w{train_days}-{ds}"
    p["model_version"] = version
    p["trained_until"] = end
    p = p.round({"y_hat_lgbm": 6, "y_hat_lag1": 6})
    nd = (pd.Timestamp(ds) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    od = (out_dir or ((lake_dir or lake()) / "ads" / "forecast_zone_hour")) / f"dt={nd}"
    od.mkdir(parents=True, exist_ok=True)
    for x in od.glob("*"):
        x.unlink()
    p.to_parquet(od / "part-0.parquet", index=False)
    if write_db:
        write_mysql(p, nd)
    return {"ds": ds, "predict_dt": nd, "zones": len(zones), "rows": len(p), "zones_predicted": int(p.groupby("taz_id")["y_hat_lgbm"].count().gt(0).sum()), "train_rows": n_train,
            "model_version": version, "seconds": round(time.time() - t0, 1),
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)}


def write_mysql(p: pd.DataFrame, nd: str, conn=None) -> int:
    from forecast import db
    own = conn is None
    conn = conn or db.connect()
    try:
        db.ensure_tables(conn)
        rows = [(r.taz_id, r.ts_hour.to_pydatetime(), None if pd.isna(r.y_hat_lgbm) else float(r.y_hat_lgbm),
                 None if pd.isna(r.y_hat_lag1) else float(r.y_hat_lag1), r.model_version,
                 r.trained_until.to_pydatetime()) for r in p.itertuples()]
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ads_forecast_zone_hour WHERE ts_hour >= %s AND ts_hour < DATE_ADD(%s, INTERVAL 1 DAY)",
                        (nd, nd))
            cur.executemany("INSERT INTO ads_forecast_zone_hour (taz_id, ts_hour, y_hat_lgbm, y_hat_lag1, model_version, "
                            "trained_until) VALUES (%s,%s,%s,%s,%s,%s)", rows)
        conn.commit()
        return len(rows)
    finally:
        if own:
            conn.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--ds", required=True, help="训练截止日（含当天），预测 ds+1")
    ap.add_argument("--train-days", type=int, default=56)
    ap.add_argument("--no-holiday", action="store_true")
    ap.add_argument("--no-db", action="store_true", help="只写 Parquet")
    ap.add_argument("--out-dir", default=None, help="Parquet 输出根目录（默认 $SZ_EV_LAKE/ads/forecast_zone_hour）")
    a = ap.parse_args(argv)
    try:
        r = run(a.ds, a.train_days, not a.no_holiday, not a.no_db, out_dir=Path(a.out_dir) if a.out_dir else None)
    except InputMissing as e:
        print(f"[forecast.daily] 输入不足：{e}", file=sys.stderr)
        return 2
    except Exception as e:  # noqa: BLE001
        if "pymysql" in type(e).__module__:
            print(f"[forecast.daily] 写 MySQL 失败：{e}", file=sys.stderr)
            return 3
        raise
    print(json.dumps(r, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
