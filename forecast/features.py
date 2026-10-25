"""片区小时级特征构建与数据加载。换全量数据只需改 load_zone_hour 的 source 参数。"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

LAGS = (1, 2, 3, 24)
ROLLS = (3, 24)
MIN_DAYS = 7
FEATURES = ["hour", "dayofweek", "is_weekend", *[f"lag_{k}" for k in LAGS],
            *[f"roll_mean_{w}" for w in ROLLS], "taz_id"]


def _from_parquet(path: Path) -> pd.DataFrame:
    """5 分钟站级 Parquet（按 dt=YYYY-MM-DD 分区）→ 片区小时。
    需要列：station_id, ts, busy, total_piles, taz_id（缺 taz_id 时用同目录 station_info 补，不在此处理）。
    occupancy = sum(busy)/sum(total_piles)，与 dwd_zone_hour.occupancy_avg 口径一致（按小时内均值）。"""
    df = pd.read_parquet(path, columns=["station_id", "ts", "busy", "total_piles", "taz_id"])
    df = df.dropna(subset=["busy", "total_piles"])
    df = df[df["total_piles"] > 0]
    df["ts"] = pd.to_datetime(df["ts"]).dt.floor("h")
    df["occ"] = df["busy"] / df["total_piles"]
    g = df.groupby(["taz_id", "ts"])["occ"].mean().rename("y").reset_index()
    return g


def load_zone_hour(source: str | Path) -> pd.DataFrame:
    """返回 taz_id(str), ts(datetime), y(占用率)。source 可以是 dwd_zone_hour.csv 或 Parquet 目录/文件。"""
    p = Path(source)
    if not p.exists():
        raise SystemExit(f"[forecast] 数据源不存在: {p}")
    if p.is_dir() or p.suffix == ".parquet":
        df = _from_parquet(p)
    else:
        raw = pd.read_csv(p)
        df = raw.rename(columns={"ts_hour": "ts", "occupancy_avg": "y"})[["taz_id", "ts", "y"]]
    df["taz_id"] = df["taz_id"].astype(str)
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.dropna(subset=["y"]).sort_values(["taz_id", "ts"]).reset_index(drop=True)
    span_days = (df["ts"].max() - df["ts"].min()) / pd.Timedelta(days=1) if len(df) else 0
    if span_days < MIN_DAYS:
        raise SystemExit(f"[forecast] 历史只有 {span_days:.1f} 天，lag-24/rolling-24 至少需要 {MIN_DAYS} 天")
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """按片区补齐整点网格后计算特征；lag/rolling 全部基于 shift(≥1)，只用过去值。"""
    out = []
    for taz, g in df.groupby("taz_id", sort=False):
        idx = pd.date_range(g["ts"].min(), g["ts"].max(), freq="h")
        g = g.set_index("ts").reindex(idx)
        g.index.name = "ts"
        g["taz_id"] = taz
        y = g["y"]
        for k in LAGS:
            g[f"lag_{k}"] = y.shift(k)
        past = y.shift(1)
        for w in ROLLS:
            g[f"roll_mean_{w}"] = past.rolling(w, min_periods=1).mean()
        out.append(g.reset_index())
    f = pd.concat(out, ignore_index=True)
    f["hour"] = f["ts"].dt.hour
    f["dayofweek"] = f["ts"].dt.day_of_week
    f["is_weekend"] = (f["dayofweek"] >= 5).astype(int)
    f["taz_id"] = f["taz_id"].astype("category")
    return f.dropna(subset=["y", "lag_1"]).reset_index(drop=True)


def time_split(f: pd.DataFrame, test_frac: float = 0.2):
    ts = np.sort(f["ts"].unique())
    cut = ts[int(len(ts) * (1 - test_frac))]
    return f[f["ts"] < cut], f[f["ts"] >= cut], pd.Timestamp(cut)
