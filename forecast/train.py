"""LightGBM 片区下一小时占用率预测 vs 上一小时基线。
用法：python -m forecast.train [--source data/demo/dwd/dwd_zone_hour.csv | data/lake/ods/charge_5min]
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

from forecast.features import FEATURES, build_features, load_zone_hour, time_split

ROOT = Path(__file__).resolve().parents[1]
LAKE_DWD = ROOT / "data" / "lake" / "dwd" / "charge_5min_clean"  # 全量清洗后 Parquet（dt=YYYY-MM-DD 分区）
DEFAULT_SOURCE = ROOT / "data" / "demo" / "dwd" / "dwd_zone_hour.csv"  # 换全量：改成 LAKE_DWD 或 --source
OUT = Path(__file__).resolve().parent


def _m(y, p):
    e = np.asarray(y) - np.asarray(p)
    return {"mae": round(float(np.mean(np.abs(e))), 5), "rmse": round(float(np.sqrt(np.mean(e ** 2))), 5)}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(DEFAULT_SOURCE))
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--test-days", type=int, default=None, help="最后 N 天做测试集（优先于 --test-frac）")
    ap.add_argument("--pred-keep-days", type=int, default=2, help="predictions.csv 只保留测试集最后 N 天")
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args(argv)
    t0 = time.time()
    f = build_features(load_zone_hour(a.source))
    tr, te, cut = time_split(f, a.test_frac, a.test_days)
    model = lgb.LGBMRegressor(n_estimators=600, learning_rate=0.05, num_leaves=63,
                              min_child_samples=50, subsample=0.8, subsample_freq=1,
                              colsample_bytree=0.9, random_state=42, verbose=-1)
    model.fit(tr[FEATURES], tr["y"], categorical_feature=["taz_id"])
    te = te.copy()
    te["y_hat_lgbm"] = np.clip(model.predict(te[FEATURES]), 0, 1)
    te["y_hat_lag1"] = te["lag_1"]
    per_zone = {str(z): {"n": int(len(g)), "lgbm": _m(g["y"], g["y_hat_lgbm"]), "lag1": _m(g["y"], g["y_hat_lag1"])}
                for z, g in te.groupby("taz_id", observed=True)}
    metrics = {
        "source": str(a.source), "rows_train": int(len(tr)), "rows_test": int(len(te)),
        "zones": int(f["taz_id"].nunique()), "split_ts": str(cut),
        "overall": {"lgbm": _m(te["y"], te["y_hat_lgbm"]), "lag1": _m(te["y"], te["y_hat_lag1"])},
        "zones_lgbm_better_rmse": sum(v["lgbm"]["rmse"] < v["lag1"]["rmse"] for v in per_zone.values()),
        "zones_lgbm_better_mae": sum(v["lgbm"]["mae"] < v["lag1"]["mae"] for v in per_zone.values()),
        "per_zone": per_zone, "seconds": round(time.time() - t0, 2),
    }
    out = Path(a.out)
    (out / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    keep = te[te["ts"] > te["ts"].max() - pd.Timedelta(days=a.pred_keep_days)]
    keep.assign(ts=keep["ts"].dt.strftime("%Y-%m-%d %H:%M:%S"))[["taz_id", "ts", "y", "y_hat_lgbm", "y_hat_lag1"]] \
        .round(6).to_csv(out / "predictions.csv", index=False)
    print(json.dumps({k: metrics[k] for k in ("rows_train", "rows_test", "zones", "split_ts", "overall",
                                               "zones_lgbm_better_mae", "zones_lgbm_better_rmse", "seconds")}, ensure_ascii=False))
    return metrics


if __name__ == "__main__":
    main()
