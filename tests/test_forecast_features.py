"""预测特征：无未来信息泄漏、时间切分严格、数据源与最少天数校验。"""
import numpy as np
import pandas as pd
import pytest

from forecast.features import FEATURES, build_features, load_zone_hour, time_split


def _toy(days=8, zones=("1", "2")):
    ts = pd.date_range("2022-09-01", periods=24 * days, freq="h")
    return pd.concat([pd.DataFrame({"taz_id": z, "ts": ts, "y": np.arange(len(ts), dtype=float) + i * 1000})
                      for i, z in enumerate(zones)], ignore_index=True)


def test_lags_only_use_past():
    f = build_features(_toy())
    for _, g in f.groupby("taz_id", observed=True):
        assert (g["lag_1"] == g["y"] - 1).all()
        assert (g["lag_24"].dropna() == g.loc[g["lag_24"].notna(), "y"] - 24).all()
        assert (g["roll_mean_3"] < g["y"]).all()  # 单调递增序列：过去均值必小于当前值
        assert (g["roll_mean_24"] < g["y"]).all()


def test_changing_future_does_not_change_features():
    a = _toy(); b = a.copy()
    last = b["ts"] == b["ts"].max()
    b.loc[last, "y"] = -999.0
    fa, fb = build_features(a), build_features(b)
    cols = [c for c in FEATURES if c != "taz_id"]
    pd.testing.assert_frame_equal(fa[cols], fb[cols])


def test_no_cross_zone_leak():
    f = build_features(_toy())
    z2 = f[f["taz_id"] == "2"]
    assert z2["lag_1"].min() >= 1000  # 不会取到片区 1 的值


def test_time_split_strict():
    tr, te, cut = time_split(build_features(_toy()))
    assert tr["ts"].max() < cut <= te["ts"].min()
    assert 0.15 < len(te) / (len(tr) + len(te)) < 0.25


def test_min_days_enforced(tmp_path):
    p = tmp_path / "z.csv"
    _toy(days=3).rename(columns={"ts": "ts_hour", "y": "occupancy_avg"}).to_csv(p, index=False)
    with pytest.raises(SystemExit, match="至少需要 7 天"):
        load_zone_hour(p)


def test_parquet_source_aggregates_to_zone_hour(tmp_path):
    pytest.importorskip("pyarrow")
    ts = pd.date_range("2022-09-01", periods=12 * 24 * 8, freq="5min")
    df = pd.DataFrame({"station_id": "s1", "ts": ts, "busy": 5.0, "total_piles": 10, "taz_id": 7})
    df["dt"] = df["ts"].dt.strftime("%Y-%m-%d")
    df.to_parquet(tmp_path / "lake", partition_cols=["dt"])
    z = load_zone_hour(tmp_path / "lake")
    assert len(z) == 24 * 8 and set(z["taz_id"]) == {"7"} and (z["y"] == 0.5).all()


def test_shipped_demo_loads():
    from forecast.train import DEFAULT_SOURCE
    z = load_zone_hour(DEFAULT_SOURCE)
    assert z["taz_id"].nunique() == 18
