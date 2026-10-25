"""Fast tests for etl/full_pipeline.py + etl/pile_power.py on a tiny synthetic fixture (no real data)."""
from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

import full_pipeline as fp
import pile_power

D0 = pd.Timestamp("2022-09-01")
NDAYS = 14
# 1001 random (never stuck); 1002 constant busy (stuck every day);
# 1003 random on days 0-1 then constant -> stuck from day 8 (7-day window days 2..8);
# 1004 only 11 valid identical points (< 12) -> never stuck
STATIONS = {1001: (10, 101), 1002: (6, 101), 1003: (8, 102), 1004: (4, 102)}


def _station_csv(sid: int, rng) -> pd.DataFrame:
    ts = pd.date_range(D0, periods=NDAYS * 288, freq="5min")
    n = len(ts); day = (ts - D0).days
    if sid == 1001:
        busy = rng.integers(0, 10, n).astype(float); busy[300] = 1000.0  # spike -> outlier
    elif sid == 1002:
        busy = np.full(n, 2.0)
    elif sid == 1003:
        busy = np.where(day < 2, rng.integers(0, 8, n), 3).astype(float)
    else:
        busy = np.full(n, np.nan); busy[np.arange(11) * 300] = 1.0
    idle = np.where(np.isnan(busy), np.nan, 1.0)
    df = pd.DataFrame({"time": ts.strftime("%Y-%m-%d %H:%M:%S"), "busy": busy, "idle": idle})
    for c in fp.RAW_COLS:
        if c not in df: df[c] = 0.5
    return df


@pytest.fixture()
def env(tmp_path, monkeypatch):
    src = tmp_path / "src"; (src / "charge_5min").mkdir(parents=True)
    rng = np.random.default_rng(0)
    for sid in STATIONS:
        _station_csv(sid, rng).to_csv(src / "charge_5min" / f"{sid}.csv", index=False)
    pd.DataFrame({"station_id": list(STATIONS), "longitude": 114.0, "latitude": 22.5,
                  "slow_count": [p // 2 for p, _ in STATIONS.values()],
                  "fast_count": [p - p // 2 for p, _ in STATIONS.values()],
                  "charge_count": [p for p, _ in STATIONS.values()],
                  "TAZID": [t for _, t in STATIONS.values()]}).to_csv(src / "station_information.csv", index=False)
    # 1001: 10 piles matches; 1002: 5 of 6 (mismatch); 1003: none; 1004: 4 piles
    piles = [(1001, 7.0, "AC")] * 5 + [(1001, 60.0, "DC")] * 5 + [(1002, 7.0, "AC")] * 5 + [(1004, 120.0, "DC")] * 4
    pd.DataFrame({"pile_id": [float(sid * 10000 + i) for i, (sid, _, _) in enumerate(piles)],
                  "power": [p for _, p, _ in piles], "pileType": [t for _, _, t in piles],
                  "station_id": [float(s) for s, _, _ in piles]}).to_csv(src / "pile_rated_power.csv", index=False)
    lake = tmp_path / "lake"
    monkeypatch.setattr(fp, "SRC", src); monkeypatch.setattr(fp, "LAKE", lake)
    monkeypatch.setattr(fp, "ProcessPoolExecutor", ThreadPoolExecutor)  # fast + allows patching
    return src, lake


def run(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["full_pipeline.py", "--workers", "2", "--group-size", "2", *args])
    fp.main()


def day(n: int) -> str:
    return (D0 + pd.Timedelta(days=n)).strftime("%Y-%m-%d")


def part(lake: Path, layer: str, dt: str) -> pd.DataFrame:
    sub = "ods/charge_5min" if layer == "ods" else "dwd/charge_5min_clean"
    return pq.read_table(lake / sub / f"dt={dt}" / "part-0.parquet").to_pandas()


def test_idempotent_rerun(env, monkeypatch):
    _, lake = env
    run(monkeypatch, "--ds", day(9))
    a = {k: part(lake, k, day(9)) for k in ("ods", "dwd")}
    run(monkeypatch, "--ds", day(9))
    for k in a:
        pd.testing.assert_frame_equal(a[k], part(lake, k, day(9)))
    assert len(a["ods"]) == len(STATIONS) * 288
    assert [p.name for p in (lake / "dwd/charge_5min_clean").iterdir()] == [f"dt={day(9)}"]  # no tmp leftovers


def test_single_day_equals_multi_day(env, monkeypatch, tmp_path):
    _, lake = env
    run(monkeypatch, "--start", day(6), "--end", day(13))
    multi = {d: part(lake, "dwd", day(d)) for d in (6, 9, 13)}
    for d in (6, 9, 13):
        monkeypatch.setattr(fp, "LAKE", tmp_path / f"lake_single_{d}")
        run(monkeypatch, "--ds", day(d))
        single = pq.read_table(tmp_path / f"lake_single_{d}/dwd/charge_5min_clean/dt={day(d)}/part-0.parquet").to_pandas()
        pd.testing.assert_frame_equal(single, multi[d])
    assert multi[9].query("station_id == 1003").is_stuck.eq(1).all()
    assert multi[6].query("station_id == 1003").is_stuck.eq(0).all()
    assert part(lake, "dwd", day(6)).query("station_id == 1001").is_outlier.sum() == 0  # spike is on day 1 only


def test_row_count_guard(env, monkeypatch):
    orig = fp._split_day
    monkeypatch.setattr(fp, "_split_day", lambda args: (lambda dt, n: (dt, n - 1))(*orig(args)))
    with pytest.raises(SystemExit, match="row mismatch"):
        run(monkeypatch, "--ds", day(9))


def _stats(src: Path, start: int, end: int):
    s, e = D0 + pd.Timedelta(days=start), D0 + pd.Timedelta(days=end + 1)
    out = []
    for sid, (p, taz) in STATIONS.items():
        _, _, st = fp._stage_one((str(src / "charge_5min" / f"{sid}.csv"), s, e, p, taz, None))
        out.append(st)
    return pd.DataFrame(out)


def test_stuck_rule_and_first_day_new_stuck(env):
    src, _ = env
    q = _stats(src, 0, NDAYS - 1).set_index("station_id")
    assert q.loc[1001, "stuck_days"] == 0
    assert q.loc[1002, "stuck_days"] == NDAYS        # constant every day
    assert q.loc[1003, "stuck_dates"][0] == day(8)   # needs a full 7-day constant window
    assert q.loc[1003, "stuck_days"] == NDAYS - 8
    assert q.loc[1004, "stuck_days"] == 0            # 11 identical valid points < 12
    dd = fp.daily_stuck(_stats(src, 6, 13)).set_index("dt")
    assert pd.isna(dd.loc[day(6), "new_stuck"])     # first day: no yesterday -> excluded
    assert dd.new_stuck.count() == len(dd) - 1
    assert dd.loc[day(8), "new_stuck"] == 1 and dd.loc[day(9), "new_stuck"] == 0
    assert dd.loc[day(8), "stuck"] == 2


def test_pile_power_into_station_info(env, monkeypatch):
    src, lake = env
    run(monkeypatch, "--ds", day(9))
    si = pd.read_parquet(lake / "ods/station_info/part-0.parquet").set_index("station_id")
    assert list(si.columns[:6]) == ["longitude", "latitude", "slow_count", "fast_count", "charge_count", "taz_id"]
    assert si.loc[1001, "rated_power_kw_total"] == pytest.approx(5 * 7 + 5 * 60)
    assert (si.loc[1001, "dc_pile_cnt"], si.loc[1001, "ac_pile_cnt"], si.loc[1001, "pile_cnt_from_power"]) == (5, 5, 10)
    assert si.loc[1003, "pile_cnt_from_power"] == 0 and pd.isna(si.loc[1003, "rated_power_kw_total"])
    pp = pd.read_parquet(lake / "ods/pile_power/part-0.parquet")
    assert str(pp.pile_id.dtype) == "int64" and str(pp.station_id.dtype) == "int32" and len(pp) == 19
    r = pile_power.build(src=src / "pile_rated_power.csv", lake=lake)  # rerun is idempotent
    assert r["charge_count_mismatch"] == 2  # 1002 (5 vs 6), 1003 (0 vs 8)
    assert pd.read_parquet(lake / "ods/station_info/part-0.parquet").shape == (4, 11)
