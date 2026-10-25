"""质检逻辑单测：用极小的合成数据跑 etl/clean_to_dwd.py 原有逻辑（不改其代码）。"""
import importlib

import numpy as np
import pandas as pd
import pytest

cd = importlib.import_module("clean_to_dwd")


def test_high_missing_threshold_constant():
    assert cd.HIGH_MISSING_THR == 0.30


def test_iqr_outlier_mask_flags_extreme():
    s = pd.Series([1, 2, 2, 3, 2, 3, 2, 1, 2, 100, np.nan])
    m = cd.iqr_outlier_mask(s)
    assert bool(m.iloc[9]) and int(m.sum()) == 1
    assert not bool(m.iloc[10])  # NaN 不算异常


def test_iqr_outlier_mask_short_or_flat():
    assert not cd.iqr_outlier_mask(pd.Series([1, 100, 2])).any()
    assert not cd.iqr_outlier_mask(pd.Series([5.0] * 20)).any()


@pytest.fixture()
def synth(tmp_path, monkeypatch):
    """3 站 × 2 天 5 分钟：A 正常，B 读数不动，C 缺 50%。"""
    ts = pd.date_range("2022-09-01", periods=576, freq="5min")
    rng = np.random.default_rng(0)
    rows = []
    for sid in ("A", "B", "C"):
        for i, t in enumerate(ts):
            busy = {"A": rng.integers(0, 10), "B": 3}.get(sid, rng.integers(0, 10))
            missing = sid == "C" and i % 2 == 0
            rows.append({
                "station_id": sid, "ts": t,
                "busy": np.nan if missing else busy,
                "idle": np.nan if missing else 10 - busy,
                "s_price": 1.0, "e_price": 0.5,
                "duration": np.nan if missing else 1.0,
                "volume": np.nan if missing else 2.0,
            })
    demo = tmp_path / "data" / "demo"
    (demo / "ods").mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    pd.DataFrame(rows).to_csv(demo / "ods" / "ods_station_raw_5min.csv", index=False)
    pd.DataFrame({"station_id": ["A", "B", "C"], "taz_id": [1, 1, 2], "total_piles": [10, 10, 10]}).to_csv(
        demo / "station_info.csv", index=False)
    for k, v in {"ROOT": tmp_path, "DEMO": demo, "ODS": demo / "ods", "DWD": demo / "dwd",
                 "DOCS": tmp_path / "docs", "REPORT": tmp_path / "docs" / "quality_report.md",
                 "INFO": demo / "station_info.csv", "LIST_CSV": demo / "demo_station_list.csv"}.items():
        monkeypatch.setattr(cd, k, v)
    cd.main()
    return pd.read_csv(demo / "dwd" / "dwd_quality_station.csv", dtype={"station_id": str}).set_index("station_id")


def test_missing_rate(synth):
    assert synth.loc["A", "missing_rate"] == 0
    assert synth.loc["C", "missing_rate"] == pytest.approx(0.5)


def test_stuck_detection(synth):
    assert synth.loc["B", "stuck_flag"] == 1
    assert synth.loc["A", "stuck_flag"] == 0 and synth.loc["C", "stuck_flag"] == 0


def test_ticket_suggest(synth):
    assert synth.loc["B", "ticket_suggest"] == "repair"
    assert synth.loc["C", "ticket_suggest"] == "inspect"  # 0.5 > 0.30
    assert synth.loc["A", "ticket_suggest"] == "none"


def test_shipped_quality_file_matches_rules():
    """随仓 dwd_quality_station.csv 与质检报告口径一致：25 / 4 / 6 / 0.2642。"""
    from conftest import ROOT
    q = pd.read_csv(ROOT / "data/demo/dwd/dwd_quality_station.csv")
    assert len(q) == 25
    assert int(q["stuck_flag"].sum()) == 4
    assert int((q["missing_rate"] > cd.HIGH_MISSING_THR).sum()) == 6
    assert round(q["missing_rows"].sum() / q["rows"].sum(), 4) == 0.2642
