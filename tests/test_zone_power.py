"""load_kw_est / power_coverage 口径：pandas（export_ads_parquet.zone_power）与 Spark（ads_metrics.ZONE_POWER_SQL）一致；
quality_station 默认只读覆盖最长的 run 目录。"""
import math
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))
import export_ads_parquet as e  # noqa: E402

INFO = pd.DataFrame({
    "station_id": [1, 2, 3, 4, 5],
    "taz_id": [10, 10, 20, 30, 30],
    "charge_count": [4, 6, 5, 2, 8],
    "rated_power_kw_total": [100.0, None, None, 50.0, 70.0],
})
# 片区 10：只有站 1 有功率 → kw=100，覆盖 4/10；片区 20：全无 → NULL，覆盖 0；片区 30：全有 → 120，覆盖 1
EXPECT = {"10": (100.0, 0.4), "20": (None, 0.0), "30": (120.0, 1.0)}


def _check(got):
    for taz, (kw, cov) in EXPECT.items():
        k, c = got[taz]
        assert (k is None or math.isnan(k)) if kw is None else k == pytest.approx(kw), (taz, k)
        assert c == pytest.approx(cov), (taz, c)


def test_zone_power_pandas():
    zp = e.zone_power(INFO)
    _check({r.taz_id: (None if pd.isna(r.kw_sum) else r.kw_sum, r.power_coverage) for r in zp.itertuples()})
    occ = pd.DataFrame({"taz_id": ["10", "20", "30", "99"], "occupancy": [0.5, 0.5, None, 0.5]})
    out = e.apply_zone_power(occ, zp)
    assert out.load_kw_est.iloc[0] == pytest.approx(50.0)
    assert out.load_kw_est.iloc[1:].isna().all()  # 全无功率 / 占用率空 / 维表里没有的片区 → NULL


def test_zone_power_spark_matches_pandas():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    import ads_metrics
    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    try:
        rows = [(str(r.taz_id), int(r.charge_count), None if pd.isna(r.rated_power_kw_total) else float(r.rated_power_kw_total))
                for r in INFO.itertuples()]
        spark.createDataFrame(rows, "taz_id STRING, total_piles INT, rated_power_kw_total DOUBLE").createOrReplaceTempView("station_info")
        rows = spark.sql(ads_metrics.ZONE_POWER_SQL.replace("ods.station_info", "station_info")).collect()
        _check({r.taz_id: (r.kw_sum, r.power_coverage) for r in rows})
    finally:
        spark.stop()


def test_quality_station_picks_full_run_only(tmp_path, monkeypatch):
    base = tmp_path / "dwd" / "quality_station"
    for run in ["2022-09-01_2022-09-01", "2022-09-01_2023-02-28", "2022-09-02_2022-09-02", "2022-09-02_2022-09-07"]:
        (base / f"run={run}").mkdir(parents=True)
        (base / f"run={run}" / "part-0.parquet").write_bytes(b"x")
    monkeypatch.delenv("SZ_QUALITY_STATION", raising=False)
    monkeypatch.setattr(e, "LAKE", tmp_path)
    assert e.quality_station_path() == str(base / "run=2022-09-01_2023-02-28")
