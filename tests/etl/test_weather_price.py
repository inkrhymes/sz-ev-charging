"""weather_price.py on a tiny synthetic lake (2 zones, 6 days, no real data)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import weather_price as wp

D0 = pd.Timestamp("2022-09-01"); NDAYS = 6


@pytest.fixture()
def lake(tmp_path):
    supp = tmp_path / "supp"; supp.mkdir(); lake = tmp_path / "lake"
    hrs = pd.date_range(D0, periods=NDAYS * 24, freq="h")
    for st in ("airport", "central"):
        rain = np.where((hrs.day % 2 == 0), 1.5, 0.0)          # every other day rains all day
        temp = np.where(hrs.day == 1, 32.0, 25.0)               # first day hot
        df = pd.DataFrame({"time": [f"{t.year}/{t.month}/{t.day} {t.hour}:00" for t in hrs], "T": temp,
                           "P0": 755.0, "P": 756.0, "U": 80, "RAIN": rain, "Td": 20.0})
        df.drop(index=5).to_csv(supp / f"x_weather_{st}.csv", index=False)  # one missing hour -> interpolated
    (lake / "ods/station_info").mkdir(parents=True)
    pd.DataFrame({"station_id": [1, 2], "longitude": [113.82, 114.06], "latitude": [22.64, 22.54],
                  "charge_count": [10, 4], "taz_id": [1, 2]}).to_parquet(lake / "ods/station_info/part-0.parquet")
    ts = pd.date_range(D0, periods=NDAYS * 288, freq="5min")
    for d in range(NDAYS):
        sel = ts[(ts >= D0 + pd.Timedelta(days=d)) & (ts < D0 + pd.Timedelta(days=d + 1))]
        rows = []
        for sid, piles in ((1, 10), (2, 4)):
            rain = (sel.day % 2 == 0)
            busy = np.where(rain, 6.0, 3.0) if sid == 1 else np.full(len(sel), 2.0)  # zone 1: +0.3 occupancy when raining
            rows.append(pd.DataFrame({"station_id": sid, "taz_id": sid, "ts": sel, "busy": busy, "total_piles": piles,
                                      "e_price": np.where(sel.hour >= 10, 1.0, 0.5), "s_price": 0.5,
                                      "occupancy": np.nan, "quality_flag": "ok"}))
        p = lake / "dwd/charge_5min_clean" / f"dt={(D0 + pd.Timedelta(days=d)):%Y-%m-%d}"; p.mkdir(parents=True)
        pd.concat(rows).to_parquet(p / "part-0.parquet", index=False)
    return supp, lake


def test_weather_price_end_to_end(lake):
    supp, lk = lake
    r = wp.run(supp, lk, D0.strftime("%Y-%m-%d"), (D0 + pd.Timedelta(days=NDAYS - 1)).strftime("%Y-%m-%d"))
    w = pd.read_parquet(lk / "dwd/weather_hourly/part-0.parquet")
    assert len(w) == 2 * NDAYS * 24 and w.temp_c.notna().all() and w.filled.sum() == 2
    zs = wp.zone_station(lk).set_index("taz_id").weather_station.to_dict()
    assert zs == {1: "airport", 2: "central"}
    zh = pd.read_parquet(lk / "dwd/zone_price_hour/part-0.parquet")
    assert len(zh) == 2 * NDAYS * 24 and zh.occupancy.notna().all()     # recomputed, not taken from dwd NaN
    assert set(zh[zh.hour < 8].tou) == {"valley"} and set(zh[zh.hour == 10].tou) == {"peak"}
    e = pd.read_parquet(lk / "ads/zone_condition_effect/part-0.parquet").set_index(["scope", "condition", "taz_id"])
    z1 = e.loc[("zone", "rain:rain_vs_dry", 1)]
    assert z1.hour_adj_diff == pytest.approx(0.3) and z1.n_cond == 72 and z1.n_base == 72
    assert e.loc[("zone", "rain:rain_vs_dry", 2)].hour_adj_diff == pytest.approx(0.0)
    city = e.xs("city", level="scope").reset_index(level="taz_id", drop=True)
    assert city.loc["rain:rain_vs_dry"].hour_adj_diff == pytest.approx(0.15)
    assert np.isnan(city.loc["tou:peak_vs_valley"].hour_adj_diff)       # TOU is defined by hour: no within-hour contrast
    assert r["zones"] == 2


def test_tou_schedule():
    assert list(wp.tou([0, 7, 8, 10, 12, 14, 18, 19, 23])) == ["valley", "valley", "flat", "peak", "flat", "peak", "peak", "flat", "flat"]
