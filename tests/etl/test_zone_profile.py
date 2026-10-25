"""zone_profile.py on a synthetic 2-zone shapefile + POIs (no real data)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

shapefile = pytest.importorskip("shapefile")
pytest.importorskip("shapely")
import zone_profile as zp

# zone 1 near Futian, zone 2 near Longgang (1 km squares, EPSG:3857)
CENTERS = {1: (114.055, 22.522), 2: (114.247, 22.720)}


def _square(lon, lat, half=500):
    x, y = zp.to_3857([lon], [lat]); x, y = float(x[0]), float(y[0])
    return [[(x - half, y - half), (x - half, y + half), (x + half, y + half), (x + half, y - half), (x - half, y - half)]]


@pytest.fixture()
def env(tmp_path):
    supp = tmp_path / "supp"; (supp / "shenzhen_districts").mkdir(parents=True); lake = tmp_path / "lake"
    w = shapefile.Writer(str(supp / "shenzhen_districts" / "Shenzhen"), shapeType=shapefile.POLYGON)
    w.field("TAZID", "N", 10, 0)
    for t, (lon, lat) in CENTERS.items():
        w.poly(_square(lon, lat)); w.record(t)
    w.close()
    rows = []
    # zone 1: 40 food + 10 life + 10 biz -> food-dominated; zone 2: 30 biz + 10 life -> biz; 5 outside
    for t, mix in ((1, {"food and beverage services": 40, "lifestyle services": 10, "business and residential": 10}),
                   (2, {"business and residential": 30, "lifestyle services": 10})):
        lon, lat = CENTERS[t]
        for typ, n in mix.items():
            rows += [(typ, lon + 0.001 * np.sin(i), lat + 0.001 * np.cos(i)) for i in range(n)]
    rows += [("lifestyle services", 113.0, 22.0)] * 5
    pd.DataFrame(rows, columns=["primary_types", "longitude", "latitude"]).to_csv(supp / "20221201-shenzhen-poi.csv", index=False)
    (lake / "ods/station_info").mkdir(parents=True)
    pd.DataFrame({"station_id": [11, 12, 21], "taz_id": [1, 1, 2],
                  "longitude": [114.055, 114.056, 114.247], "latitude": [22.522, 22.523, 22.720],
                  "charge_count": [10, 6, 4], "fast_count": [5, 0, 4],
                  "rated_power_kw_total": [500.0, 42.0, 0.0]}).to_parquet(lake / "ods/station_info/part-0.parquet")
    return supp, lake


def test_function_labels(env):
    supp, lake = env
    z = zp.function_labels(supp, lake).set_index("taz_id")
    assert list(z.index) == [1, 2]
    assert z.loc[1, ["poi_food", "poi_life", "poi_biz_res", "poi_total"]].tolist() == [40, 10, 10, 60]
    assert z.loc[1, "function_label"] == "餐饮为主" and z.loc[2, "function_label"] == "商务住宅为主"
    assert z.loc[1, "district"] == "福田区" and z.loc[2, "district"] == "龙岗区"
    assert abs(z.loc[1, "area_km2"] - 1.0) < 1e-6 and z.loc[1, "poi_density_km2"] == 60.0
    assert "倍" in z.loc[1, "label_reason"]
    poi = pd.read_parquet(lake / "ods/poi")
    assert len(poi) == 105 and poi.taz_id.isna().sum() == 5
    assert (lake / "ads/zone_function_label/part-0.parquet").exists()


def test_few_poi_and_nearest_district():
    assert list(zp.nearest_district([113.93, 114.48], [22.53, 22.59])) == ["南山区", "大鹏新区"]


def test_site_suggest(env, tmp_path):
    supp, lake = env
    zp.function_labels(supp, lake)
    raw = tmp_path / "raw"; raw.mkdir()
    ids = [11, 12, 21]
    pd.DataFrame([[0, 150, 30000], [150, 0, 30000], [30000, 30000, 0]], index=pd.Index(ids, name="ID"),
                 columns=ids).to_csv(raw / "station_distance.csv")
    ts = pd.date_range("2022-09-01", periods=48, freq="h")
    rows = []
    for t, busy_eve, busy_day in ((1, 0.95, 0.5), (2, 0.1, 0.05)):
        for x in ts:
            rows.append((t, x, busy_eve if 18 <= x.hour <= 22 else busy_day, x.hour))
    (lake / "dwd/zone_price_hour").mkdir(parents=True)
    pd.DataFrame(rows, columns=["taz_id", "ts", "occupancy", "hour"]).to_parquet(lake / "dwd/zone_price_hour/part-0.parquet")
    z = zp.site_suggest(raw, lake, "2026-10-25")
    assert list(z.columns) == zp.OUT_COLS
    assert (lake / "ads/zone_site_suggest/run_dt=2026-10-25/part-0.parquet").exists()
    z = z.set_index("taz_id")
    assert z.loc[1, "rank"] == 1 and z.loc[1, "piles"] == 16 and z.loc[2, "piles"] == 4
    assert z.loc[1, "rated_kw"] == 542.0 and pd.isna(z.loc[2, "rated_kw"])
    assert abs(z.loc[1, "avg_occupancy"] - (0.95 * 5 + 0.5 * 19) / 24) < 1e-4
    assert z.loc[1, "peak_occupancy"] == 0.95 and z.loc[1, "busy_hours_ratio"] == round(5 / 24, 4)
    # 2 zones: pct ranks are 100 / 50; zone1 more demand, more supply (16 vs 4 piles in 2 km)
    assert z.loc[1, "demand_score"] == 100 and z.loc[1, "supply_score"] == 100
    assert z.loc[1, "gap_score"] == 70.0 and z.loc[2, "gap_score"] == pytest.approx(0.7 * 50 + 0.3 * 50)
    assert z.loc[1, "suggest_add_piles"] == 0  # gap 70 < 75 -> watch, not add
    assert "观察" in z.loc[1, "reason"] and "维持现状" in z.loc[2, "reason"]
    assert z.loc[1, "function_tag"] == "餐饮为主"


def test_add_piles_count(env, tmp_path):
    supp, lake = env
    zp.function_labels(supp, lake)
    raw = tmp_path / "raw"; raw.mkdir(); ids = [11, 12, 21]
    pd.DataFrame(30000, index=pd.Index(ids, name="ID"), columns=ids).to_csv(raw / "station_distance.csv")
    ts = pd.date_range("2022-09-01", periods=48, freq="h")
    rows = [(t, x, (0.98 if t == 1 else 0.1), x.hour) for t in (1, 2) for x in ts]
    (lake / "dwd/zone_price_hour").mkdir(parents=True)
    pd.DataFrame(rows, columns=["taz_id", "ts", "occupancy", "hour"]).to_parquet(lake / "dwd/zone_price_hour/part-0.parquet")
    z = zp.site_suggest(raw, lake, "x").set_index("taz_id")
    # zone1 supply 0 within 2 km except itself: pile counts 10,6 / 4 -> supply ranks; gap high
    assert z.loc[1, "gap_score"] >= 75
    assert z.loc[1, "suggest_add_piles"] == int(np.ceil(16 * 0.98 / 0.7 - 16))  # 7
    assert "建议加 7 根桩" in z.loc[1, "reason"]
