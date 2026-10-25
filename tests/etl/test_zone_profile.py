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
