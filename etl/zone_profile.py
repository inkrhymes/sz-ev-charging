#!/usr/bin/env python3
"""Zone function labels (POI + TAZ polygons) and charging site suggestions.

Step 4  `function`: ods/poi, ads/zone_function_label
Step 5  `suggest` : ads/zone_site_suggest   (needs step 4 + dwd/zone_price_hour from weather_price.py)

Usage:
  python etl/zone_profile.py function [--supp DIR]
  python etl/zone_profile.py suggest  [--raw DIR]
Lake root = $SZ_EV_LAKE (default data/lake). Memory < 1 GB.

Notes
- Shenzhen.shp is the TAZ polygon layer (TAZID, EPSG:3857 metres); it has no district names.
  District = nearest district government seat to the zone centre (approximate, see DISTRICT_SEATS).
- The POI file has only 3 categories: lifestyle services / business and residential / food and beverage services.
"""
from __future__ import annotations
import argparse, os
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))
SUPP = Path(os.environ.get("URBANEV_SUPP", "/workspace/urbanev_supp/UrbanEVSupplemental"))
RAW = Path(os.environ.get("URBANEV_SRC", "/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/20220901-20230228_station-raw"))

POI_CAT = {"lifestyle services": "life", "business and residential": "biz_res", "food and beverage services": "food"}
CAT_CN = {"life": "生活服务", "biz_res": "商务住宅", "food": "餐饮"}
LQ_MAIN = 1.2          # category share >= 1.2x city share -> "<cat>为主"
DISTRICT_SEATS = {     # approx. anchor points (district seat + main subdistricts), lon/lat
    "福田区": [(114.055, 22.522), (114.04, 22.57), (114.01, 22.53)],
    "罗湖区": [(114.131, 22.548), (114.15, 22.57), (114.11, 22.58)],
    "南山区": [(113.930, 22.533), (113.95, 22.58), (113.91, 22.49), (113.98, 22.54)],
    "宝安区": [(113.884, 22.555), (113.87, 22.58), (113.82, 22.67), (113.83, 22.73), (113.93, 22.68), (113.85, 22.62)],
    "龙岗区": [(114.247, 22.720), (114.12, 22.61), (114.20, 22.65), (114.30, 22.77), (114.13, 22.69), (114.25, 22.62)],
    "盐田区": [(114.236, 22.557), (114.22, 22.55), (114.30, 22.60)],
    "龙华区": [(114.044, 22.696), (114.06, 22.73), (114.04, 22.62), (114.00, 22.65)],
    "坪山区": [(114.346, 22.708), (114.35, 22.69), (114.40, 22.70)],
    "光明区": [(113.936, 22.749), (113.89, 22.78), (113.95, 22.79)],
    "大鹏新区": [(114.479, 22.588), (114.42, 22.63), (114.48, 22.54), (114.55, 22.53)]}
EVENING_H = range(18, 23)  # 18:00-22:59
NEAR_M = 2000


def to_3857(lon, lat):
    lon = np.asarray(lon, float); lat = np.asarray(lat, float)
    return lon * 20037508.34 / 180, np.log(np.tan((90 + lat) * np.pi / 360)) * 6378137.0


def read_taz(shp: Path) -> tuple[pd.DataFrame, list]:
    import shapefile
    from shapely.geometry import shape
    r = shapefile.Reader(str(shp))
    rec = pd.DataFrame(r.records(), columns=[f[0] for f in r.fields[1:]])
    geoms = [shape(s.__geo_interface__) for s in r.shapes()]
    taz = pd.DataFrame({"taz_id": rec.TAZID.round().astype("int32"), "area_km2": [g.area / 1e6 for g in geoms]})
    return taz, geoms


def nearest_district(lon, lat):
    names = [n for n, pts in DISTRICT_SEATS.items() for _ in pts]
    c = np.array([p for pts in DISTRICT_SEATS.values() for p in pts])
    dx = (np.asarray(lon)[:, None] - c[:, 0]) * np.cos(np.radians(22.6)); dy = np.asarray(lat)[:, None] - c[:, 1]
    return np.array(names)[np.argmin(dx * dx + dy * dy, axis=1)]


def function_labels(supp: Path, lake: Path) -> pd.DataFrame:
    import shapely
    taz, geoms = read_taz(supp / "shenzhen_districts" / "Shenzhen.shp")
    poi = pd.read_csv(next(supp.glob("*poi*.csv")))
    poi["cat"] = poi.primary_types.map(POI_CAT).fillna("other")
    x, y = to_3857(poi.longitude, poi.latitude)
    tree = shapely.STRtree(geoms)
    pi, gi = tree.query(shapely.points(x, y), predicate="within")
    poi["taz_id"] = pd.array([pd.NA] * len(poi), dtype="Int32")
    poi.loc[pi, "taz_id"] = taz.taz_id.to_numpy()[gi]
    d = lake / "ods" / "poi"; d.mkdir(parents=True, exist_ok=True)
    poi.rename(columns={"primary_types": "poi_type"})[["poi_type", "cat", "longitude", "latitude", "taz_id"]].to_parquet(d / "part-0.parquet", index=False)

    cnt = poi.dropna(subset=["taz_id"]).pivot_table(index="taz_id", columns="cat", values="longitude", aggfunc="size", fill_value=0)
    for c in CAT_CN: cnt[c] = cnt.get(c, 0)
    z = taz.merge(cnt[list(CAT_CN)].reset_index(), on="taz_id", how="left").fillna({c: 0 for c in CAT_CN})
    z = z.rename(columns={c: f"poi_{c}" for c in CAT_CN})
    cols = [f"poi_{c}" for c in CAT_CN]
    z[cols] = z[cols].astype("int32"); z["poi_total"] = z[cols].sum(axis=1).astype("int32")
    z["poi_density_km2"] = (z.poi_total / z.area_km2.where(z.area_km2 > 0)).round(1)
    city = z[cols].sum() / z.poi_total.sum()
    for c in CAT_CN:
        z[f"share_{c}"] = (z[f"poi_{c}"] / z.poi_total.where(z.poi_total > 0)).round(4)
    lq = pd.DataFrame({c: z[f"share_{c}"] / city[f"poi_{c}"] for c in CAT_CN})
    top = lq.fillna(0).idxmax(axis=1).where(z.poi_total > 0); topv = lq.max(axis=1)
    q1, q2 = z.loc[z.poi_total > 0, "poi_density_km2"].quantile([1/3, 2/3])
    dens = np.where(z.poi_total == 0, "无", np.where(z.poi_density_km2 >= q2, "高", np.where(z.poi_density_km2 >= q1, "中", "低")))
    main = np.where(z.poi_total < 20, "POI很少", np.where(topv >= LQ_MAIN, top.map(CAT_CN).fillna("") + "为主", "混合"))
    z["density_level"] = dens; z["function_label"] = main
    z["label_reason"] = [
        "区内 POI 不到 20 个，样本太少，不下结论" if t < 20 else
        (f"{CAT_CN[k]}占 {s:.0%}，是全市平均（{city[f'poi_{k}']:.0%}）的 {v:.1f} 倍" if v >= LQ_MAIN else
         f"三类 POI 占比都接近全市平均（最高是{CAT_CN[k]}，{v:.1f} 倍）") + f"；POI 密度 {d:.0f} 个/km²，全市属{lv}"
        for t, k, v, s, d, lv in zip(z.poi_total, top.fillna("life"), topv.fillna(0),
                                     [z.loc[i, f"share_{k}"] if pd.notna(k) else 0 for i, k in zip(z.index, top)],
                                     z.poi_density_km2.fillna(0), dens)]
    # district: centre = mean of charging-station coords if the zone has stations, else polygon centroid
    si = pd.read_parquet(lake / "ods" / "station_info" / "part-0.parquet")
    sc = si.groupby("taz_id")[["longitude", "latitude"]].mean()
    cen = np.array([[g.centroid.x, g.centroid.y] for g in geoms])
    lon = cen[:, 0] * 180 / 20037508.34; lat = np.degrees(2 * np.arctan(np.exp(cen[:, 1] / 6378137.0)) - np.pi / 2)
    z["center_lon"] = z.taz_id.map(sc.longitude).fillna(pd.Series(lon, index=z.index)).round(6)
    z["center_lat"] = z.taz_id.map(sc.latitude).fillna(pd.Series(lat, index=z.index)).round(6)
    z["district"] = nearest_district(z.center_lon, z.center_lat)
    z["district_method"] = "nearest_seat_approx"
    z["has_station"] = z.taz_id.isin(si.taz_id).astype("int8")
    z["area_km2"] = z.area_km2.round(3)
    d = lake / "ads" / "zone_function_label"; d.mkdir(parents=True, exist_ok=True)
    z.to_parquet(d / "part-0.parquet", index=False)
    return z


def site_suggest(raw: Path, lake: Path) -> pd.DataFrame:
    zh = pd.read_parquet(lake / "dwd" / "zone_price_hour" / "part-0.parquet", columns=["taz_id", "ts", "occupancy", "hour"])
    ev = zh[zh.hour.isin(EVENING_H)]
    occ = zh.groupby("taz_id").occupancy.agg(occ_avg="mean", occ_p90="quantile")
    occ["occ_p90"] = zh.groupby("taz_id").occupancy.quantile(.9)
    occ["occ_evening_p90"] = ev.groupby("taz_id").occupancy.quantile(.9)
    hp = zh.groupby(["taz_id", "hour"]).occupancy.mean().unstack()
    occ["peak_hour"] = hp.idxmax(axis=1).astype("int8"); occ["peak_hour_occ"] = hp.max(axis=1)
    occ["hours_observed"] = zh.groupby("taz_id").occupancy.count()
    si = pd.read_parquet(lake / "ods" / "station_info" / "part-0.parquet")
    st = si.groupby("taz_id").agg(station_cnt=("station_id", "size"), pile_cnt=("charge_count", "sum"),
                                  fast_cnt=("fast_count", "sum"), rated_power_kw=("rated_power_kw_total", "sum"))
    # nearby supply: piles at stations within 2 km of each station (incl. itself), averaged over the zone
    dist = pd.read_csv(raw / "station_distance.csv", index_col=0, dtype=np.float32)
    dist.index = dist.index.astype(int); dist.columns = dist.columns.astype(int)
    piles = si.set_index("station_id").charge_count.reindex(dist.columns).fillna(0).to_numpy(np.float32)
    near = ((dist.to_numpy() <= NEAR_M) @ piles)
    near = pd.Series(near, index=dist.index).reindex(si.station_id).to_numpy(); del dist
    st["piles_within_2km"] = pd.Series(near, index=si.index).groupby(si.taz_id).mean().round(1)
    fl = pd.read_parquet(lake / "ads" / "zone_function_label" / "part-0.parquet")
    z = occ.join(st, how="inner").reset_index().merge(
        fl[["taz_id", "district", "function_label", "poi_total", "poi_density_km2", "share_life", "share_biz_res", "share_food", "area_km2"]],
        on="taz_id", how="left")
    z["piles_per_km2"] = (z.pile_cnt / z.area_km2.where(z.area_km2 > 0)).round(2)
    z["rated_power_kw"] = z.rated_power_kw.where(z.rated_power_kw > 0)
    # transparent score: average of three percentile ranks (0-1), higher = more need for piles
    z["r_evening"] = z.occ_evening_p90.rank(pct=True)
    z["r_avg"] = z.occ_avg.rank(pct=True)
    z["r_supply"] = 1 - z.piles_within_2km.rank(pct=True)
    z["score"] = (0.5 * z.r_evening + 0.3 * z.r_avg + 0.2 * z.r_supply).round(4)
    z["score_formula"] = "0.5*晚高峰P90占用率排名 + 0.3*平均占用率排名 + 0.2*(1-周边2km桩数排名)"
    sug, why = [], []
    for r in z.itertuples():
        base = (f"晚高峰（18–23 点）忙的时候占用率到 {r.occ_evening_p90:.0%}，平时平均 {r.occ_avg:.0%}；"
                f"区内 {r.pile_cnt} 根桩，周边 2 公里平均 {r.piles_within_2km:.0f} 根")
        if r.score >= 0.75 and r.occ_evening_p90 >= 0.6:
            sug.append("add_piles"); why.append(base + "。晚上经常排满、周边桩也不多，建议加桩。")
        elif r.score >= 0.6 or r.occ_evening_p90 >= 0.6:
            sug.append("watch"); why.append(base + "。有时比较紧张，先观察，看后面几个月会不会更忙。")
        else:
            sug.append("keep"); why.append(base + "。桩够用，维持现状。")
    z["suggestion"] = sug; z["reason"] = why
    z = z.sort_values("score", ascending=False).reset_index(drop=True); z.insert(1, "rank", np.arange(1, len(z) + 1, dtype="int32"))
    z["taz_id"] = z.taz_id.astype("int32")
    d = lake / "ads" / "zone_site_suggest"; d.mkdir(parents=True, exist_ok=True)
    z.to_parquet(d / "part-0.parquet", index=False)
    return z


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("step", choices=["function", "suggest"])
    ap.add_argument("--supp", type=Path, default=SUPP); ap.add_argument("--raw", type=Path, default=RAW)
    a = ap.parse_args()
    if a.step == "function":
        z = function_labels(a.supp, LAKE)
        print(len(z), z.function_label.value_counts().to_dict(), z.district.value_counts().to_dict(), z.density_level.value_counts().to_dict())
    else:
        z = site_suggest(a.raw, LAKE)
        print(len(z), z.suggestion.value_counts().to_dict(), z.score.describe().round(3).to_dict())
