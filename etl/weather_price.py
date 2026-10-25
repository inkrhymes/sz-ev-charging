#!/usr/bin/env python3
"""Weather + price -> lake, and occupancy-under-conditions analysis.

Writes (lake root = $SZ_EV_LAKE, default data/lake):
  ods/weather/station=<airport|central>/part-0.parquet   raw hourly CSV as Parquet (+ ts)
  dwd/weather_hourly/part-0.parquet       station, ts, temp_c, dew_c, humidity_pct, rain_mm, pressure_*, is_rain, is_hot
  dwd/zone_weather_hour/part-0.parquet    taz_id, ts, weather station of nearest centroid + its readings
  dwd/zone_price_hour/part-0.parquet      taz_id, ts, occupancy, e_price, s_price, price, tou, price_level
  ads/zone_condition_effect/part-0.parquet  rain/hot/peak/high-price effect on occupancy, per zone + city

Usage:
  python etl/weather_price.py --supp /workspace/urbanev_supp/UrbanEVSupplemental [--start D --end D]
Effects are compared inside the same (zone, hour-of-day) cell first, then averaged, so the
daily rhythm (night empty / day busy) does not leak into the result.
"""
from __future__ import annotations
import argparse, os
from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))
SUPP = Path(os.environ.get("URBANEV_SUPP", "/workspace/urbanev_supp/UrbanEVSupplemental"))
# approx. locations (dataset gives none): Bao'an airport station / Futian city-centre station
WEATHER_STATIONS = {"airport": (113.811, 22.639), "central": (114.058, 22.543)}
HOT_C = 30.0
# Shenzhen commercial TOU (2022): peak 10-12,14-19; valley 0-8; rest flat
PEAK_H = set(range(10, 12)) | set(range(14, 19)); VALLEY_H = set(range(0, 8))
MIN_N = 3  # min hours on each side of a (zone, hour) cell


def tou(hour):
    h = np.asarray(hour)
    return np.where(np.isin(h, list(PEAK_H)), "peak", np.where(np.isin(h, list(VALLEY_H)), "valley", "flat"))


def load_weather(supp: Path, lake: Path) -> pd.DataFrame:
    out = []
    for st in WEATHER_STATIONS:
        f = next(supp.glob(f"*weather_{st}.csv"))
        raw = pd.read_csv(f)
        raw.insert(0, "ts", pd.to_datetime(raw.pop("time"), format="%Y/%m/%d %H:%M").astype("datetime64[us]"))
        d = lake / "ods" / "weather" / f"station={st}"; d.mkdir(parents=True, exist_ok=True)
        raw.to_parquet(d / "part-0.parquet", index=False)
        w = pd.DataFrame({"station": st, "ts": raw.ts.dt.floor("h"), "temp_c": raw["T"], "dew_c": raw["Td"],
                          "humidity_pct": raw["U"], "rain_mm": raw["RAIN"],
                          "pressure_station_mmhg": raw["P0"], "pressure_sea_mmhg": raw["P"]})
        w = w.groupby(["station", "ts"], as_index=False).mean()  # one row per hour
        full = pd.DataFrame({"ts": pd.date_range(w.ts.min(), w.ts.max(), freq="h").astype("datetime64[us]")})
        w = full.merge(w.drop(columns="station"), on="ts", how="left")
        w["filled"] = w.temp_c.isna().astype("int8")
        w = w.set_index("ts").interpolate(limit=3).reset_index(); w.insert(0, "station", st)
        out.append(w)
    w = pd.concat(out, ignore_index=True)
    w["is_rain"] = (w.rain_mm > 0).astype("int8"); w["is_hot"] = (w.temp_c >= HOT_C).astype("int8")
    d = lake / "dwd" / "weather_hourly"; d.mkdir(parents=True, exist_ok=True)
    w.to_parquet(d / "part-0.parquet", index=False)
    return w


def zone_station(lake: Path) -> pd.DataFrame:
    si = pd.read_parquet(lake / "ods" / "station_info" / "part-0.parquet")
    z = si.groupby("taz_id", as_index=False)[["longitude", "latitude"]].mean()
    dist = {}
    for st, (lon, lat) in WEATHER_STATIONS.items():
        dx = (z.longitude - lon) * 111.32 * np.cos(np.radians(22.6)); dy = (z.latitude - lat) * 110.57
        dist[st] = np.hypot(dx, dy)
    dd = pd.DataFrame(dist)
    z["weather_station"] = dd.idxmin(axis=1); z["weather_km"] = dd.min(axis=1).round(2)
    return z


def zone_hour(lake: Path, start: str, end: str) -> pd.DataFrame:
    dset = ds.dataset(lake / "dwd" / "charge_5min_clean", partitioning="hive")
    out = []
    for dt in pd.date_range(start, end).strftime("%Y-%m-%d"):
        t = dset.to_table(columns=["taz_id", "ts", "busy", "total_piles", "e_price", "s_price", "quality_flag"],
                          filter=ds.field("dt") == dt).to_pandas()
        if t.empty: continue
        t = t[(t.quality_flag == "ok") & (t.total_piles > 0)].copy()
        # recompute: dwd.occupancy before the operator-precedence fix is NaN for even pile counts
        t["occupancy"] = t.busy / t.total_piles
        t["ts"] = t.ts.dt.floor("h")
        out.append(t.groupby(["taz_id", "ts"], as_index=False).agg(
            occupancy=("occupancy", "mean"), e_price=("e_price", "mean"), s_price=("s_price", "mean"), n5=("occupancy", "size")))
    zh = pd.concat(out, ignore_index=True)
    zh["price"] = zh.e_price + zh.s_price
    zh["hour"] = zh.ts.dt.hour; zh["tou"] = tou(zh.hour)
    med = zh.groupby(["taz_id", "hour"]).price.transform("median")
    zh["price_level"] = np.where(zh.price > med + 1e-9, "high", np.where(zh.price < med - 1e-9, "low", "mid"))
    return zh


def effect(df: pd.DataFrame, cond: str, a, b, by_zone: bool) -> pd.DataFrame:
    """Occupancy when df[cond]==a vs ==b. hour_adj_diff: diff inside each (zone, hour-of-day)
    cell with >= MIN_N hours on both sides, averaged weighted by the smaller side. For the city
    row the cells are all (zone, hour) pairs. NaN when no cell has both sides (e.g. TOU, which
    is defined by hour)."""
    d = df[df[cond].isin([a, b])].dropna(subset=["occupancy"])
    cell = d.groupby(["taz_id", "hour", cond]).occupancy.agg(["mean", "size"]).unstack(cond)
    for col in [("mean", a), ("mean", b), ("size", a), ("size", b)]:
        if col not in cell.columns: cell[col] = np.nan
    cell = cell[(cell[("size", a)] >= MIN_N) & (cell[("size", b)] >= MIN_N)]
    cell = pd.DataFrame({"diff": cell[("mean", a)] - cell[("mean", b)],
                         "w": np.minimum(cell[("size", a)], cell[("size", b)])}).reset_index()
    def row(sub, cs, taz):
        sa, sb = sub[sub[cond] == a].occupancy, sub[sub[cond] == b].occupancy
        return dict(taz_id=taz, occ_cond=sa.mean(), occ_base=sb.mean(), n_cond=len(sa), n_base=len(sb),
                    raw_diff=sa.mean() - sb.mean(),
                    hour_adj_diff=float(np.average(cs["diff"], weights=cs.w)) if len(cs) else np.nan,
                    cells_used=len(cs))
    if by_zone:
        cg = dict(tuple(cell.groupby("taz_id")))
        rows = [row(g, cg.get(t, cell.iloc[:0]), t) for t, g in d.groupby("taz_id")]
    else:
        rows = [row(d, cell, pd.NA)]
    r = pd.DataFrame(rows)
    r.insert(0, "scope", "zone" if by_zone else "city"); r.insert(1, "condition", f"{cond}:{a}_vs_{b}")
    return r


def run(supp: Path, lake: Path, start: str, end: str) -> dict:
    w = load_weather(supp, lake)
    z = zone_station(lake)
    zh = zone_hour(lake, start, end)
    zw = zh[["taz_id", "ts"]].merge(z[["taz_id", "weather_station", "weather_km"]], on="taz_id")
    zw = zw.merge(w.rename(columns={"station": "weather_station"}), on=["weather_station", "ts"], how="left")
    for sub, df in (("zone_weather_hour", zw), ("zone_price_hour", zh)):
        d = lake / "dwd" / sub; d.mkdir(parents=True, exist_ok=True)
        df.to_parquet(d / "part-0.parquet", index=False)
    m = zh.merge(zw[["taz_id", "ts", "is_rain", "is_hot"]], on=["taz_id", "ts"], how="left")
    m["rain"] = m.is_rain.map({1: "rain", 0: "dry"}); m["heat"] = m.is_hot.map({1: "hot", 0: "normal"})
    specs = [("rain", "rain", "dry"), ("heat", "hot", "normal"), ("tou", "peak", "valley"), ("price_level", "high", "low")]
    res = pd.concat([effect(m, c, a, b, k) for c, a, b in specs for k in (False, True)], ignore_index=True)
    res["taz_id"] = res.taz_id.astype("Int32")
    d = lake / "ads" / "zone_condition_effect"; d.mkdir(parents=True, exist_ok=True)
    res.to_parquet(d / "part-0.parquet", index=False)
    return dict(weather_rows=len(w), zone_hour_rows=len(zh), zones=int(zh.taz_id.nunique()),
                zone_station=z.weather_station.value_counts().to_dict(), effect_rows=len(res),
                city=res[res.scope == "city"].drop(columns=["scope", "taz_id"]).round(4).to_dict("records"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--supp", type=Path, default=SUPP)
    ap.add_argument("--start", default="2022-09-01"); ap.add_argument("--end", default="2023-02-28")
    a = ap.parse_args()
    import json; print(json.dumps(run(a.supp, LAKE, a.start, a.end), ensure_ascii=False, indent=1, default=str))
