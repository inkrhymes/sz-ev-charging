"""Pile rated power -> ods/pile_power, and enrich ods/station_info.

Source: UrbanEV 20220901-20230228_station-raw/pile_rated_power.csv
  (pile_id,power,pileType,station_id; ids stored as floats)
Writes:
  data/lake/ods/pile_power/part-0.parquet  (pile_id int64, station_id int32, power_kw double, pile_type AC/DC)
  data/lake/ods/station_info/part-0.parquet  + rated_power_kw_total, dc_pile_cnt, ac_pile_cnt, pile_cnt_from_power
Existing station_info columns are kept unchanged; re-running is idempotent.
Usage: python etl/pile_power.py [--src CSV]   (lake root from SZ_EV_LAKE, default data/lake)
"""
from __future__ import annotations
import argparse, os
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))
SRC = Path(os.environ.get("URBANEV_SRC", "/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/20220901-20230228_station-raw")) / "pile_rated_power.csv"
NEW_COLS = ["rated_power_kw_total", "dc_pile_cnt", "ac_pile_cnt", "pile_cnt_from_power"]


def build(src: Path = SRC, lake: Path = LAKE) -> dict:
    raw = pd.read_csv(src)
    p = pd.DataFrame({"pile_id": raw.pile_id.round().astype("int64"),
                      "station_id": raw.station_id.round().astype("int32"),
                      "power_kw": raw.power.astype("float64"),
                      "pile_type": raw.pileType.astype(str).str.strip().str.upper()})
    assert p.pile_type.isin(["AC", "DC"]).all(), p.pile_type.unique()
    assert p.pile_id.is_unique
    out = lake / "ods" / "pile_power"; out.mkdir(parents=True, exist_ok=True)
    p.to_parquet(out / "part-0.parquet", index=False)

    agg = p.groupby("station_id").agg(rated_power_kw_total=("power_kw", "sum"),
                                      dc_pile_cnt=("pile_type", lambda s: int((s == "DC").sum())),
                                      ac_pile_cnt=("pile_type", lambda s: int((s == "AC").sum())),
                                      pile_cnt_from_power=("pile_id", "size")).reset_index()
    sp = lake / "ods" / "station_info" / "part-0.parquet"
    si = pd.read_parquet(sp)
    base = si.drop(columns=[c for c in NEW_COLS if c in si.columns])
    m = base.merge(agg, on="station_id", how="left")
    for c in NEW_COLS[1:]: m[c] = m[c].fillna(0).astype("int32")
    m["station_id"] = m["station_id"].astype("int32")
    assert len(m) == len(base)
    m.to_parquet(sp, index=False)
    mism = m[m.pile_cnt_from_power != m.charge_count]
    return dict(piles=len(p), stations_with_power=int(agg.station_id.nunique()),
                orphan_stations=int((~agg.station_id.isin(base.station_id)).sum()),
                station_info_rows=len(m), no_power=int((m.pile_cnt_from_power == 0).sum()),
                charge_count_mismatch=len(mism), mismatch_sample=mism[["station_id", "charge_count", "pile_cnt_from_power"]].head(10).to_dict("records"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--src", type=Path, default=SRC)
    print(build(ap.parse_args().src))
