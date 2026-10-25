#!/usr/bin/env python3
"""Load data/demo/raw/*.csv into a pandas ODS frame; write CSV under data/demo/ods/.

File-based first deliverable — no live MySQL required.
TODO: INSERT INTO ods_station_raw_5min / ods_station_info when MySQL is up.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
RAW = DEMO / "raw"
ODS = DEMO / "ods"
INFO = DEMO / "station_info.csv"
IDS_FILE = DEMO / "demo_station_ids.txt"

RAW_COLS = [
    "station_id",
    "ts",
    "busy",
    "idle",
    "s_price",
    "e_price",
    "fast_busy",
    "fast_idle",
    "slow_busy",
    "slow_idle",
    "duration",
    "volume",
    "src_file",
    "load_at",
]


def load_ids() -> list[str]:
    return [ln.strip() for ln in IDS_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()]


def main() -> None:
    if not RAW.is_dir():
        raise SystemExit(f"missing {RAW}; run generate_demo_subset.py first")

    ODS.mkdir(parents=True, exist_ok=True)
    ids = load_ids()
    load_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    frames: list[pd.DataFrame] = []

    for sid in ids:
        path = RAW / f"{sid}.csv"
        if not path.is_file():
            raise SystemExit(f"missing raw file: {path}")
        df = pd.read_csv(path)
        # UrbanEV column `time` → ods `ts`
        if "time" in df.columns:
            df = df.rename(columns={"time": "ts"})
        df["station_id"] = sid
        df["src_file"] = path.name
        df["load_at"] = load_at
        for c in RAW_COLS:
            if c not in df.columns:
                df[c] = None
        frames.append(df[RAW_COLS])

    ods = pd.concat(frames, ignore_index=True)
    out_5min = ODS / "ods_station_raw_5min.csv"
    ods.to_csv(out_5min, index=False)
    print(f"[load_ods] ods_station_raw_5min rows={len(ods)} stations={ods['station_id'].nunique()}")
    print(f"[load_ods] wrote {out_5min}")

    if INFO.is_file():
        info = pd.read_csv(INFO)
        # drop helper flag col for ODS mirror if present (keep file; MySQL schema has no flag)
        out_info = ODS / "ods_station_info.csv"
        keep = [
            c
            for c in [
                "station_id",
                "taz_id",
                "lon_gcj",
                "lat_gcj",
                "lon_wgs",
                "lat_wgs",
                "total_piles",
                "rated_power_kw",
            ]
            if c in info.columns
        ]
        info[keep].to_csv(out_info, index=False)
        print(f"[load_ods] ods_station_info rows={len(info)} wrote {out_info}")
    else:
        print("[load_ods] WARN: station_info.csv missing")

    # TODO: when MySQL available —
    #   LOAD DATA / executemany into ods_station_raw_5min, ods_station_info
    print("[load_ods] TODO: SQL insert into MySQL ods_* (file-based OK for D1–D2)")


if __name__ == "__main__":
    main()
