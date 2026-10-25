#!/usr/bin/env python3
"""把 data/demo/ods/ods_station_raw_5min.csv.gz 切成按天分区 Parquet，供 Spark 自测。

输出：data/lake/ods/charge_5min/dt=YYYY-MM-DD/part-0.parquet（与全量约定同结构）
用法：python spark/tools/make_demo_lake.py [--days N]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "data" / "demo" / "ods" / "ods_station_raw_5min.csv.gz"
OUT = ROOT / "data" / "lake" / "ods" / "charge_5min"
COLS = ["station_id", "ts", "busy", "idle", "s_price", "e_price", "duration", "volume"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="只取前 N 天，0=全部")
    a = ap.parse_args()
    df = pd.read_csv(SRC, usecols=COLS, dtype={"station_id": str})
    df["ts"] = pd.to_datetime(df["ts"])
    df["dt"] = df["ts"].dt.strftime("%Y-%m-%d")
    days = sorted(df["dt"].unique())
    if a.days:
        days = days[: a.days]
    for d in days:
        part = df[df["dt"] == d].drop(columns="dt")
        p = OUT / f"dt={d}"
        p.mkdir(parents=True, exist_ok=True)
        part.to_parquet(p / "part-0.parquet", index=False)
        print(f"{p.relative_to(ROOT)} rows={len(part)}")


if __name__ == "__main__":
    main()
