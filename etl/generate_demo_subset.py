#!/usr/bin/env python3
"""Copy locked UrbanEV demo stations into data/demo/raw/ (optional 14-day slice).

Does NOT invent station IDs — reads data/demo/demo_station_ids.txt + demo_station_list.csv.
Source preference:
  1) URBANEV_RAW env or default gdrive charge_5min
  2) thesis_ev_data/demo_subset/charge_5min fallback
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
IDS_FILE = DEMO / "demo_station_ids.txt"
LIST_CSV = DEMO / "demo_station_list.csv"
RAW_OUT = DEMO / "raw"
STATION_INFO_OUT = DEMO / "station_info.csv"

DEFAULT_URBANEV = Path(
    "/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/"
    "20220901-20230228_station-raw/charge_5min"
)
FALLBACK = Path("/workspace/thesis_ev_data/demo_subset/charge_5min")
INFO_CANDIDATES = [
    Path("/workspace/thesis_ev_data/demo_subset/station_information_demo.csv"),
    Path(
        "/workspace/thesis_ev_data/gdrive/UrbanEV/UrbanEVDataset/"
        "20220901-20230228_station-raw/station_information.csv"
    ),
]

# Default demo window (Asia/Shanghai wall clock as stored in UrbanEV)
DEFAULT_START = "2022-09-01"
DEFAULT_END = "2022-09-15"  # exclusive → 14 days


def load_ids() -> list[str]:
    ids = [ln.strip() for ln in IDS_FILE.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if len(ids) < 20:
        raise SystemExit(f"expected ≥20 locked ids in {IDS_FILE}, got {len(ids)}")
    return ids


def resolve_src(src_dir: Path | None) -> Path:
    if src_dir is not None:
        return src_dir
    if DEFAULT_URBANEV.is_dir():
        return DEFAULT_URBANEV
    if FALLBACK.is_dir():
        return FALLBACK
    raise SystemExit("UrbanEV charge_5min not found; set --src")


def build_station_info(ids: list[str]) -> pd.DataFrame:
    roles = pd.read_csv(LIST_CSV)
    roles["station_id"] = roles["sid" if "sid" in roles.columns else "station_id"].astype(str)
    role_map = {}
    for _, r in roles.iterrows():
        role = str(r.get("role", "正常站"))
        if "卡死" in role:
            flag = "stuck"
        elif "高缺失" in role:
            flag = "high_missing"
        else:
            flag = "normal"
        role_map[str(r["station_id"])] = flag

    info_path = next((p for p in INFO_CANDIDATES if p.is_file()), None)
    if info_path is None:
        raise SystemExit("station_information*.csv not found")

    info = pd.read_csv(info_path)
    # normalize columns
    colmap = {}
    for c in info.columns:
        cl = c.lower()
        if cl in ("station_id", "sid"):
            colmap[c] = "station_id"
        elif cl == "tazid":
            colmap[c] = "taz_id"
        elif cl in ("longitude", "lon", "lon_gcj"):
            colmap[c] = "lon_gcj"
        elif cl in ("latitude", "lat", "lat_gcj"):
            colmap[c] = "lat_gcj"
        elif cl in ("charge_count", "total_piles"):
            colmap[c] = "total_piles"
    info = info.rename(columns=colmap)
    info["station_id"] = info["station_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    info = info[info["station_id"].isin(ids)].copy()

    rows = []
    for sid in ids:
        sub = info[info["station_id"] == sid]
        if sub.empty:
            raise SystemExit(f"station_id {sid} missing from station_information")
        r = sub.iloc[0]
        total = int(r["total_piles"]) if pd.notna(r.get("total_piles")) else None
        # rated_power_kw: rough default 7kW * piles if unknown
        rated = float(total * 7.0) if total else None
        rows.append(
            {
                "station_id": sid,
                "taz_id": str(int(r["taz_id"])) if pd.notna(r.get("taz_id")) else None,
                "lon_gcj": float(r["lon_gcj"]) if pd.notna(r.get("lon_gcj")) else None,
                "lat_gcj": float(r["lat_gcj"]) if pd.notna(r.get("lat_gcj")) else None,
                "lon_wgs": None,
                "lat_wgs": None,
                "total_piles": total,
                "rated_power_kw": rated,
                "flag": role_map.get(sid, "normal"),
            }
        )
    return pd.DataFrame(rows)


def copy_or_slice(
    src: Path,
    sid: str,
    start: str | None,
    end: str | None,
) -> int:
    src_file = src / f"{sid}.csv"
    if not src_file.is_file():
        raise SystemExit(f"missing source CSV: {src_file}")
    dst = RAW_OUT / f"{sid}.csv"
    if start is None and end is None:
        shutil.copy2(src_file, dst)
        # count lines cheaply
        with open(dst, encoding="utf-8") as f:
            return max(sum(1 for _ in f) - 1, 0)

    df = pd.read_csv(src_file)
    df["time"] = pd.to_datetime(df["time"])
    if start:
        df = df[df["time"] >= pd.Timestamp(start)]
    if end:
        df = df[df["time"] < pd.Timestamp(end)]
    df.to_csv(dst, index=False)
    return len(df)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--src", type=Path, default=None, help="UrbanEV charge_5min dir")
    ap.add_argument("--start", default=DEFAULT_START, help="slice start YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=DEFAULT_END, help="slice end YYYY-MM-DD (exclusive)")
    ap.add_argument("--full", action="store_true", help="copy full period (ignore start/end)")
    args = ap.parse_args()

    ids = load_ids()
    src = resolve_src(args.src)
    RAW_OUT.mkdir(parents=True, exist_ok=True)

    start = end = None
    if not args.full:
        start, end = args.start, args.end

    print(f"[generate] src={src}")
    print(f"[generate] stations={len(ids)} window={'FULL' if args.full else f'{start}..{end}'}")

    total_rows = 0
    for sid in ids:
        n = copy_or_slice(src, sid, start, end)
        total_rows += n
        print(f"  {sid}.csv -> {n} rows")

    info = build_station_info(ids)
    info.to_csv(STATION_INFO_OUT, index=False)
    print(f"[generate] wrote {STATION_INFO_OUT} ({len(info)} stations)")
    print(f"[generate] raw rows total={total_rows} -> {RAW_OUT}")


if __name__ == "__main__":
    main()
