#!/usr/bin/env python3
"""Clean ODS demo CSVs → DWD files + docs/quality_report.md with measured numbers.

Rules (aligned with data_dictionary §2 / §4):
  - is_row_missing: busy & idle both NaN (整行缺失)
  - is_stuck: station whose non-missing busy has nunique==1 over the window (占用长期不变)
  - is_outlier: busy outside station IQR fences (on non-missing rows)
  - occupancy = busy / total_piles
  - ticket_suggest: repair (stuck) / inspect (missing_rate>0.30) / none

No MySQL required. TODO: INSERT into dwd_* / ads_quality_summary later.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
ODS = DEMO / "ods"
DWD = DEMO / "dwd"
DOCS = ROOT / "docs"
REPORT = DOCS / "quality_report.md"
INFO = DEMO / "station_info.csv"
LIST_CSV = DEMO / "demo_station_list.csv"

HIGH_MISSING_THR = 0.30


def iqr_outlier_mask(s: pd.Series) -> pd.Series:
    x = s.dropna()
    if len(x) < 8:
        return pd.Series(False, index=s.index)
    q1, q3 = x.quantile(0.25), x.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return pd.Series(False, index=s.index)
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    return s.notna() & ((s < lo) | (s > hi))


def main() -> None:
    raw_path = ODS / "ods_station_raw_5min.csv"
    if not raw_path.is_file():
        raise SystemExit(f"missing {raw_path}; run load_ods.py first")

    DWD.mkdir(parents=True, exist_ok=True)
    ods = pd.read_csv(raw_path)
    ods["ts"] = pd.to_datetime(ods["ts"])
    info = pd.read_csv(INFO)
    info["station_id"] = info["station_id"].astype(str)
    piles = info.set_index("station_id")["total_piles"].to_dict()
    taz = info.set_index("station_id")["taz_id"].astype(str).to_dict()

    ods["station_id"] = ods["station_id"].astype(str)
    n_raw = len(ods)

    # --- row missing: both busy and idle null ---
    is_row_missing = ods["busy"].isna() & ods["idle"].isna()
    # also treat all-key-null as missing
    is_row_missing = is_row_missing | (
        ods["busy"].isna() & ods["idle"].isna() & ods["duration"].isna() & ods["volume"].isna()
    )

    # --- stuck detection per station ---
    stuck_stations: set[str] = set()
    for sid, g in ods.groupby("station_id"):
        valid = g.loc[~is_row_missing.loc[g.index], "busy"]
        if len(valid) >= 12 and valid.nunique(dropna=True) == 1:
            stuck_stations.add(sid)

    # --- outliers on busy (per station, non-missing) ---
    is_outlier = pd.Series(False, index=ods.index)
    for sid, g in ods.groupby("station_id"):
        mask = iqr_outlier_mask(g["busy"])
        is_outlier.loc[g.index] = mask.fillna(False).to_numpy()

    # --- build dwd_station_5min_clean ---
    dwd = pd.DataFrame(
        {
            "station_id": ods["station_id"],
            "ts": ods["ts"],
            "busy": ods["busy"],
            "idle": ods["idle"],
            "total_piles": ods["station_id"].map(piles),
            "s_price": ods["s_price"],
            "e_price": ods["e_price"],
            "duration": ods["duration"],
            "volume": ods["volume"],
            "is_row_missing": is_row_missing.astype(int),
            "is_stuck": ods["station_id"].isin(stuck_stations).astype(int),
            "is_outlier": is_outlier.astype(int),
        }
    )
    # occupancy only when not missing and piles>0
    dwd["occupancy"] = np.where(
        (dwd["is_row_missing"] == 0) & dwd["total_piles"].notna() & (dwd["total_piles"] > 0) & dwd["busy"].notna(),
        dwd["busy"] / dwd["total_piles"],
        np.nan,
    )
    dwd["quality_flag"] = np.where(
        dwd["is_row_missing"] == 1,
        "dropped_src",
        np.where(dwd["is_outlier"] == 1, "ok", "ok"),
    )

    out_clean = DWD / "dwd_station_5min_clean.csv"
    dwd.to_csv(out_clean, index=False)

    # --- station quality summary ---
    q_rows = []
    for sid, g in dwd.groupby("station_id"):
        miss_rate = float(g["is_row_missing"].mean())
        stuck_flag = 1 if sid in stuck_stations else 0
        # max consecutive missing gap in days
        miss = g.sort_values("ts")["is_row_missing"].to_numpy()
        max_gap_slots = 0
        cur = 0
        for v in miss:
            if v == 1:
                cur += 1
                max_gap_slots = max(max_gap_slots, cur)
            else:
                cur = 0
        gap_days_max = int(round(max_gap_slots * 5 / (60 * 24)))
        if stuck_flag:
            ticket = "repair"
        elif miss_rate > HIGH_MISSING_THR:
            ticket = "inspect"
        else:
            ticket = "none"
        q_rows.append(
            {
                "station_id": sid,
                "taz_id": taz.get(sid),
                "missing_rate": round(miss_rate, 6),
                "stuck_flag": stuck_flag,
                "gap_days_max": gap_days_max,
                "ticket_suggest": ticket,
                "rows": len(g),
                "missing_rows": int(g["is_row_missing"].sum()),
                "outlier_rows": int(g["is_outlier"].sum()),
            }
        )
    quality = pd.DataFrame(q_rows).sort_values("station_id")
    out_q = DWD / "dwd_quality_station.csv"
    quality.to_csv(out_q, index=False)

    # --- zone hour aggregate (simple) ---
    ok = dwd[dwd["is_row_missing"] == 0].copy()
    ok["taz_id"] = ok["station_id"].map(taz)
    ok["ts_hour"] = ok["ts"].dt.floor("h")
    zone = (
        ok.groupby(["taz_id", "ts_hour"], as_index=False)
        .agg(
            occupancy_avg=("occupancy", "mean"),
            busy_sum=("busy", "sum"),
            piles_sum=("total_piles", "sum"),
            station_cnt=("station_id", "nunique"),
        )
    )
    out_zone = DWD / "dwd_zone_hour.csv"
    zone.to_csv(out_zone, index=False)

    # --- metrics ---
    overall_missing = float(dwd["is_row_missing"].mean())
    stuck_cnt = int(quality["stuck_flag"].sum())
    high_missing_cnt = int((quality["missing_rate"] > HIGH_MISSING_THR).sum())
    station_cnt = int(quality["station_id"].nunique())
    zone_cnt = int(quality["taz_id"].nunique())
    ts_min, ts_max = dwd["ts"].min(), dwd["ts"].max()
    outlier_rows = int(dwd["is_outlier"].sum())
    repair_cnt = int((quality["ticket_suggest"] == "repair").sum())
    inspect_cnt = int((quality["ticket_suggest"] == "inspect").sum())

    # write quality report
    lines = []
    lines.append("# 演示子集质检报告（实测）")
    lines.append("")
    lines.append(f"- 生成时间（箱内 Asia/Shanghai）：{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- 数据窗：`{ts_min}` ～ `{ts_max}`")
    lines.append(f"- 站点数：**{station_cnt}**（锁定列表，未另造站号）")
    lines.append(f"- 片区数（TAZ）：**{zone_cnt}**")
    lines.append(f"- 原始/清洗行数：**{n_raw}**")
    lines.append(f"- 脚本：`etl/generate_demo_subset.py` → `load_ods.py` → `clean_to_dwd.py`")
    lines.append("")
    lines.append("## 1. 汇总指标（对齐字典 §4）")
    lines.append("")
    lines.append("| 指标 | 实测值 | 演示子集目标 |")
    lines.append("|------|--------|--------------|")
    lines.append(f"| 整行缺失率 | **{overall_missing*100:.2f}%** ({int(dwd['is_row_missing'].sum())}/{n_raw}) | 同规则统计（全量参考 ~21.2%） |")
    lines.append(f"| 疑似卡死站 | **{stuck_cnt}** | ≥2 |")
    lines.append(f"| 缺失>30% 站 | **{high_missing_cnt}** | ≥3 |")
    lines.append(f"| busy IQR 异常行 | {outlier_rows} | — |")
    lines.append(f"| 工单建议 repair / inspect | {repair_cnt} / {inspect_cnt} | 卡死→repair，高缺失→inspect |")
    lines.append("")
    lines.append("## 2. 站级质量表")
    lines.append("")
    lines.append("| station_id | taz_id | missing_rate | stuck_flag | gap_days_max | ticket_suggest |")
    lines.append("|------------|--------|--------------|------------|--------------|----------------|")
    for _, r in quality.sort_values(["stuck_flag", "missing_rate"], ascending=[False, False]).iterrows():
        lines.append(
            f"| {r['station_id']} | {r['taz_id']} | {r['missing_rate']:.4%} | {int(r['stuck_flag'])} "
            f"| {int(r['gap_days_max'])} | {r['ticket_suggest']} |"
        )
    lines.append("")
    lines.append("## 3. 产物路径")
    lines.append("")
    lines.append(f"- `{out_clean.relative_to(ROOT)}`")
    lines.append(f"- `{out_q.relative_to(ROOT)}`")
    lines.append(f"- `{out_zone.relative_to(ROOT)}`")
    lines.append(f"- `{REPORT.relative_to(ROOT)}`")
    lines.append("")
    lines.append("## 4. 说明")
    lines.append("")
    lines.append("- 卡死判定：窗口内非缺失 `busy` 唯一取值（nunique==1 且有效点≥12）。")
    lines.append("- 整行缺失：`busy` 与 `idle` 同时为空。")
    lines.append("- 异常：按站对 `busy` 做 1.5×IQR；不删除行，仅打 `is_outlier`。")
    lines.append("- MySQL：`dwd_*` / `ads_quality_summary` 入库 TODO（本交付为文件）。")
    lines.append("")

    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"[clean] overall_missing={overall_missing:.4%}")
    print(f"[clean] stuck_cnt={stuck_cnt} high_missing_cnt={high_missing_cnt}")
    print(f"[clean] wrote {out_clean}")
    print(f"[clean] wrote {out_q}")
    print(f"[clean] wrote {out_zone}")
    print(f"[clean] wrote {REPORT}")
    # TODO: INSERT INTO dwd_station_5min_clean, dwd_quality_station, dwd_zone_hour, ads_quality_summary


if __name__ == "__main__":
    main()
