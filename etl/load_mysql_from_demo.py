#!/usr/bin/env python3
"""Load data/demo ods+dwd CSVs into MySQL and backfill ads_* (小黑)."""
from __future__ import annotations

import csv
import os
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
ODS = DEMO / "ods"
DWD = DEMO / "dwd"

CFG = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "sz_ev"),
    database=os.getenv("MYSQL_DB", "sz_ev"),
    charset="utf8mb4",
    local_infile=True,
)


def conn():
    return pymysql.connect(**CFG)


def load_csv(cur, table: str, path: Path, cols: list[str]):
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = []
        for row in r:
            vals = []
            for c in cols:
                v = row.get(c)
                vals.append(None if v in ("", None) else v)
            rows.append(vals)
            if len(rows) >= 2000:
                cur.executemany(
                    f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})",
                    rows,
                )
                rows = []
        if rows:
            cur.executemany(
                f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})",
                rows,
            )


def main():
    c = conn()
    cur = c.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in [
        "ads_ticket",
        "ads_baseline_naive",
        "ads_zone_occupancy_hour",
        "ads_city_overview",
        "ads_quality_summary",
        "dwd_zone_hour",
        "dwd_quality_station",
        "dwd_station_5min_clean",
        "ods_station_raw_5min",
        "ods_station_info",
    ]:
        cur.execute(f"TRUNCATE TABLE {t}")

    load_csv(
        cur,
        "ods_station_info",
        ODS / "ods_station_info.csv",
        [
            "station_id",
            "taz_id",
            "lon_gcj",
            "lat_gcj",
            "lon_wgs",
            "lat_wgs",
            "total_piles",
            "rated_power_kw",
        ],
    )
    # raw optional (large) — skip if missing
    raw = ODS / "ods_station_raw_5min.csv"
    if raw.exists() and raw.stat().st_size < 50_000_000:
        load_csv(
            cur,
            "ods_station_raw_5min",
            raw,
            [
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
            ],
        )

    load_csv(
        cur,
        "dwd_station_5min_clean",
        DWD / "dwd_station_5min_clean.csv",
        [
            "station_id",
            "ts",
            "busy",
            "idle",
            "total_piles",
            "s_price",
            "e_price",
            "duration",
            "volume",
            "is_row_missing",
            "is_stuck",
            "is_outlier",
            "occupancy",
            "quality_flag",
        ],
    )
    load_csv(
        cur,
        "dwd_quality_station",
        DWD / "dwd_quality_station.csv",
        ["station_id", "missing_rate", "stuck_flag", "gap_days_max", "ticket_suggest"],
    )
    # dwd_quality may not have taz in table — CSV has taz_id; ignore extra
    load_csv(
        cur,
        "dwd_zone_hour",
        DWD / "dwd_zone_hour.csv",
        ["taz_id", "ts_hour", "occupancy_avg", "busy_sum", "piles_sum", "station_cnt"],
    )

    # quality summary from report numbers + live counts
    cur.execute(
        """
        REPLACE INTO ads_quality_summary (metric, value, note) VALUES
        ('demo_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station), '演示子集'),
        ('row_missing_rate', 0.2642, '14天窗实测 docs/quality_report.md'),
        ('stuck_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1), '窗内实测'),
        ('high_missing_station_cnt', (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30), 'missing>30%')
        """
    )
    cur.execute(
        """
        INSERT INTO ads_zone_occupancy_hour (taz_id, ts_hour, occupancy, load_kw_est)
        SELECT taz_id, ts_hour, occupancy_avg, ROUND(busy_sum * 7, 2)
        FROM dwd_zone_hour
        ON DUPLICATE KEY UPDATE occupancy=VALUES(occupancy), load_kw_est=VALUES(load_kw_est)
        """
    )
    cur.execute(
        """
        INSERT INTO ads_city_overview (dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt)
        SELECT DATE(ts_hour),
               (SELECT COUNT(*) FROM dwd_quality_station),
               COUNT(DISTINCT taz_id),
               AVG(occupancy_avg),
               (SELECT HOUR(ts_hour) FROM dwd_zone_hour z2
                 WHERE DATE(z2.ts_hour)=DATE(dwd_zone_hour.ts_hour)
                 GROUP BY HOUR(ts_hour) ORDER BY AVG(occupancy_avg) DESC LIMIT 1),
               (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1),
               (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30)
        FROM dwd_zone_hour
        GROUP BY DATE(ts_hour)
        ON DUPLICATE KEY UPDATE
          station_cnt=VALUES(station_cnt), zone_cnt=VALUES(zone_cnt),
          avg_occupancy=VALUES(avg_occupancy), peak_hour=VALUES(peak_hour),
          stuck_cnt=VALUES(stuck_cnt), high_missing_cnt=VALUES(high_missing_cnt)
        """
    )
    cur.execute(
        """
        INSERT INTO ads_ticket (station_id, type, status)
        SELECT station_id,
               CASE WHEN stuck_flag=1 THEN 'repair' ELSE 'inspect' END,
               'open'
        FROM dwd_quality_station
        WHERE stuck_flag=1 OR missing_rate>0.30
        """
    )
    c.commit()
    cur.close()
    c.close()
    print("OK loaded demo CSVs → MySQL ads backfilled")


if __name__ == "__main__":
    main()
