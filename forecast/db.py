"""forecast 用到的 MySQL 连接（与 api/app.py 同一套环境变量）。"""
from __future__ import annotations

import os


def connect():
    import pymysql
    return pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                           user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                           database=os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "sz_ev")), charset="utf8mb4",
                           init_command="SET time_zone='+08:00'", autocommit=False, connect_timeout=5)


def ensure_tables(conn) -> None:
    from pathlib import Path
    sql = (Path(__file__).resolve().parents[1] / "sql" / "ads" / "06_ads_forecast.sql").read_text(encoding="utf-8")
    with conn.cursor() as cur:
        for stmt in [s for s in sql.split(";") if "CREATE" in s]:
            cur.execute(stmt)
    conn.commit()
