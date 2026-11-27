"""验证方案 C MySQL 演示子集装载函数、轻量实时回放脚本与 api/app.py 直接启动安全性。"""
from __future__ import annotations

import re
import runpy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BANNED_UI_WORDS = re.compile(r"口径|基线|窗口|血缘|stub|（无）|undefined|NaN|\[object Object\]")


def test_build_site_suggest_rows_from_real_demo():
    from etl.load_mysql_from_demo import build_site_suggest_rows

    rows = build_site_suggest_rows()
    assert len(rows) == 18
    ranks = [r[14] for r in rows]
    assert ranks == list(range(1, 19))

    for r in rows:
        (
            run_dt, taz_id, lon, lat, st_cnt, piles, _rated_kw,
            avg_occ, peak_occ, busy_ratio, demand, supply, gap,
            add_piles, rank, fn_tag, reason,
        ) = r
        assert run_dt == "2022-09-14"
        assert taz_id.isdigit()
        assert 113.7 <= lon <= 114.6 and 22.4 <= lat <= 22.9
        assert st_cnt >= 1 and piles >= 1
        assert 0.0 <= avg_occ <= 1.0 and 0.0 <= peak_occ <= 1.0 and 0.0 <= busy_ratio <= 1.0
        assert 0.0 <= demand <= 100.0 and 0.0 <= supply <= 100.0 and 0.0 <= gap <= 100.0
        assert add_piles >= 0 and rank >= 1
        assert fn_tag and reason
        assert not BANNED_UI_WORDS.search(reason), reason
        assert not BANNED_UI_WORDS.search(fn_tag), fn_tag


def test_realtime_windows_and_snapshot_from_real_demo():
    from etl.realtime_replay_mysql import build_realtime_snapshot_rows, iter_demo_windows

    windows = list(iter_demo_windows(date_filter="2022-09-01"))
    assert len(windows) > 0
    ws, we, recs = windows[0]
    assert (we - ws).total_seconds() == 900
    assert len(recs) > 0
    for rec in recs:
        assert 0.0 <= rec["occupancy"] <= 1.0
        assert rec["piles_sum"] > 0 and rec["station_cnt"] >= 1

    snap = build_realtime_snapshot_rows(windows=2, date_filter="2022-09-01")
    assert len(snap) >= len(recs)
    # 14 列对齐 rt_zone_occupancy UPSERT_SQL
    assert all(len(row) == 14 for row in snap)


def test_realtime_replay_cli_dry_run(capsys):
    from etl.realtime_replay_mysql import main

    rc = main(["--date", "2022-09-01", "--interval", "0", "--limit", "2", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "batch=1" in out and "batch=2" in out


def test_build_forecast_rows_from_repo_predictions():
    from etl.load_mysql_from_demo import build_forecast_rows

    zone_hour_rows, daily_rows = build_forecast_rows()
    assert len(zone_hour_rows) > 1000
    assert [r[0] for r in daily_rows] == ["2023-02-27", "2023-02-28"]
    # 第一天没有 24 小时前真值，第二天应能算出 mae_lag24
    assert daily_rows[0][6] is None
    assert daily_rows[1][6] is not None and daily_rows[1][6] >= 0
    for ds, n, mae_l, rmse_l, mae_1, rmse_1, _m24, ver, drift in daily_rows:
        assert n > 0 and mae_l >= 0 and rmse_l >= mae_l and mae_1 >= 0 and rmse_1 >= mae_1
        assert ver and drift in (0, 1)


def test_build_demo_ops_runs_and_dq_alerts():
    from etl.load_mysql_from_demo import build_demo_dq_alerts, build_demo_ops_run_rows

    ops_rows, dq_log_rows = build_demo_ops_run_rows()
    dates = sorted({r[1] for r in ops_rows})
    assert len(dates) == 14 and dates[0] == "2022-09-01" and dates[-1] == "2022-09-14"
    assert len(ops_rows) == 14 * 11
    assert len(dq_log_rows) == 14 * 4

    alerts = build_demo_dq_alerts(latest_ds=dates[-1])
    assert len(alerts) >= 10
    severities = {a[3] for a in alerts}
    assert "block" in severities and "warn" in severities

    from etl.load_mysql_from_demo import build_demo_audit_logs
    audits = build_demo_audit_logs(latest_ds=dates[-1])
    assert len(audits) >= 5
    assert {"login", "ticket_create", "ticket_update", "auth_denied", "export"} <= {a[2] for a in audits}


def test_load_mysql_from_demo_main_with_fake_db(monkeypatch):
    import etl.load_mysql_from_demo as mod

    executed: list[str] = []
    inserted_tables: set[str] = set()

    class FakeCursor:
        def execute(self, sql, args=None):
            executed.append(sql)
            self._last_sql = sql

        def executemany(self, sql, rows):
            executed.append(sql)
            m = re.search(r"INTO\s+([a-zA-Z0-9_]+)", sql, re.I)
            if m and rows:
                inserted_tables.add(m.group(1))

        def fetchone(self):
            if "SHOW TABLES" in getattr(self, "_last_sql", ""):
                return ("ok",)
            return None

        def close(self):
            pass

    class FakeConn:
        def __init__(self):
            self.committed = False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            self.committed = True

        def close(self):
            pass

    fake = FakeConn()
    monkeypatch.setattr(mod, "conn", lambda: fake)
    mod.main()
    assert fake.committed is True
    for expected in (
        "ods_station_info",
        "dwd_station_5min_clean",
        "dwd_quality_station",
        "dwd_zone_hour",
        "ads_zone_site_suggest",
        "rt_zone_occupancy",
        "ads_forecast_zone_hour",
        "ads_forecast_metric_daily",
        "ops_task_run",
        "sys_op_log",
        "dq_run_log",
        "dq_alert",
    ):
        assert expected in inserted_tables, f"missing insert into {expected}"


def test_api_app_direct_run_no_circular_import(monkeypatch):
    """直接以 __main__ 运行 api/app.py 时不应再因 routes/read.py 反向 import app 报循环导入。"""
    import flask

    ran = {}
    monkeypatch.setattr(flask.Flask, "run", lambda self, **kw: ran.update(kw))

    api_dir = str(ROOT / "api")
    old_path = list(sys.path)
    saved = {k: v for k, v in sys.modules.items() if k in ("app", "ops", "lineage") or k.startswith(("routes", "utils"))}
    try:
        for k in list(saved):
            sys.modules.pop(k, None)
        sys.path.insert(0, api_dir)
        runpy.run_path(str(ROOT / "api" / "app.py"), run_name="__main__")
        assert ran.get("port") == 5000
    finally:
        for k in list(sys.modules):
            if k in ("app", "ops", "lineage") or k.startswith(("routes", "utils")):
                sys.modules.pop(k, None)
        sys.modules.update(saved)
        sys.path[:] = old_path
