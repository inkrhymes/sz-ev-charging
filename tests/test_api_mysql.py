"""API MySQL 数据源模式（API_SOURCE=mysql）：用假 _sql 验证字段与文件模式一致；连不上时退回文件模式。
设置 SZ_TEST_MYSQL=1 时额外对真实 sz_ev 做冒烟测试。"""
import ast
import datetime as dt
import importlib
import os
from decimal import Decimal
from pathlib import Path

import pytest

FAKE = {
    "ads_city_overview": [{"dt": dt.date(2022, 9, 1), "station_cnt": 1682, "zone_cnt": 325, "avg_occupancy": Decimal("0.303123"),
                           "peak_hour": 6, "stuck_cnt": 6, "high_missing_cnt": 180}],
    "DISTINCT taz_id": [{"taz_id": "102"}, {"taz_id": "11"}],
    "FROM ads_zone_occupancy_hour WHERE": [{"taz_id": "11", "ts_hour": dt.datetime(2022, 9, 1, 0), "occupancy": Decimal("0.5"),
                                            "load_kw_est": None}],
    "FROM ads_baseline_naive WHERE": [{"taz_id": "11", "ts_hour": dt.datetime(2022, 9, 1, 1), "y_true": Decimal("0.5"),
                                       "y_hat_lag1": Decimal("0.4"), "abs_err": Decimal("0.1")}],
    "LIMIT 1": [{"taz_id": "11"}],
    "ads_quality_summary": [{"metric": "row_missing_rate", "value": Decimal("0.114230"), "note": "x"}],
    "dim_station": [{"station_id": "1", "taz_id": "11", "longitude": 114.0, "latitude": 22.5, "total_piles": 10}],
    "ads_zone_daily": [{"taz_id": "11", "lon": 114.0, "lat": 22.5, "station_count": 1, "total_piles": 10,
                        "avg_occupancy": Decimal("0.3")}],
}


def fake_sql(query, args=()):
    for k, v in FAKE.items():
        if k in query:
            return v
    raise AssertionError(query)


@pytest.fixture()
def app_mod(monkeypatch):
    m = importlib.import_module("app")
    monkeypatch.setenv("API_SOURCE", "mysql")
    m.app.config["TESTING"] = True
    return m


def data(resp):
    j = resp.get_json()
    assert resp.status_code == 200 and j["ok"] is True, j
    return j["data"]


def test_mysql_mode_fields(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "_sql", fake_sql)
    c = app_mod.app.test_client()
    assert data(c.get("/api/health"))["mode"] == "mysql"
    o = data(c.get("/api/overview?dt=2022-09-01"))
    assert set(o) == {"dt", "station_cnt", "zone_cnt", "avg_occupancy", "peak_hour", "stuck_cnt", "high_missing_cnt"}
    assert o["station_cnt"] == 1682 and o["avg_occupancy"] == 0.3031
    assert data(c.get("/api/zones")) == ["11", "102"]
    p = data(c.get("/api/zone/series?taz_id=11"))["points"][0]
    assert set(p) == {"taz_id", "ts_hour", "occupancy", "load_kw_est"} and p["ts_hour"] == "2022-09-01 00:00:00"
    b = data(c.get("/api/baseline"))
    assert b["taz_id"] == "11" and set(b["points"][0]) == {"taz_id", "ts_hour", "y_true", "y_hat_lag1", "abs_err"}
    assert data(c.get("/api/quality"))[0] == {"metric": "row_missing_rate", "value": 0.1142, "note": "x"}


def test_mysql_unreachable_falls_back_to_file(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "_sql", lambda q, a=(): None)
    d = data(app_mod.app.test_client().get("/api/overview?dt=2022-09-01"))
    assert d["station_cnt"] == 25


def test_main_block_is_last():
    """python app.py 启动时 app.run() 阻塞，其后的路由不会注册（地图、告警路由在 main 之后会不生效）。"""
    tree = ast.parse((Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8"))
    last = tree.body[-1]
    assert isinstance(last, ast.If) and "__main__" in ast.unparse(last.test)


@pytest.mark.skipif(os.getenv("SZ_TEST_MYSQL") != "1", reason="需要真实 MySQL：SZ_TEST_MYSQL=1")
def test_real_mysql_smoke(app_mod):
    c = app_mod.app.test_client()
    assert data(c.get("/api/overview?dt=2022-09-01"))["station_cnt"] == 1682
    assert len(data(c.get("/api/zone/series"))["points"]) > 0
