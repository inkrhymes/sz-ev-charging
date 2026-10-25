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
    "MIN(dt) AS d0": [{"d0": dt.date(2022, 9, 1), "d1": dt.date(2023, 2, 28)}],
    "ads_city_overview": [{"dt": dt.date(2022, 9, 1), "station_cnt": 1682, "zone_cnt": 325, "avg_occupancy": Decimal("0.303123"),
                           "peak_hour": 6, "stuck_cnt": 6, "high_missing_cnt": 180}],
    "DISTINCT taz_id": [{"taz_id": "102"}, {"taz_id": "11"}],
    "FROM ads_zone_occupancy_hour WHERE": [{"taz_id": "11", "ts_hour": dt.datetime(2022, 9, 1, 0), "occupancy": Decimal("0.5"),
                                            "load_kw_est": None, "power_coverage": 0.85}],
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
    assert set(p) == {"taz_id", "ts_hour", "occupancy", "load_kw_est", "power_coverage"} and p["power_coverage"] == 0.85 and p["ts_hour"] == "2022-09-01 00:00:00"
    b = data(c.get("/api/baseline"))
    assert b["taz_id"] == "11" and {"metrics", "forecast_window"} <= set(b)
    assert {"taz_id", "ts_hour", "y_true", "y_hat_lag1", "abs_err", "y_hat_lgbm", "abs_err_lgbm"} <= set(b["points"][0])
    assert data(c.get("/api/quality"))[0] == {"metric": "row_missing_rate", "value": 0.1142, "note": "x"}


def test_mysql_unreachable_falls_back_to_file(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "_sql", lambda q, a=(): None)
    d = data(app_mod.app.test_client().get("/api/overview?dt=2022-09-01"))
    assert d["station_cnt"] == 25


def _boom():
    raise ConnectionError("can't connect to mysql:3306 (timed out)")


class _OkConn:
    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a):
        return 1

    def close(self):
        pass


def test_unreachable_fallback_is_visible(app_mod, monkeypatch):
    """非严格：退回文件，但响应头和 /api/health 都说明实际数据源与错误。"""
    monkeypatch.delenv("API_STRICT_MYSQL", raising=False)
    monkeypatch.setattr(app_mod, "_mysql_connect", _boom)
    c = app_mod.app.test_client()
    r = c.get("/api/overview?dt=2022-09-01")
    assert data(r)["station_cnt"] == 25
    assert r.headers["X-Data-Source"] == "file" and r.headers["X-Data-Fallback"] == "true"
    h = data(c.get("/api/health"))
    assert h["mode"] == "mysql" and h["source"] == "file" and h["fallback"] is True
    assert "timed out" in h["mysql_error"] and h["strict_mysql"] is False


def test_health_reports_mysql_when_reachable(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "_mysql_connect", lambda: _OkConn())
    r = app_mod.app.test_client().get("/api/health")
    h = data(r)
    assert h["source"] == "mysql" and h["fallback"] is False and h["mysql_error"] is None
    assert r.headers["X-Data-Source"] == "mysql" and r.headers["X-Data-Fallback"] == "false"


def test_file_mode_health_unchanged(app_mod, monkeypatch):
    monkeypatch.setenv("API_SOURCE", "file")
    r = app_mod.app.test_client().get("/api/health")
    h = data(r)
    assert h["mode"] == h["source"] and h["fallback"] is False and "X-Data-Source" not in r.headers


@pytest.mark.parametrize("path", ["/api/health", "/api/overview?dt=2022-09-01", "/api/zones", "/api/zone/series?taz_id=11",
                                  "/api/baseline", "/api/quality", "/api/alerts", "/api/tickets"])
def test_strict_mysql_returns_503(app_mod, monkeypatch, path):
    monkeypatch.setenv("API_STRICT_MYSQL", "1")
    monkeypatch.setattr(app_mod, "_mysql_connect", _boom)
    monkeypatch.setattr(app_mod.pymysql if hasattr(app_mod, "pymysql") else __import__("pymysql"), "connect",
                        lambda *a, **k: _boom())
    r = app_mod.app.test_client().get(path)
    assert r.status_code == 503, (path, r.get_json())
    assert r.get_json()["ok"] is False


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


def test_data_range_mysql(app_mod, monkeypatch):
    monkeypatch.setattr(app_mod, "_sql", fake_sql)
    assert data(app_mod.app.test_client().get("/api/data_range")) == {"from": "2022-09-01", "to": "2023-02-28"}


def test_data_range_file(app_mod, monkeypatch):
    monkeypatch.setenv("API_SOURCE", "file")
    d = data(app_mod.app.test_client().get("/api/data_range"))
    assert d["from"] <= d["to"] and d["from"].startswith("2022-")
