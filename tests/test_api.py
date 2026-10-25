"""API 测试（file 模式，读随仓 demo 数据），对应 docs/acceptance_checklist.md 中可脚本化的项。"""
import importlib

import pytest


@pytest.fixture()
def client():
    app_mod = importlib.import_module("app")
    app_mod._TICKETS = None
    app_mod._ALERTS = None  # 每个用例重置内存工单
    app_mod.app.config["TESTING"] = True
    return app_mod.app.test_client()


def data(resp):
    j = resp.get_json()
    assert resp.status_code == 200 and j["ok"] is True, j
    return j["data"]


def test_health_file_mode(client):
    assert data(client.get("/api/health"))["mode"] == "file"


def test_overview_kpis(client):
    d = data(client.get("/api/overview?dt=2022-09-01"))
    assert (d["station_cnt"], d["zone_cnt"], d["stuck_cnt"], d["high_missing_cnt"]) == (25, 23, 4, 6)


def test_quality(client):
    d = {m["metric"]: m["value"] for m in data(client.get("/api/quality"))}
    assert len(d) == 4 and d["row_missing_rate"] == 0.2642


def test_zones(client):
    assert len(data(client.get("/api/zones"))) == 18


def test_zone_series_default_168(client):
    pts = data(client.get("/api/zone/series?taz_id=1106"))["points"]
    assert len(pts) == 168 and all(p["taz_id"] == "1106" for p in pts)
    assert len({p["occupancy"] for p in pts}) > 1  # 曲线有起伏


def test_zone_series_range_48(client):
    pts = data(client.get("/api/zone/series?taz_id=1011&from=2022-09-01&to=2022-09-02"))["points"]
    assert len(pts) == 48


def test_baseline(client):
    pts = data(client.get("/api/baseline?taz_id=1011&from=2022-09-01&to=2022-09-02"))["points"]
    assert len(pts) == 48
    for prev, cur in zip(pts, pts[1:]):
        assert cur["y_hat_lag1"] == prev["y_true"]
        assert cur["abs_err"] == pytest.approx(abs(cur["y_true"] - cur["y_hat_lag1"]), abs=1e-5)


def test_stations_pagination(client):
    d = data(client.get("/api/stations?page=1&page_size=10"))
    assert d["total"] == 25 and len(d["items"]) == 10
    d3 = data(client.get("/api/stations?page=3&page_size=10"))
    assert len(d3["items"]) == 5


def test_stations_filter(client):
    assert data(client.get("/api/stations?status=stuck"))["total"] == 4
    assert data(client.get("/api/stations?status=high_missing"))["total"] == 6
    assert client.get("/api/stations?sort=bogus").status_code == 400


def test_tickets_list(client):
    t = data(client.get("/api/tickets"))
    assert len(t) == 10
    assert sum(x["type"] == "repair" for x in t) == 4 and sum(x["type"] == "inspect" for x in t) == 6


def test_login_roles(client):
    r = data(client.post("/api/auth/login", json={"user": "demo", "role": "readonly"}))
    assert r["token"] == "demo-readonly"
    assert data(client.post("/api/auth/login", json={"role": "admin"}))["token"] == "demo-admin"
    assert client.post("/api/auth/login", json={"role": "root"}).status_code == 400


ADMIN = {"Authorization": "Bearer demo-admin"}
RO = {"Authorization": "Bearer demo-readonly"}


def test_ticket_patch_admin(client):
    assert data(client.patch("/api/tickets/1", json={"status": "processing"}, headers=ADMIN))["status"] == "processing"
    bad = client.patch("/api/tickets/1", json={"status": "x"}, headers=ADMIN)
    assert bad.status_code == 400 and bad.get_json()["ok"] is False
    assert client.patch("/api/tickets/999", json={"status": "done"}, headers=ADMIN).status_code == 404


@pytest.mark.parametrize("path,status", [("/api/tickets/1", "done"), ("/api/alerts/1", "closed")])
def test_patch_role_from_token(client, path, status):
    r = client.patch(path, json={"status": status}, headers=RO)
    assert r.status_code == 403 and "只读" in r.get_json()["error"]
    # 请求体里自称 admin 也不行
    assert client.patch(path, json={"status": status, "role": "admin"}, headers=RO).status_code == 403
    assert client.patch(path, json={"status": status, "role": "admin"}).status_code == 401
    assert client.patch(path, json={"status": status}, headers={"Authorization": "Bearer forged"}).status_code == 401
    assert data(client.patch(path, json={"status": status}, headers=ADMIN))["status"] == status


def test_cors_preflight(client):
    r = client.open("/api/tickets", method="OPTIONS")
    assert r.status_code in (200, 204) and r.headers["Access-Control-Allow-Origin"] == "*"


def test_dashboard_served(client):
    r = client.get("/")
    assert r.status_code == 200 and "深圳市电动汽车充电站运行管理可视化系统" in r.get_data(as_text=True)


def test_alerts_states_and_permission(client):
    r = client.get("/api/alerts")
    alerts = r.get_json()["data"]
    assert alerts and {a["status"] for a in alerts} <= {"open", "ack", "closed"}
    aid = alerts[0]["alert_id"]
    assert client.patch(f"/api/alerts/{aid}", json={"status": "ack"}, headers=RO).status_code == 403
    assert client.patch(f"/api/alerts/{aid}", json={"status": "processing2"}, headers=ADMIN).status_code == 400
    r = client.patch(f"/api/alerts/{aid}", json={"status": "ack"}, headers=ADMIN)
    assert r.get_json()["data"]["status"] == "ack"
    r = client.patch(f"/api/alerts/{aid}", json={"status": "closed"}, headers=ADMIN)
    assert r.get_json()["data"]["status"] == "closed"


def test_map_zones(client):
    d = client.get("/api/map/zones").get_json()["data"]
    assert d["zones"] and all({"taz_id", "lon", "lat", "station_count"} <= set(z) for z in d["zones"])
    assert all("lon" in s and "taz_id" in s for s in d["stations"])


def test_alert_rule_level_and_ddl_fields(client):
    alerts = client.get("/api/alerts").get_json()["data"]
    assert all({"ds", "layer", "metric_value", "threshold", "updated_at"} <= set(a) for a in alerts)
    assert any(a["station_id"] is None for a in alerts)


def test_norm_alert_from_ddl_row():
    import app as appmod
    a = appmod._norm_alert({"alert_id": 7, "ds": "2022-09-02", "layer": "ods", "rule_name": "row_count", "severity": "block",
                            "station_id": None, "taz_id": None, "metric_value": 0.0, "threshold": 1.0, "status": "ack"})
    assert a["level"] == "high" and a["status"] == "ack" and a["kind"] == "row_count" and a["station_id"] is None


def test_lake_map_optional(monkeypatch, tmp_path):
    import app as appmod
    monkeypatch.setattr(appmod, "LAKE", str(tmp_path / "nope"))
    appmod._lake_map.cache_clear()
    assert appmod._lake_map() is None
    appmod._lake_map.cache_clear()


def test_map_zones_get_not_405(client):
    # 小黑遇到的 405：未知 /api 路径被只登记了 OPTIONS 的兜底路由接住
    assert client.get("/api/map/zones").status_code == 200
    assert client.options("/api/map/zones").status_code in (200, 204)
    r = client.get("/api/no-such-thing")
    assert r.status_code == 404 and r.get_json()["ok"] is False


def test_lake_map_real_ads(monkeypatch):
    import os, app as appmod
    from pathlib import Path
    root = os.getenv("SZ_EV_LAKE") or "/workspace/sz-ev-charging/data/lake"
    if not (Path(root) / "ads" / "zone_hour").exists():
        import pytest; pytest.skip("没有真实 lake 数据")
    monkeypatch.setattr(appmod, "LAKE", root)
    appmod._lake_map.cache_clear()
    zones, stations = appmod._lake_map()
    appmod._lake_map.cache_clear()
    assert len(stations) == 1682 and len(zones) == 331
    assert sum(z["avg_occupancy"] is not None for z in zones) > 300
