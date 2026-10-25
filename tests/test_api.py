"""API 测试（file 模式，读随仓 demo 数据），对应 docs/acceptance_checklist.md 中可脚本化的项。"""
import importlib

import pytest


@pytest.fixture()
def client():
    app_mod = importlib.import_module("app")
    app_mod._TICKETS = None  # 每个用例重置内存工单
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


def test_ticket_patch(client):
    assert data(client.patch("/api/tickets/1", json={"status": "processing"}))["status"] == "processing"
    bad = client.patch("/api/tickets/1", json={"status": "x"})
    assert bad.status_code == 400 and bad.get_json()["ok"] is False
    assert client.patch("/api/tickets/999", json={"status": "done"}).status_code == 404


def test_cors_preflight(client):
    r = client.open("/api/tickets", method="OPTIONS")
    assert r.status_code in (200, 204) and r.headers["Access-Control-Allow-Origin"] == "*"


def test_dashboard_served(client):
    r = client.get("/")
    assert r.status_code == 200 and "深圳市电动汽车充电站运行管理可视化系统" in r.get_data(as_text=True)
