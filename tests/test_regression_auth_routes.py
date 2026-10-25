"""回归测试：工单/告警改状态的权限在 MySQL 模式下仍然生效；地图、告警路由在导入 app 时就已注册（防 405 复发）。
不需要真实数据库：用假连接替代 _alert_db。"""
import importlib

import pytest

ADMIN = {"Authorization": "Bearer demo-admin"}
RO = {"Authorization": "Bearer demo-readonly"}


class FakeCur:
    def __init__(self, log):
        self.log = log
        self.lastrowid = 1

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, q, args=()):
        self.log.append(q)
        return 1

    def fetchone(self):
        return {"ticket_id": 7, "station_id": "1554", "type": "repair", "status": "processing",
                "alert_id": None, "created_at": None, "updated_at": None}

    def fetchall(self):
        return []


class FakeCon:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self, *a, **k):
        return FakeCur(self.log)

    def commit(self):
        pass


@pytest.fixture()
def db(monkeypatch):
    m = importlib.import_module("app")
    m.app.config["TESTING"] = True
    log = []
    monkeypatch.setenv("ALERT_DB", "mysql")
    monkeypatch.setattr(m, "_alert_db", lambda: FakeCon(log))
    return m.app.test_client(), log


@pytest.mark.parametrize("path,status", [("/api/tickets/7", "processing"), ("/api/alerts/7", "ack")])
@pytest.mark.parametrize("headers,code", [(RO, 403), ({}, 401), ({"Authorization": "Bearer forged"}, 401)])
def test_mysql_mode_patch_denied_without_admin(db, path, status, headers, code):
    client, log = db
    r = client.patch(path, json={"status": status, "role": "admin"}, headers=headers)
    assert r.status_code == code
    assert r.get_json()["ok"] is False
    assert not any("UPDATE" in q.upper() for q in log), "没有权限也写了库"


def test_mysql_mode_ticket_patch_admin_writes_db(db):
    client, log = db
    r = client.patch("/api/tickets/7", json={"status": "processing"}, headers=ADMIN)
    assert r.status_code == 200, r.get_json()
    assert r.get_json()["data"]["status"] == "processing"
    assert any("UPDATE ads_ticket" in q for q in log)


def test_routes_registered_on_import():
    m = importlib.import_module("app")
    rules = {(r.rule, meth) for r in m.app.url_map.iter_rules() for meth in r.methods}
    for need in [("/api/map/zones", "GET"), ("/api/alerts", "GET"), ("/api/alerts/<int:alert_id>", "PATCH"),
                 ("/api/tickets", "GET"), ("/api/tickets", "POST"), ("/api/tickets/<int:ticket_id>", "PATCH"),
                 ("/api/baseline", "GET")]:
        assert need in rules, need


def test_map_zones_not_405():
    m = importlib.import_module("app")
    assert m.app.test_client().get("/api/map/zones").status_code == 200


def test_ticket_processing_sets_started_at_once(db):
    client, log = db
    client.patch("/api/tickets/7", json={"status": "processing"}, headers=ADMIN)
    q = next(q for q in log if "UPDATE ads_ticket" in q)
    assert "started_at=COALESCE(started_at, NOW())" in q
    log.clear()
    client.patch("/api/tickets/7", json={"status": "done"}, headers=ADMIN)
    assert not any("started_at" in q for q in log if "UPDATE" in q)
