"""模块 6 系统管理与操作审计日志：登录/退出/告警/工单/越权拦截/导出全链路测试。"""
from __future__ import annotations

from urllib.parse import quote

import pytest


@pytest.fixture
def audit_client(monkeypatch):
    import app as api_app

    monkeypatch.setenv("API_SOURCE", "file")
    monkeypatch.setenv("AUTH_DEMO_ENABLED", "1")
    api_app._AUDIT_LOGS = None
    api_app._TICKETS = None
    api_app._ALERTS = None
    return api_app.app.test_client()


def test_audit_login_logout_and_denied(monkeypatch, audit_client):
    # 1) 管理员登录成功 -> 记录 login / ok
    r = audit_client.post("/api/auth/login", json={"user": "伍浩庚", "role": "admin"})
    assert r.status_code == 200
    assert r.get_json()["data"]["token"] == "demo-admin"

    # 2) 只读访客尝试创建工单 -> 403 且自动记录 auth_denied / denied
    r_deny = audit_client.post(
        "/api/tickets",
        json={"station_id": "1334", "type": "repair"},
        headers={"Authorization": "Bearer demo-readonly", "X-Demo-User": quote("访客小李")},
    )
    assert r_deny.status_code == 403

    # 3) 关闭演示管理员时登录被拒 -> 503 且记录 login / denied
    monkeypatch.setenv("AUTH_DEMO_ENABLED", "0")
    r_503 = audit_client.post("/api/auth/login", json={"user": "ops_user", "role": "admin"})
    assert r_503.status_code == 503

    # 4) 退出登录 -> 记录 logout / ok
    r_out = audit_client.post("/api/auth/logout", json={"user": "伍浩庚", "role": "admin"})
    assert r_out.status_code == 200 and r_out.get_json()["data"]["logged_out"] is True

    # 5) 查询审计列表，核验全部记录按最新倒序出现
    res = audit_client.get("/api/ops/audit?page_size=20")
    assert res.status_code == 200
    data = res.get_json()["data"]
    assert data["source"] == "file"
    assert data["counts"]["denied"] >= 2
    items = data["items"]
    assert items[0]["action"] == "logout" and items[0]["actor"] == "伍浩庚"
    assert any(x["action"] == "login" and x["actor"] == "伍浩庚" and x["result"] == "ok" for x in items)
    assert any(x["action"] == "auth_denied" and x["actor"] == "访客小李" and x["result"] == "denied" for x in items)
    assert any(x["action"] == "login" and x["actor"] == "ops_user" and x["result"] == "denied" for x in items)


def test_audit_ticket_alert_filters_and_export(audit_client):
    admin_hdrs = {"Authorization": "Bearer demo-admin", "X-Demo-User": quote("运维管理员")}

    # 派发新工单
    r_new = audit_client.post("/api/tickets", json={"station_id": "1554", "type": "repair"}, headers=admin_hdrs)
    assert r_new.status_code == 201
    tid = r_new.get_json()["data"]["ticket"]["ticket_id"]

    # 流转工单状态
    r_upd = audit_client.patch(f"/api/tickets/{tid}", json={"status": "processing"}, headers=admin_hdrs)
    assert r_upd.status_code == 200

    # 处理告警
    alerts = audit_client.get("/api/alerts").get_json()["data"]
    aid = alerts[0]["alert_id"]
    r_al = audit_client.patch(f"/api/alerts/{aid}", json={"status": "ack", "note": "已安排检修"}, headers=admin_hdrs)
    assert r_al.status_code == 200

    # 按操作类型与关键词筛选
    q_tk = audit_client.get(f"/api/ops/audit?action=ticket_update&q=WO-{tid:04d}").get_json()["data"]
    assert q_tk["total"] == 1
    assert q_tk["items"][0]["actor"] == "运维管理员"
    assert "处理中" in q_tk["items"][0]["detail"]

    q_al = audit_client.get(f"/api/ops/audit?action=alert_update&q=AL-{aid:04d}").get_json()["data"]
    assert q_al["total"] == 1
    assert "已安排检修" in q_al["items"][0]["detail"]

    # 导出 CSV（含 UTF-8 BOM 并自动新增一条 export 审计）
    r_csv = audit_client.get("/api/ops/audit/export", headers=admin_hdrs)
    assert r_csv.status_code == 200
    assert r_csv.data.startswith(b"\xef\xbb\xbf")
    text = r_csv.data.decode("utf-8-sig")
    assert "日志编号,时间,操作人" in text and "运维管理员" in text

    q_exp = audit_client.get("/api/ops/audit?action=export").get_json()["data"]
    assert q_exp["total"] >= 1
    assert q_exp["items"][0]["actor"] == "运维管理员"

    # 参数校验
    assert audit_client.get("/api/ops/audit?action=invalid").status_code == 400
    assert audit_client.get("/api/ops/audit?result=invalid").status_code == 400
    assert audit_client.get("/api/ops/audit?role=invalid").status_code == 400
    assert audit_client.get("/api/ops/audit?page=abc").status_code == 400


def test_audit_read_and_write_mysql_mode(monkeypatch):
    import app as api_app

    monkeypatch.setenv("API_SOURCE", "mysql")
    monkeypatch.setenv("AUTH_DEMO_ENABLED", "1")

    db_rows = [
        {
            "log_id": 10, "actor": "db_admin", "role": "admin", "action": "login",
            "target_type": "auth", "target_id": "db_admin", "detail": "管理员 db_admin 登录系统",
            "result": "ok", "ip": "10.0.0.2", "created_at": "2022-09-14 10:00:00",
        }
    ]
    monkeypatch.setattr(api_app, "_sql", lambda q, a=(): db_rows if "sys_op_log" in q else [])

    inserted = []

    class FakeCur:
        lastrowid = 11

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def execute(self, sql, args=()):
            inserted.append((sql, args))

    class FakeConn:
        def cursor(self):
            return FakeCur()

        def commit(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(api_app, "_mysql_connect", lambda: FakeConn())

    c = api_app.app.test_client()
    r = c.get("/api/ops/audit")
    assert r.status_code == 200
    d = r.get_json()["data"]
    assert d["source"] == "mysql" and d["total"] == 1 and d["items"][0]["actor"] == "db_admin"

    r_login = c.post("/api/auth/login", json={"user": "db_admin", "role": "admin"})
    assert r_login.status_code == 200
    assert any("INSERT INTO sys_op_log" in sql for sql, _ in inserted)
