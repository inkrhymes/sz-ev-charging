"""tickets HTTP routes.

The handlers stay behavior-compatible with the original monolithic
api/app.py. Shared data-source helpers remain in app.py during the
incremental refactor so existing tests and fallback behavior are preserved.
"""
from datetime import datetime, timedelta
from collections import defaultdict
import csv
import os

from flask import g, jsonify, request
import app as appmod
from app import alerts_store, alerts_write, demo_auth_enabled, err, ok, tickets_store

def tickets_list():
    used, rows = appmod._ticket_db_call(lambda cur: (cur.execute("SELECT * FROM ads_ticket ORDER BY ticket_id"), cur.fetchall())[1])
    if used:
        return ok([appmod._ticket_out(r) for r in rows])
    return ok(tickets_store())



def tickets_patch(ticket_id):
    denied = appmod._require_admin()
    if denied:
        return denied
    body, problem = appmod._json_body()
    if problem:
        return problem
    status = body.get("status")
    if not isinstance(status, str) or status not in {"open", "processing", "done"}:
        return err("status must be open|processing|done")
    def upd(cur):
        # started_at：第一次进入 processing 时记下开始处理时间，之后不再改（见 sql/migrations/20261025_ads_ticket_started_at.sql）
        sql = ("UPDATE ads_ticket SET status=%s, updated_at=NOW()"
               + (", started_at=COALESCE(started_at, NOW())" if status == "processing" else "") + " WHERE ticket_id=%s")
        if not cur.execute(sql, (status, ticket_id)):
            cur.execute("SELECT 1 FROM ads_ticket WHERE ticket_id=%s", (ticket_id,))
            if not cur.fetchone():
                return None
        cur.execute("SELECT * FROM ads_ticket WHERE ticket_id=%s", (ticket_id,))
        return appmod._ticket_out(cur.fetchone())
    used, t = appmod._ticket_db_call(upd, write=True)
    if used:
        return ok(t) if t else err("ticket not found", 404)
    for t in tickets_store():
        if t["ticket_id"] == ticket_id:
            t["status"] = status
            if status == "processing" and not t.get("started_at"):
                t["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            return ok(t)
    return err("ticket not found", 404)



def tickets_create():
    """从告警派工单；MySQL 模式下工单与告警状态在同一事务内更新。"""
    denied = appmod._require_admin()
    if denied:
        return denied
    body, problem = appmod._json_body()
    if problem:
        return problem
    sid_raw = body.get("station_id")
    if isinstance(sid_raw, bool) or not isinstance(sid_raw, (str, int)):
        return err("station_id 必须是字符串或整数")
    sid = str(sid_raw).strip()
    if not sid:
        return err("station_id 不能为空")
    if len(sid) > 64:
        return err("station_id 不能超过 64 个字符")
    typ = body.get("type") or "repair"
    if not isinstance(typ, str) or typ not in {"repair", "inspect"}:
        return err("type must be repair|inspect")
    alert_id = body.get("alert_id")
    if alert_id is not None:
        alert_id = appmod._positive_int(alert_id)
        if alert_id is None:
            return err("alert_id 必须是正整数")

    def ins(cur):
        if alert_id is not None:
            # 锁住告警行，再检查工单，避免并发派工把同一告警派成两张工单。
            cur.execute("SELECT alert_id FROM dq_alert WHERE alert_id=%s FOR UPDATE", (alert_id,))
            if not cur.fetchone():
                return {"alert_missing": True}
            cur.execute("SELECT ticket_id FROM ads_ticket WHERE alert_id=%s LIMIT 1", (alert_id,))
            d = cur.fetchone()
            if d:
                return {"dup": d["ticket_id"]}
        cur.execute("INSERT INTO ads_ticket (station_id, alert_id, type, status) VALUES (%s, %s, %s, 'open')",
                    (sid, alert_id, typ))
        cur.execute("SELECT * FROM ads_ticket WHERE ticket_id=%s", (cur.lastrowid,))
        row = cur.fetchone()
        ticket = appmod._ticket_out(row) if row else None
        if ticket is None:
            raise RuntimeError("工单插入后未能读取记录")
        alert = None
        if alert_id is not None:
            note = f"已派工单 WO-{ticket['ticket_id']:04d}"
            cur.execute("UPDATE dq_alert SET status=%s, note=%s, updated_at=NOW() WHERE alert_id=%s",
                        ("ack", note, alert_id))
            if getattr(cur, "rowcount", 0):
                cur.execute("SELECT * FROM dq_alert WHERE alert_id=%s", (alert_id,))
                row = cur.fetchone()
                alert = appmod._norm_alert(row) if row else None
        return {"ticket": ticket, "alert": alert}

    used, result = appmod._ticket_db_call(ins, write=True)
    if used:
        if "alert_missing" in result:
            return err("alert not found", 404)
        if "dup" in result:
            return err(f"这条告警已经派过工单 WO-{result['dup']:04d}", 409)
        return ok(result), 201
    store = tickets_store()
    if alert_id is not None:
        alert = next((a for a in alerts_store() if a.get("alert_id") == alert_id), None)
        if alert is None:
            return err("alert not found", 404)
        dup = next((t for t in store if t.get("alert_id") == alert_id), None)
        if dup:
            return err(f"这条告警已经派过工单 WO-{dup['ticket_id']:04d}", 409)
    ticket = {"ticket_id": max((x["ticket_id"] for x in store), default=0) + 1, "station_id": sid, "type": typ,
              "status": "open", "alert_id": alert_id, "created_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
    store.append(ticket)
    alert = alerts_write(alert_id, "ack", f"已派工单 WO-{ticket['ticket_id']:04d}") if alert_id is not None else None
    return ok({"ticket": ticket, "alert": alert}), 201



def auth_login():
    body, problem = appmod._json_body()
    if problem:
        return problem
    user = body.get("user") or "demo"
    role = body.get("role") or "readonly"
    if not isinstance(user, str) or not user.strip() or len(user) > 64:
        return err("user 必须是 1–64 个字符")
    if not isinstance(role, str) or role not in {"readonly", "admin"}:
        return err("role must be readonly|admin")
    if role == "admin" and not demo_auth_enabled():
        return err("演示管理员认证未启用；仅可在本地演示环境设置 AUTH_DEMO_ENABLED=1", 503)
    return ok({"user": user.strip(), "role": role, "token": f"demo-{role}"})




def register(app):
    app.add_url_rule("/api/tickets", endpoint="tickets_list", view_func=tickets_list, methods=["GET"])
    app.add_url_rule("/api/tickets/<int:ticket_id>", endpoint="tickets_patch", view_func=tickets_patch, methods=["PATCH"])
    app.add_url_rule("/api/tickets", endpoint="tickets_create", view_func=tickets_create, methods=["POST"])
    app.add_url_rule("/api/auth/login", endpoint="auth_login", view_func=auth_login, methods=["POST"])
