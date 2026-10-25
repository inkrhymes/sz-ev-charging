"""Flask 薄 API：对齐 docs/api_contract.md（小粉）。
默认读 data/demo/dwd CSV（小紫 ETL）；无文件时回落 MOCK。
环境：MOCK=1 强制假数据；REPO_ROOT 可改仓库根。
"""
from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__)

REPO = Path(os.getenv("REPO_ROOT", str(Path(__file__).resolve().parents[1])))
DWD = REPO / "data" / "demo" / "dwd"
FORCE_MOCK = os.getenv("MOCK", "0") == "1"


@app.after_request
def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,OPTIONS"
    return resp


def _in_range(ts: str) -> bool:
    """按 ?from=YYYY-MM-DD&to=YYYY-MM-DD（含端点）过滤；未传则不过滤。"""
    f, t = request.args.get("from"), request.args.get("to")
    d = ts[:10]
    return (not f or d >= f) and (not t or d <= t)


@app.get("/")
def dashboard():
    """同源托管大屏 web/index.html，打开 http://localhost:5000 即可访问。"""
    return send_from_directory(str(REPO / "web"), "index.html")


@app.route("/api/<path:_any>", methods=["OPTIONS"])
def _options(_any):
    return ("", 204)


def ok(data):
    return jsonify({"ok": True, "data": data})


def err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def _has_dwd() -> bool:
    return (DWD / "dwd_quality_station.csv").is_file() and (DWD / "dwd_zone_hour.csv").is_file()


def use_file() -> bool:
    return (not FORCE_MOCK) and _has_dwd()


@lru_cache(maxsize=1)
def _quality_rows():
    path = DWD / "dwd_quality_station.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@lru_cache(maxsize=1)
def _zone_rows():
    path = DWD / "dwd_zone_hour.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _tickets_from_quality():
    tickets = []
    tid = 1
    for r in _quality_rows():
        sug = (r.get("ticket_suggest") or "none").strip()
        if sug in {"repair", "inspect"}:
            tickets.append(
                {
                    "ticket_id": tid,
                    "station_id": r["station_id"],
                    "type": sug,
                    "status": "open" if tid % 3 else "processing",
                }
            )
            tid += 1
    return tickets


# mutable ticket state once loaded
_TICKETS: list[dict] | None = None


def tickets_store():
    global _TICKETS
    if _TICKETS is None:
        if use_file():
            _TICKETS = _tickets_from_quality()
        else:
            _TICKETS = [
                {"ticket_id": 1, "station_id": "1554", "type": "repair", "status": "open"},
                {"ticket_id": 2, "station_id": "1010", "type": "inspect", "status": "open"},
                {"ticket_id": 3, "station_id": "2285", "type": "inspect", "status": "processing"},
            ]
    return _TICKETS


@app.get("/api/health")
def health():
    return ok(
        {
            "service": "sz-ev-api",
            "mode": "file" if use_file() else "mock",
            "dwd": str(DWD),
        }
    )


@app.get("/api/overview")
def overview():
    dt = request.args.get("dt") or "2022-09-01"
    if use_file():
        q = _quality_rows()
        z = _zone_rows()
        stuck = sum(1 for r in q if r.get("stuck_flag") in {"1", "1.0", 1})
        high = sum(1 for r in q if float(r.get("missing_rate") or 0) > 0.30)
        zones = {r["taz_id"] for r in q} | {r["taz_id"] for r in z}
        # avg occupancy & peak hour on first day window
        by_hour = defaultdict(list)
        for r in z:
            if not r["ts_hour"].startswith(dt[:10]):
                continue
            try:
                hour = int(r["ts_hour"][11:13])
                by_hour[hour].append(float(r["occupancy_avg"]))
            except (ValueError, IndexError):
                continue
        peak_hour = 19
        avg_occ = 0.0
        if by_hour:
            peak_hour = max(by_hour, key=lambda h: sum(by_hour[h]) / len(by_hour[h]))
            allv = [v for vs in by_hour.values() for v in vs]
            avg_occ = round(sum(allv) / len(allv), 4) if allv else 0.0
        else:
            allv = [float(r["occupancy_avg"]) for r in z[:500]]
            avg_occ = round(sum(allv) / len(allv), 4) if allv else 0.0
        return ok(
            {
                "dt": dt[:10],
                "station_cnt": len(q),
                "zone_cnt": len(zones),
                "avg_occupancy": avg_occ,
                "peak_hour": peak_hour,
                "stuck_cnt": stuck,
                "high_missing_cnt": high,
            }
        )
    return ok(
        {
            "dt": dt[:10],
            "station_cnt": 25,
            "zone_cnt": 23,
            "avg_occupancy": 0.42,
            "peak_hour": 19,
            "stuck_cnt": 4,
            "high_missing_cnt": 6,
        }
    )


@app.get("/api/zone/series")
def zone_series():
    taz_id = request.args.get("taz_id")
    if use_file():
        rows = _zone_rows()
        if not taz_id:
            taz_id = rows[0]["taz_id"] if rows else "1011"
        points = []
        for r in rows:
            if r["taz_id"] != taz_id or not _in_range(r["ts_hour"]):
                continue
            busy = float(r.get("busy_sum") or 0)
            piles = float(r.get("piles_sum") or 0) or 1
            occ = float(r.get("occupancy_avg") or 0)
            # load ≈ occupancy * piles * 7kW rough demo
            points.append(
                {
                    "taz_id": taz_id,
                    "ts_hour": r["ts_hour"],
                    "occupancy": round(occ, 6),
                    "load_kw_est": round(occ * piles * 7.0, 2),
                }
            )
            if len(points) >= 168 and not request.args.get("to"):
                break
        return ok({"taz_id": taz_id, "points": points})
    taz_id = taz_id or "1011"
    base = datetime.fromisoformat("2022-09-01T00:00:00")
    series = [
        {
            "taz_id": taz_id,
            "ts_hour": (base + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "occupancy": round(0.3 + 0.2 * ((i % 24) / 24), 4),
            "load_kw_est": round(120 + 40 * ((i % 24) / 24), 2),
        }
        for i in range(48)
    ]
    return ok({"taz_id": taz_id, "points": series})


@app.get("/api/quality")
def quality():
    if use_file():
        q = _quality_rows()
        stuck = sum(1 for r in q if r.get("stuck_flag") in {"1", "1.0", 1})
        high = sum(1 for r in q if float(r.get("missing_rate") or 0) > 0.30)
        # row missing from report: prefer summing missing_rows/rows
        miss = sum(int(float(r.get("missing_rows") or 0)) for r in q)
        total = sum(int(float(r.get("rows") or 0)) for r in q) or 1
        return ok(
            [
                {
                    "metric": "row_missing_rate",
                    "value": round(miss / total, 4),
                    "note": f"{miss}/{total} 实测",
                },
                {"metric": "stuck_station_cnt", "value": stuck, "note": "演示子集实测"},
                {
                    "metric": "high_missing_station_cnt",
                    "value": high,
                    "note": "missing_rate>0.30",
                },
                {"metric": "demo_station_cnt", "value": len(q), "note": "锁定 25 站"},
            ]
        )
    return ok(
        [
            {"metric": "row_missing_rate", "value": 0.2642, "note": "质检报告实测"},
            {"metric": "stuck_station_cnt", "value": 4, "note": "演示子集实测"},
            {"metric": "high_missing_station_cnt", "value": 6, "note": "missing_rate>0.30"},
            {"metric": "demo_station_cnt", "value": 25, "note": "锁定 25 站"},
        ]
    )


@app.get("/api/baseline")
def baseline():
    taz_id = request.args.get("taz_id")
    if use_file():
        rows = _zone_rows()
        if not taz_id:
            taz_id = rows[0]["taz_id"] if rows else "1011"
        series = [r for r in rows if r["taz_id"] == taz_id and _in_range(r["ts_hour"])]
        if not request.args.get("to"):
            series = series[:48]
        points = []
        prev = None
        for r in series:
            y = float(r["occupancy_avg"])
            yhat = prev if prev is not None else y
            points.append(
                {
                    "taz_id": taz_id,
                    "ts_hour": r["ts_hour"],
                    "y_true": round(y, 6),
                    "y_hat_lag1": round(yhat, 6),
                    "abs_err": round(abs(y - yhat), 6),
                }
            )
            prev = y
        return ok({"taz_id": taz_id, "points": points})
    taz_id = taz_id or "1011"
    base = datetime.fromisoformat("2022-09-01T01:00:00")
    points = []
    prev = 0.35
    for i in range(24):
        y = round(0.35 + 0.15 * ((i % 12) / 12), 4)
        points.append(
            {
                "taz_id": taz_id,
                "ts_hour": (base + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
                "y_true": y,
                "y_hat_lag1": prev,
                "abs_err": round(abs(y - prev), 4),
            }
        )
        prev = y
    return ok({"taz_id": taz_id, "points": points})


@app.get("/api/tickets")
def tickets_list():
    return ok(tickets_store())


@app.patch("/api/tickets/<int:ticket_id>")
def tickets_patch(ticket_id):
    body = request.get_json(silent=True) or {}
    status = body.get("status")
    if status not in {"open", "processing", "done"}:
        return err("status must be open|processing|done")
    for t in tickets_store():
        if t["ticket_id"] == ticket_id:
            t["status"] = status
            return ok(t)
    return err("ticket not found", 404)


@app.post("/api/auth/login")
def auth_login():
    body = request.get_json(silent=True) or {}
    user = body.get("user") or "demo"
    role = body.get("role") or "readonly"
    if role not in {"readonly", "admin"}:
        return err("role must be readonly|admin")
    return ok({"user": user, "role": role, "token": f"demo-{role}"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
