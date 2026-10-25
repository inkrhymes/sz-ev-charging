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


@app.get("/api/zones")
def zones_list():
    if use_file():
        ids = sorted({r["taz_id"] for r in _zone_rows()}, key=lambda x: int(x) if str(x).isdigit() else 0)
    else:
        ids = ["1011", "1062", "335"]
    return ok(ids)


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


def _require_admin():
    """角色只认请求头 Authorization: Bearer <token>（token 由 /api/auth/login 下发，形如 demo-admin），不信任请求体里的 role。
    返回 None 表示放行，否则返回错误响应。"""
    h = request.headers.get("Authorization", "")
    token = h[7:].strip() if h.lower().startswith("bearer ") else ""
    role = {"demo-admin": "admin", "demo-readonly": "readonly"}.get(token)
    if role is None:
        return err("未登录或登录已失效，请先登录", 401)
    if role != "admin":
        return err("访客为只读权限，无法修改，请使用管理员登录", 403)
    return None


@app.patch("/api/tickets/<int:ticket_id>")
def tickets_patch(ticket_id):
    denied = _require_admin()
    if denied:
        return denied
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


# ---- 站点明细（数据质量 / 运维工单表格页）----
STATUS_LABEL = {"stuck": "疑似卡死", "high_missing": "高缺失", "normal": "正常"}


@lru_cache(maxsize=1)
def _station_detail_rows():
    import gzip

    q = {r["station_id"]: r for r in _quality_rows()}
    demo_list = REPO / "data" / "demo" / "demo_station_list.csv"
    piles = {}
    if demo_list.is_file():
        with demo_list.open(encoding="utf-8") as f:
            for r in csv.DictReader(f):
                piles[r["sid"]] = int(float(r.get("charge_count") or 0))
    occ_sum, occ_n, vol = defaultdict(float), defaultdict(int), defaultdict(float)
    clean = DWD / "dwd_station_5min_clean.csv"
    opener = None
    if clean.is_file():
        opener = lambda: clean.open(encoding="utf-8")
    elif clean.with_suffix(".csv.gz").is_file():
        opener = lambda: gzip.open(clean.with_suffix(".csv.gz"), "rt", encoding="utf-8")
    if opener:
        with opener() as f:
            for r in csv.DictReader(f):
                sid = r["station_id"]
                if r.get("occupancy"):
                    occ_sum[sid] += float(r["occupancy"])
                    occ_n[sid] += 1
                if r.get("volume"):
                    vol[sid] += float(r["volume"])
    rows = []
    for sid, r in q.items():
        mr = float(r.get("missing_rate") or 0)
        stuck = r.get("stuck_flag") in {"1", "1.0"}
        status = "stuck" if stuck else ("high_missing" if mr > 0.30 else "normal")
        avg = round(occ_sum[sid] / occ_n[sid], 4) if occ_n[sid] else None
        rows.append({
            "station_id": sid,
            "taz_id": r.get("taz_id"),
            "total_piles": piles.get(sid),
            "avg_occupancy": avg,
            "total_volume_kwh": round(vol[sid], 2) if sid in vol else None,
            "missing_rate": round(mr, 4),
            "gap_days_max": int(float(r.get("gap_days_max") or 0)),
            "status": status,
            "status_label": STATUS_LABEL[status],
            "ticket_suggest": r.get("ticket_suggest") or "none",
        })
    ranked = sorted([x for x in rows if x["avg_occupancy"] is not None], key=lambda x: -x["avg_occupancy"])
    for i, x in enumerate(ranked, 1):
        x["occupancy_rank"] = i
    for x in rows:
        x.setdefault("occupancy_rank", None)
    return rows


@app.get("/api/stations")
def stations():
    """站点明细：?status=stuck|high_missing|normal &taz_id= &sort=avg_occupancy|missing_rate|station_id &order=asc|desc &page=1 &page_size=10"""
    if not use_file():
        return ok({"total": 0, "page": 1, "page_size": 10, "items": []})
    rows = list(_station_detail_rows())
    st = request.args.get("status")
    if st:
        rows = [r for r in rows if r["status"] == st]
    taz = request.args.get("taz_id")
    if taz:
        rows = [r for r in rows if str(r["taz_id"]) == taz]
    sort = request.args.get("sort", "avg_occupancy")
    if sort not in {"avg_occupancy", "missing_rate", "station_id", "total_volume_kwh"}:
        return err("bad sort")
    desc = request.args.get("order", "desc") == "desc"
    rows.sort(key=lambda r: (r[sort] is None, r[sort] if r[sort] is not None else 0), reverse=False)
    if desc:
        nn = [r for r in rows if r[sort] is not None][::-1]
        rows = nn + [r for r in rows if r[sort] is None]
    try:
        page = max(1, int(request.args.get("page", 1)))
        size = min(100, max(1, int(request.args.get("page_size", 10))))
    except ValueError:
        return err("bad page")
    total = len(rows)
    items = rows[(page - 1) * size: page * size]
    return ok({"total": total, "page": page, "page_size": size, "items": items})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)


# ---- 片区地图 / 告警（小黄，前端第 6、7 页）----
@app.get("/api/map/zones")
def map_zones():
    """片区地图：每个片区的中心经纬度、站数、平均占用率，以及站点坐标。"""
    if not use_file():
        return ok({"zones": [], "stations": []})
    demo_list = REPO / "data" / "demo" / "demo_station_list.csv"
    coords = {}
    if demo_list.is_file():
        with demo_list.open(encoding="utf-8") as f:
            for r in csv.DictReader(f):
                coords[r["sid"]] = (float(r["longitude"]), float(r["latitude"]))
    stations_out, agg = [], defaultdict(lambda: {"lon": 0.0, "lat": 0.0, "n": 0, "occ": [], "piles": 0})
    for s in _station_detail_rows():
        if s["station_id"] not in coords:
            continue
        lon, lat = coords[s["station_id"]]
        stations_out.append({**s, "lon": lon, "lat": lat})
        a = agg[str(s["taz_id"])]
        a["lon"] += lon; a["lat"] += lat; a["n"] += 1; a["piles"] += s["total_piles"] or 0
        if s["avg_occupancy"] is not None:
            a["occ"].append(s["avg_occupancy"])
    zones = [{
        "taz_id": z, "lon": round(a["lon"] / a["n"], 6), "lat": round(a["lat"] / a["n"], 6),
        "station_count": a["n"], "total_piles": a["piles"],
        "avg_occupancy": round(sum(a["occ"]) / len(a["occ"]), 4) if a["occ"] else None,
    } for z, a in agg.items()]
    return ok({"zones": zones, "stations": stations_out})


ALERT_LEVEL = {"stuck": "high", "high_missing": "medium", "long_gap": "low"}
_ALERTS: list[dict] | None = None


def alerts_store():
    """由数据检查结果生成告警；状态保存在进程内存。TODO: MySQL 模式落表 ads_alert。"""
    global _ALERTS
    if _ALERTS is None:
        _ALERTS = []
        rows = _station_detail_rows() if use_file() else []
        for s in rows:
            kinds = []
            if s["status"] == "stuck":
                kinds.append(("stuck", "读数长时间不变"))
            if s["missing_rate"] > 0.30:
                kinds.append(("high_missing", f"缺数比例 {s['missing_rate']*100:.1f}%"))
            if s["gap_days_max"] >= 2:
                kinds.append(("long_gap", f"最长断档 {s['gap_days_max']} 天"))
            for k, detail in kinds:
                _ALERTS.append({"alert_id": len(_ALERTS) + 1, "station_id": s["station_id"], "taz_id": s["taz_id"],
                                "kind": k, "level": ALERT_LEVEL[k], "detail": detail, "status": "open",
                                "created_at": "2022-09-14 23:55", "note": ""})
    return _ALERTS


@app.get("/api/alerts")
def alerts_list():
    return ok(alerts_store())


@app.patch("/api/alerts/<int:alert_id>")
def alerts_patch(alert_id):
    denied = _require_admin()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    status = body.get("status")
    if status not in {"open", "processing", "closed"}:
        return err("status must be open|processing|closed")
    for a in alerts_store():
        if a["alert_id"] == alert_id:
            a["status"] = status
            a["note"] = str(body.get("note") or a["note"])[:200]
            return ok(a)
    return err("alert not found", 404)
