"""Flask 薄 API：对齐 docs/api_contract.md（小粉）。
默认读 data/demo/dwd CSV（小紫 ETL）；无文件时回落 MOCK。
环境：MOCK=1 强制假数据；REPO_ROOT 可改仓库根。
API_SOURCE=mysql 时 overview / zones / zone/series / baseline / quality 改读 MySQL（sz_ev 的 ads_* 表，
连接用 MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE）；返回字段不变，库里无数据或连不上时退回文件模式。
地图接口由 MAP_DB=mysql 单独控制（见 _map_db）。
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


@app.route("/api/<path:_any>", methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"])
def _api_fallback(_any):
    # 只用来应答跨域预检。以前这里只登记了 OPTIONS，旧代码里没有的接口会被它接住、回 405，
    # 看起来像"方法不对"；现在未知接口统一回 404，一眼能看出是服务端代码没更新。
    if request.method == "OPTIONS":
        return ("", 204)
    return jsonify({"ok": False, "error": f"接口不存在：/api/{_any}（服务端可能还是旧版本）"}), 404


def ok(data):
    return jsonify({"ok": True, "data": data})


def err(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def _has_dwd() -> bool:
    return (DWD / "dwd_quality_station.csv").is_file() and (DWD / "dwd_zone_hour.csv").is_file()


def use_file() -> bool:
    return (not FORCE_MOCK) and _has_dwd()


# ---- MySQL 数据源（API_SOURCE=mysql 启用；默认仍为文件模式）----
def use_mysql() -> bool:
    return (not FORCE_MOCK) and os.getenv("API_SOURCE", "file").lower() == "mysql"


def _sql(query: str, args=()) -> list[dict] | None:
    """执行只读查询；连不上或出错返回 None，调用方据此退回文件模式。"""
    try:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "sz_ev")), charset="utf8mb4",
                               cursorclass=pymysql.cursors.DictCursor, connect_timeout=3)
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(query, args)
            return list(cur.fetchall())
    except Exception:
        return None
    finally:
        conn.close()


def _num(v, nd=None):
    if v is None:
        return None
    v = float(v)
    return round(v, nd) if nd is not None else v


def _ts(v) -> str:
    return v.strftime("%Y-%m-%d %H:%M:%S") if hasattr(v, "strftime") else str(v)


def _range_args(default_limit: int):
    """from/to（含端点）→ SQL 条件；未传 to 时限制条数，与文件模式一致。"""
    f, t = request.args.get("from"), request.args.get("to")
    cond, args = "", []
    if f:
        cond += " AND ts_hour >= %s"; args.append(f)
    if t:
        cond += " AND ts_hour < DATE_ADD(%s, INTERVAL 1 DAY)"; args.append(t)
    limit = "" if t else f" LIMIT {int(default_limit)}"
    return cond, args, limit


def _first_taz(table: str):
    r = _sql(f"SELECT taz_id FROM {table} ORDER BY CAST(taz_id AS UNSIGNED), taz_id LIMIT 1")
    return r[0]["taz_id"] if r else None


def _mysql_overview(dt: str):
    r = _sql("SELECT dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt "
             "FROM ads_city_overview WHERE dt=%s", (dt[:10],))
    if not r:
        return None
    r = r[0]
    return {"dt": str(r["dt"])[:10], "station_cnt": r["station_cnt"], "zone_cnt": r["zone_cnt"],
            "avg_occupancy": _num(r["avg_occupancy"], 4), "peak_hour": r["peak_hour"],
            "stuck_cnt": r["stuck_cnt"], "high_missing_cnt": r["high_missing_cnt"]}


def _mysql_zones():
    r = _sql("SELECT DISTINCT taz_id FROM ads_zone_occupancy_hour")
    if not r:
        return None
    return sorted({str(x["taz_id"]) for x in r}, key=lambda x: int(x) if x.isdigit() else 0)


def _mysql_series(taz_id):
    taz_id = taz_id or _first_taz("ads_zone_occupancy_hour")
    if taz_id is None:
        return None
    cond, args, limit = _range_args(168)
    r = _sql("SELECT taz_id, ts_hour, occupancy, load_kw_est FROM ads_zone_occupancy_hour "
             f"WHERE taz_id=%s{cond} ORDER BY ts_hour{limit}", (taz_id, *args))
    if r is None:
        return None
    return {"taz_id": taz_id, "points": [
        {"taz_id": str(x["taz_id"]), "ts_hour": _ts(x["ts_hour"]), "occupancy": _num(x["occupancy"], 6),
         "load_kw_est": _num(x["load_kw_est"], 2)} for x in r]}


def _mysql_baseline(taz_id):
    taz_id = taz_id or _first_taz("ads_baseline_naive")
    if taz_id is None:
        return None
    cond, args, limit = _range_args(48)
    r = _sql("SELECT taz_id, ts_hour, y_true, y_hat_lag1, abs_err FROM ads_baseline_naive "
             f"WHERE taz_id=%s{cond} ORDER BY ts_hour{limit}", (taz_id, *args))
    if r is None:
        return None
    return {"taz_id": taz_id, "points": [
        {"taz_id": str(x["taz_id"]), "ts_hour": _ts(x["ts_hour"]), "y_true": _num(x["y_true"], 6),
         "y_hat_lag1": _num(x["y_hat_lag1"], 6), "abs_err": _num(x["abs_err"], 6)} for x in r]}


def _mysql_quality():
    r = _sql("SELECT metric, value, note FROM ads_quality_summary ORDER BY metric")
    if not r:
        return None
    return [{"metric": x["metric"], "value": _num(x["value"], 4), "note": x["note"]} for x in r]



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
            "mode": "mysql" if use_mysql() else ("file" if use_file() else "mock"),
            "dwd": str(DWD),
        }
    )


@app.get("/api/overview")
def overview():
    dt = request.args.get("dt") or "2022-09-01"
    if use_mysql() and (r := _mysql_overview(dt)) is not None:
        return ok(r)
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
    if use_mysql() and (r := _mysql_zones()) is not None:
        return ok(r)
    if use_file():
        ids = sorted({r["taz_id"] for r in _zone_rows()}, key=lambda x: int(x) if str(x).isdigit() else 0)
    else:
        ids = ["1011", "1062", "335"]
    return ok(ids)


@app.get("/api/zone/series")
def zone_series():
    taz_id = request.args.get("taz_id")
    if use_mysql() and (r := _mysql_series(taz_id)) is not None:
        return ok(r)
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
    if use_mysql() and (r := _mysql_quality()) is not None:
        return ok(r)
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


FORECAST = REPO / "forecast"


def _forecast_preds():
    """forecast/predictions.csv -> {(taz_id, ts_hour): row}。ts 为本地时间（UTC+8）字符串，与 dwd ts_hour 同格式。缺文件返回空。"""
    path = FORECAST / "predictions.csv"
    out = {}
    try:
        with path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                out[(str(r["taz_id"]).strip(), str(r["ts"]).strip()[:19])] = r
    except (OSError, KeyError, ValueError):
        return {}
    return out


def _forecast_metrics():
    import json
    keys = ("mae_lgbm", "mae_lag1", "rmse_lgbm", "rmse_lag1", "split_ts")
    try:
        m = json.loads((FORECAST / "metrics.json").read_text(encoding="utf-8"))
        o = m.get("overall", {})
        return {
            "mae_lgbm": o.get("lgbm", {}).get("mae"),
            "mae_lag1": o.get("lag1", {}).get("mae"),
            "rmse_lgbm": o.get("lgbm", {}).get("rmse"),
            "rmse_lag1": o.get("lag1", {}).get("rmse"),
            "split_ts": m.get("split_ts"),
        }
    except (OSError, ValueError, AttributeError):
        return {k: None for k in keys}


def _with_lgbm(points, preds):
    for p in points:
        r = preds.get((str(p["taz_id"]), p["ts_hour"]))
        try:
            yh = round(float(r["y_hat_lgbm"]), 6) if r else None
        except (TypeError, ValueError):
            yh = None
        p["y_hat_lgbm"] = yh
        p["abs_err_lgbm"] = None if yh is None else round(abs(p["y_true"] - yh), 6)
    return points


def _pred_series(taz_id, preds):
    """演示 CSV 不含预测窗口（2023-02）时，直接用 predictions.csv 的真值/上一小时值出点。"""
    pts = []
    for (z, ts), r in sorted(preds.items(), key=lambda kv: kv[0][1]):
        if z != taz_id or not _in_range(ts):
            continue
        y = float(r["y"])
        yl = float(r["y_hat_lag1"])
        pts.append({"taz_id": taz_id, "ts_hour": ts, "y_true": round(y, 6),
                    "y_hat_lag1": round(yl, 6), "abs_err": round(abs(y - yl), 6)})
    return pts


def _forecast_window():
    ts = sorted({k[1] for k in _forecast_preds()})
    return {"from": ts[0], "to": ts[-1]} if ts else None


@app.get("/api/baseline")
def baseline():
    taz_id = request.args.get("taz_id")
    if use_mysql() and (r := _mysql_baseline(taz_id)) is not None:
        preds = _forecast_preds()
        return ok({**r, "points": _with_lgbm(r["points"], preds),
                   "metrics": _forecast_metrics(), "forecast_window": _forecast_window()})
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
        preds = _forecast_preds()
        if not points and preds:
            points = _pred_series(taz_id, preds)
        return ok({"taz_id": taz_id, "points": _with_lgbm(points, preds),
                   "metrics": _forecast_metrics(), "forecast_window": _forecast_window()})
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
    return ok({"taz_id": taz_id, "points": _with_lgbm(points, {}),
               "metrics": _forecast_metrics(), "forecast_window": _forecast_window()})


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


@app.post("/api/tickets")
def tickets_create():
    """从告警派工单：{station_id, type?, alert_id?}。带 alert_id 时同一条告警只派一次，并把告警改成处理中（ack）。
    工单仍保存在进程内存（ads_ticket 表没有 alert_id 列）。"""
    denied = _require_admin()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    sid = str(body.get("station_id") or "").strip()
    typ = body.get("type") or "repair"
    if not sid:
        return err("station_id 不能为空")
    if typ not in {"repair", "inspect"}:
        return err("type must be repair|inspect")
    alert_id = body.get("alert_id")
    store = tickets_store()
    if alert_id is not None:
        alert_id = int(alert_id)
        dup = next((t for t in store if t.get("alert_id") == alert_id), None)
        if dup:
            return err(f"这条告警已经派过工单 WO-{dup['ticket_id']:04d}", 409)
    t = {"ticket_id": max((x["ticket_id"] for x in store), default=0) + 1, "station_id": sid, "type": typ,
         "status": "open", "alert_id": alert_id, "created_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
    store.append(t)
    alert = alerts_write(alert_id, "ack", f"已派工单 WO-{t['ticket_id']:04d}") if alert_id is not None else None
    return ok({"ticket": t, "alert": alert}), 201


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




# ---- 片区地图 / 告警（小黄，前端第 6、7 页）----
LAKE = os.getenv("SZ_EV_LAKE") or str(REPO / "data" / "lake")


@lru_cache(maxsize=1)
def _lake_map():
    """可选：SZ_EV_LAKE 指向数据湖根目录（含 ods/station_info、ads/zone_hour，可选 dwd/quality_station）时读 parquet。
    只读需要的列并在 pyarrow 里聚合，内存占用很小；缺任何一块就返回 None，退回 demo CSV。"""
    root = Path(LAKE)
    if not (root / "ods" / "station_info").exists() or not (root / "ads" / "zone_hour").exists():
        return None
    try:
        import pyarrow.dataset as pads
        import pyarrow.compute as pc
        si = pads.dataset(root / "ods" / "station_info", format="parquet", partitioning="hive") \
            .to_table(columns=["station_id", "longitude", "latitude", "charge_count", "taz_id"]).to_pylist()
        zds = pads.dataset(root / "ads" / "zone_hour", format="parquet", partitioning="hive")
        has_center = {"center_lon", "center_lat"} <= set(zds.schema.names)
        cols = ["taz_id", "occupancy_avg", "busy_sum", "piles_sum", "station_cnt"] + (["center_lon", "center_lat"] if has_center else [])
        zh = zds.to_table(columns=cols)
        zh = zh.set_column(0, "taz_id", pc.cast(zh["taz_id"], "string"))
        aggs = [("occupancy_avg", "mean"), ("busy_sum", "sum"), ("piles_sum", "sum"), ("piles_sum", "max"), ("station_cnt", "max")]
        if has_center:
            aggs += [("center_lon", "max"), ("center_lat", "max")]
        g = zh.group_by("taz_id").aggregate(aggs).to_pylist()
    except Exception:
        return None
    qual = {}
    try:
        qt = pads.dataset(root / "dwd" / "quality_station", format="parquet", partitioning="hive")
        cols = ["station_id", "missing_rate", "stuck_flag", "gap_days_max"] + (["run"] if "run" in qt.schema.names else [])
        for r in sorted(qt.to_table(columns=cols).to_pylist(), key=lambda r: str(r.get("run") or "")):
            qual[str(r["station_id"])] = r  # 同一站多次运行时取最新一次
    except Exception:
        pass
    occ = {str(r["taz_id"]): r for r in g}
    stations, pos = [], defaultdict(lambda: [0.0, 0.0, 0, 0])
    for r in si:
        sid, taz = str(r["station_id"]), str(r["taz_id"])
        q = qual.get(sid, {})
        mr = q.get("missing_rate")
        status = "stuck" if q.get("stuck_flag") == 1 else ("high_missing" if (mr or 0) > 0.30 else "normal")
        stations.append({"station_id": sid, "taz_id": taz, "lon": r["longitude"], "lat": r["latitude"],
                         "total_piles": r["charge_count"], "avg_occupancy": None,
                         "missing_rate": round(mr, 4) if mr is not None else None,
                         "status": status, "status_label": STATUS_LABEL[status]})
        a = pos[taz]; a[0] += r["longitude"]; a[1] += r["latitude"]; a[2] += 1; a[3] += r["charge_count"] or 0
    zones = []
    for taz, (lon, lat, n, piles) in pos.items():
        o = occ.get(taz, {})
        v = o.get("occupancy_avg_mean")
        if v is None and o.get("piles_sum_sum"):  # 有的小时 occupancy_avg 为空，用忙桩数/总桩数补
            v = (o.get("busy_sum_sum") or 0) / o["piles_sum_sum"]
        clon, clat = o.get("center_lon_max"), o.get("center_lat_max")
        zones.append({"taz_id": taz, "lon": round(clon if clon is not None else lon / n, 6),
                      "lat": round(clat if clat is not None else lat / n, 6),
                      "station_count": o.get("station_cnt_max") or n, "total_piles": piles,  # piles_sum 是 5 分钟粒度累加，不能当桩数
                      "avg_occupancy": round(v, 4) if v is not None else None})
    return zones, stations


def _mysql_map():
    """MAP_DB=mysql 时优先读库：dim_station（站点坐标）+ ads_zone_daily（片区日均、中心点，取最新一天）。连不上就返回 None。"""
    if os.getenv("MAP_DB", "").lower() != "mysql":
        return None
    try:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4", connect_timeout=3,
                               cursorclass=pymysql.cursors.DictCursor)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT station_id, taz_id, longitude, latitude, total_piles FROM dim_station")
            si = cur.fetchall()
            cur.execute("SELECT taz_id, avg_occupancy, station_cnt, pile_cnt, center_lon, center_lat FROM ads_zone_daily "
                        "WHERE dt = (SELECT MAX(dt) FROM ads_zone_daily)")
            zd = {str(r["taz_id"]): r for r in cur.fetchall()}
    except Exception:
        return None
    if not si:
        return None
    f = lambda v: float(v) if v is not None else None
    stations, pos = [], defaultdict(lambda: [0.0, 0.0, 0, 0])
    for r in si:
        taz = str(r["taz_id"])
        stations.append({"station_id": str(r["station_id"]), "taz_id": taz, "lon": f(r["longitude"]), "lat": f(r["latitude"]),
                         "total_piles": r["total_piles"], "avg_occupancy": None, "missing_rate": None,
                         "status": "normal", "status_label": STATUS_LABEL["normal"]})
        a = pos[taz]; a[0] += f(r["longitude"]); a[1] += f(r["latitude"]); a[2] += 1; a[3] += r["total_piles"] or 0
    zones = []
    for taz, (lon, lat, n, piles) in pos.items():
        z = zd.get(taz, {})
        occ = f(z.get("avg_occupancy"))
        zones.append({"taz_id": taz, "lon": round(f(z.get("center_lon")) or lon / n, 6), "lat": round(f(z.get("center_lat")) or lat / n, 6),
                      "station_count": z.get("station_cnt") or n, "total_piles": z.get("pile_cnt") or piles,
                      "avg_occupancy": round(occ, 4) if occ is not None else None})
    return zones, stations


@app.get("/api/map/zones")
def map_zones():
    """片区地图：每个片区的中心经纬度、站数、平均占用率，以及站点坐标。"""
    src = "mysql"
    lake = _mysql_map()
    if not lake:
        src, lake = "lake", _lake_map()
    if lake:
        zones, stations = lake
        taz = request.args.get("taz_id")
        if taz:
            return ok({"zones": [], "stations": [s for s in stations if s["taz_id"] == taz], "source": src})
        # 站多时不一次性返回，前端点片区时再按 taz_id 取
        return ok({"zones": zones, "stations": stations if len(stations) <= 300 else [],
                   "station_total": len(stations), "source": src})
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
ALERT_STATES = {"open", "ack", "closed"}
_ALERTS: list[dict] | None = None

# MySQL 表 dq_alert（小白在 quality/sql/ 下建表）。设置 ALERT_DB=mysql 才启用，否则用内存。
# 表结构见 quality/sql/dq_alert.sql（主键 alert_id，状态 open/ack/closed）。


def _alert_db():
    if os.getenv("ALERT_DB", "").lower() != "mysql":
        return None
    try:
        import pymysql
        return pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4",
                               cursorclass=pymysql.cursors.DictCursor, connect_timeout=3)
    except Exception:
        return None  # 连不上就退回内存


def _norm_alert(r: dict) -> dict:
    """dq_alert 一行 -> 前端字段。station_id / taz_id 为空表示整批数据（规则级）告警。"""
    sev = r.get("severity") or r.get("level")
    level = {"block": "high", "warn": "medium"}.get(sev, sev or "low")
    st = r.get("status") or "open"
    fmt = lambda v: None if v is None else str(v)[:16]
    return {
        "alert_id": r.get("alert_id"), "ds": fmt(r.get("ds")), "layer": r.get("layer"),
        "station_id": r.get("station_id"), "taz_id": r.get("taz_id"),
        "kind": r.get("kind") or r.get("rule_name"), "rule_name": r.get("rule_name"), "level": level,
        "metric_value": r.get("metric_value"), "threshold": r.get("threshold"),
        "detail": r.get("detail") or "", "status": "ack" if st == "processing" else st,
        "note": r.get("note") or "", "created_at": fmt(r.get("created_at")), "updated_at": fmt(r.get("updated_at")),
    }


def alerts_store():
    """由数据检查结果生成告警；状态保存在进程内存（无 MySQL 时的兜底）。"""
    global _ALERTS
    if _ALERTS is None:
        _ALERTS = []
        rows = _station_detail_rows() if use_file() else []
        for s in rows:
            kinds = []
            if s["status"] == "stuck":
                kinds.append(("stuck", "读数长时间不变"))
            if s["missing_rate"] > 0.30:
                kinds.append(("high_missing", "缺的数据太多"))
            if s["gap_days_max"] >= 2:
                kinds.append(("long_gap", "最长断档天数（天）"))
            for k, detail in kinds:
                mv, th = {"stuck": (1, None), "high_missing": (s["missing_rate"], 0.30), "long_gap": (s["gap_days_max"], 2)}[k]
                _ALERTS.append({"alert_id": len(_ALERTS) + 1, "ds": "2022-09-14", "layer": "dwd",
                                "station_id": s["station_id"], "taz_id": s["taz_id"], "kind": k, "rule_name": k,
                                "level": ALERT_LEVEL[k], "metric_value": mv, "threshold": th, "detail": detail,
                                "status": "open", "note": "", "created_at": "2022-09-14 23:55", "updated_at": None})
        # 规则级（整批数据）示例：整体缺数比例
        qr = _quality_rows() if use_file() else []
        tot = sum(int(float(r.get("rows") or 0)) for r in qr)
        q = {"row_missing_rate": round(sum(int(float(r.get("missing_rows") or 0)) for r in qr) / tot, 4)} if tot else {}
        if q.get("row_missing_rate", 0) > 0.2:
            _ALERTS.append({"alert_id": len(_ALERTS) + 1, "ds": "2022-09-14", "layer": "dwd", "station_id": None,
                            "taz_id": None, "kind": "row_missing_rate", "rule_name": "row_missing_rate", "level": "medium",
                            "metric_value": q["row_missing_rate"], "threshold": 0.2,
                            "detail": "整批数据缺数比例偏高", "status": "open", "note": "",
                            "created_at": "2022-09-14 23:55", "updated_at": None})
    return _ALERTS


def alerts_read() -> tuple[list[dict], str]:
    con = _alert_db()
    if con:
        try:
            with con, con.cursor() as cur:
                cur.execute("SELECT * FROM dq_alert ORDER BY created_at DESC LIMIT 1000")
                return [_norm_alert(r) for r in cur.fetchall()], "mysql"
        except Exception:
            pass
    return alerts_store(), "memory"


def alerts_write(alert_id: int, status: str, note: str | None):
    con = _alert_db()
    if con:
        try:
            with con, con.cursor() as cur:
                if note is None:
                    n = cur.execute("UPDATE dq_alert SET status=%s, updated_at=NOW() WHERE alert_id=%s", (status, alert_id))
                else:
                    n = cur.execute("UPDATE dq_alert SET status=%s, note=%s, updated_at=NOW() WHERE alert_id=%s",
                                    (status, str(note)[:500], alert_id))
                con.commit()
                if n:
                    cur.execute("SELECT * FROM dq_alert WHERE alert_id=%s", (alert_id,))
                    return _norm_alert(cur.fetchone())
                return None
        except Exception:
            pass
    for a in alerts_store():
        if a["alert_id"] == alert_id:
            a["status"] = status
            if note is not None:
                a["note"] = str(note)[:500]
            a["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            return a
    return None


@app.get("/api/alerts")
def alerts_list():
    rows, _src = alerts_read()
    st = request.args.get("status")
    return ok([a for a in rows if not st or a["status"] == st])


@app.patch("/api/alerts/<int:alert_id>")
def alerts_patch(alert_id):
    denied = _require_admin()
    if denied:
        return denied
    body = request.get_json(silent=True) or {}
    status = body.get("status")
    if status == "processing":
        status = "ack"  # 兼容旧前端
    if status not in ALERT_STATES:
        return err("status must be open|ack|closed")
    a = alerts_write(alert_id, status, body.get("note"))
    return ok(a) if a else err("alert not found", 404)


# 必须放在文件末尾：python app.py 时 app.run() 会阻塞，写在它后面的路由（地图、告警）不会注册。
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
