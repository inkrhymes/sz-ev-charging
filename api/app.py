"""Flask 薄 API：对齐 docs/api_contract.md（小粉）。
默认读 data/demo/dwd CSV（小紫 ETL）；无文件时回落 MOCK。
环境：MOCK=1 强制假数据；REPO_ROOT 可改仓库根。
API_SOURCE=mysql 时 overview / zones / zone/series / baseline / quality 改读 MySQL（sz_ev 的 ads_* 表，
连接用 MYSQL_HOST/MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE）；返回字段不变，库里无数据或连不上时退回文件模式。
地图 / 告警 / 工单也跟随 API_SOURCE=mysql；MAP_DB、ALERT_DB 仍可单独覆盖（=mysql 强制开，=file/memory 强制关），见 _db_on。
"""
from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode

from flask import Flask, g, has_request_context, jsonify, request, send_from_directory

app = Flask(__name__)

REPO = Path(os.getenv("REPO_ROOT", str(Path(__file__).resolve().parents[1])))
DWD = REPO / "data" / "demo" / "dwd"
FORCE_MOCK = os.getenv("MOCK", "0") == "1"


def _cors_allowed_origins() -> set[str]:
    raw = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:5096,http://127.0.0.1:5096")
    return {origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()}


@app.after_request
def _cors(resp):
    origin = request.headers.get("Origin")
    allowed = _cors_allowed_origins()
    if origin and (origin in allowed or "*" in allowed):
        resp.headers["Access-Control-Allow-Origin"] = "*" if "*" in allowed else origin
        resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,PUT,DELETE,OPTIONS"
        resp.headers["Access-Control-Max-Age"] = "600"
        if "*" not in allowed:
            resp.vary.add("Origin")
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


def _json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return None, err("请求体必须是 JSON 对象")
    return body, None


def _positive_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = value
    elif isinstance(value, float) and value.is_integer():
        number = int(value)
    elif isinstance(value, str) and value.strip().isdecimal():
        try:
            number = int(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if number > 0 else None


def _has_dwd() -> bool:
    return (DWD / "dwd_quality_station.csv").is_file() and (DWD / "dwd_zone_hour.csv").is_file()


def use_file() -> bool:
    return (not FORCE_MOCK) and _has_dwd()


# ---- MySQL 数据源（API_SOURCE=mysql 启用；默认仍为文件模式）----
def use_mysql() -> bool:
    return (not FORCE_MOCK) and os.getenv("API_SOURCE", "file").lower() == "mysql"


def _db_on(override: str) -> bool:
    """某块功能是否走 MySQL：单独的开关变量（MAP_DB / ALERT_DB）有值时以它为准，否则跟随 API_SOURCE。"""
    v = os.getenv(override, "").strip().lower()
    return v == "mysql" if v else use_mysql()


class MySQLUnavailable(Exception):
    """API_STRICT_MYSQL=1 时 MySQL 不可用，直接 503，不退回文件。"""


class MySQLWriteUnavailable(MySQLUnavailable):
    """MySQL 写失败时拒绝写入，不回退到进程内存。"""


def strict_mysql() -> bool:
    return os.getenv("API_STRICT_MYSQL", "").strip().lower() in ("1", "true", "yes", "on")


def _mysql_failed(e: Exception) -> None:
    """MySQL 模式下读库失败：记录到本次请求（响应头 X-Data-Fallback / /api/health 可见），并打日志；严格模式直接抛出 → 503。"""
    msg = f"{type(e).__name__}: {e}"
    try:
        g.mysql_error = msg
    except RuntimeError:  # 不在请求上下文
        pass
    app.logger.warning("MySQL 不可用，%s：%s", "严格模式返回 503" if strict_mysql() else "本次退回文件/内存数据", msg)
    if strict_mysql():
        raise MySQLUnavailable(msg)


@app.errorhandler(MySQLUnavailable)
def _on_mysql_unavailable(e):
    return jsonify({"ok": False, "error": f"MySQL 不可用（API_STRICT_MYSQL=1，不退回文件）：{e}",
                    "source": "mysql", "fallback": False, "mysql_error": str(e)}), 503


@app.errorhandler(MySQLWriteUnavailable)
def _on_mysql_write_unavailable(_e):
    return jsonify({"ok": False, "error": "MySQL 不可用，写操作已拒绝；未切换到内存存储，请检查数据库连接后重试",
                    "source": "mysql", "fallback": False}), 503


@app.after_request
def _data_source_headers(resp):
    """MySQL 模式下每个 /api 响应都带上实际数据源，退回文件时不再“悄悄”。"""
    if use_mysql() and request.path.startswith("/api/"):
        err_msg = getattr(g, "mysql_error", None)
        resp.headers["X-Data-Source"] = "file" if err_msg else "mysql"
        resp.headers["X-Data-Fallback"] = "true" if err_msg else "false"
    return resp


def _mysql_connect():
    import pymysql
    return pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                           user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                           database=os.getenv("MYSQL_DATABASE", os.getenv("MYSQL_DB", "sz_ev")), charset="utf8mb4",
                           init_command="SET time_zone='+08:00'", cursorclass=pymysql.cursors.DictCursor, connect_timeout=3)


def _mysql_connection_for_query():
    """同一 HTTP 请求复用一个连接；非请求上下文独立管理。"""
    if has_request_context():
        conn = getattr(g, "_mysql_conn", None)
        if conn is None:
            conn = _mysql_connect()
            g._mysql_conn = conn
        return conn, False
    return _mysql_connect(), True


def _close_mysql_connection(conn):
    try:
        conn.close()
    except Exception as e:
        app.logger.warning("关闭 MySQL 连接失败：%s", type(e).__name__)


@app.teardown_request
def _close_request_mysql_connection(_error=None):
    conn = getattr(g, "_mysql_conn", None)
    if conn is not None:
        g._mysql_conn = None
        _close_mysql_connection(conn)


def _sql(query: str, args=()) -> list[dict] | None:
    """执行只读查询；请求内复用连接，出错时关闭坏连接并按原策略回退或返回 503。"""
    conn = None
    owned = False
    try:
        conn, owned = _mysql_connection_for_query()
        with conn.cursor() as cur:
            cur.execute(query, args)
            return list(cur.fetchall())
    except Exception as e:
        if conn is not None and not owned:
            _close_mysql_connection(conn)
            try:
                if getattr(g, "_mysql_conn", None) is conn:
                    g._mysql_conn = None
            except RuntimeError:
                pass
        _mysql_failed(e)
        return None
    finally:
        if conn is not None and owned:
            _close_mysql_connection(conn)


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
    r = _sql("SELECT taz_id, ts_hour, occupancy, load_kw_est, power_coverage FROM ads_zone_occupancy_hour "
             f"WHERE taz_id=%s{cond} ORDER BY ts_hour{limit}", (taz_id, *args))
    if r is None:
        return None
    return {"taz_id": taz_id, "points": [
        {"taz_id": str(x["taz_id"]), "ts_hour": _ts(x["ts_hour"]), "occupancy": _num(x["occupancy"], 6),
         "load_kw_est": _num(x["load_kw_est"], 2),
         "power_coverage": _num(x.get("power_coverage"), 4)} for x in r]}


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


def _mysql_forecast_preds(taz_id):
    """MySQL 模式优先读每日生产预测表 ads_forecast_zone_hour（forecast/daily.py 写入）。
    返回 ({(taz_id, ts_hour): {"y_hat_lgbm": ..}}, forecast_window)；表不存在/为空时返回 ({}, None)，调用方退回 predictions.csv。"""
    try:
        conn = _mysql_connect()
    except Exception:
        return {}, None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(ts_hour) AS t0, MAX(ts_hour) AS t1 FROM ads_forecast_zone_hour")
            w = cur.fetchone() or {}
            if not w.get("t0"):
                return {}, None
            cur.execute("SELECT taz_id, ts_hour, y_hat_lgbm FROM ads_forecast_zone_hour WHERE taz_id=%s", (taz_id,))
            rows = cur.fetchall()
    except Exception:  # 表还没建（老库）→ 静默退回 predictions.csv
        return {}, None
    finally:
        conn.close()
    preds = {(str(x["taz_id"]), _ts(x["ts_hour"])): {"y_hat_lgbm": x["y_hat_lgbm"]} for x in rows}
    return preds, {"from": _ts(w["t0"]), "to": _ts(w["t1"])}


# 工单：API_SOURCE=mysql（或 ALERT_DB=mysql）时存 ads_ticket（含 alert_id 列，见 sql/migrations/20261025_ads_ticket_alert_id.sql），否则进程内存
def _ticket_out(r: dict) -> dict:
    fmt = lambda v: None if v is None else str(v)[:16]
    return {"ticket_id": r["ticket_id"], "station_id": r["station_id"], "type": r["type"], "status": r["status"],
            "alert_id": r.get("alert_id"), "started_at": fmt(r.get("started_at")),
            "created_at": fmt(r.get("created_at")), "updated_at": fmt(r.get("updated_at"))}


def _ticket_db_call(fn, *, write=False):
    """在 ads_ticket 上执行 fn(cur)；写失败返回 503，不回退内存。"""
    if not _db_on("ALERT_DB"):
        return False, None
    con = _alert_db()
    if not con:
        if write:
            raise MySQLWriteUnavailable()
        return False, None
    try:
        with con, con.cursor() as cur:
            r = fn(cur)
            con.commit()
            return True, r
    except MySQLUnavailable:
        raise
    except Exception as e:
        _mysql_failed(e)
        if write:
            raise MySQLWriteUnavailable() from e
        return False, None


def demo_auth_enabled() -> bool:
    return os.getenv("AUTH_DEMO_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}


def _require_admin():
    """演示令牌仅在 AUTH_DEMO_ENABLED=1 时生效，不可用于生产认证。"""
    h = request.headers.get("Authorization", "")
    token = h[7:].strip() if h.lower().startswith("bearer ") else ""
    role = {"demo-admin": "admin", "demo-readonly": "readonly"}.get(token)
    if role is None:
        return err("未登录或登录已失效，请先登录", 401)
    if role == "admin" and not demo_auth_enabled():
        return err("演示管理员认证未启用；请勿将演示认证用于公网部署", 503)
    if role != "admin":
        return err("访客为只读权限，无法修改，请使用管理员登录", 403)
    return None


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
    """API_SOURCE=mysql（或 MAP_DB=mysql）时优先读库：dim_station（站点坐标）+ ads_zone_daily（片区日均、中心点，取最新一天）。连不上就返回 None。"""
    if not _db_on("MAP_DB"):
        return None
    try:
        import pymysql
        conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4", init_command="SET time_zone='+08:00'", connect_timeout=3,
                               cursorclass=pymysql.cursors.DictCursor)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT station_id, taz_id, longitude, latitude, total_piles FROM dim_station")
            si = cur.fetchall()
            cur.execute("SELECT taz_id, avg_occupancy, station_cnt, pile_cnt, center_lon, center_lat FROM ads_zone_daily "
                        "WHERE dt = (SELECT MAX(dt) FROM ads_zone_daily)")
            zd = {str(r["taz_id"]): r for r in cur.fetchall()}
    except Exception as e:
        _mysql_failed(e)
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


ALERT_LEVEL = {"stuck": "high", "high_missing": "medium", "long_gap": "low"}
ALERT_STATES = {"open", "ack", "closed"}
_ALERTS: list[dict] | None = None

# MySQL 表 dq_alert（小白在 quality/sql/ 下建表）。API_SOURCE=mysql 或 ALERT_DB=mysql 时启用，否则用内存。
# 表结构见 quality/sql/dq_alert.sql（主键 alert_id，状态 open/ack/closed）。


def _alert_db():
    if not _db_on("ALERT_DB"):
        return None
    try:
        import pymysql
        return pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                               user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                               database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4", init_command="SET time_zone='+08:00'",
                               cursorclass=pymysql.cursors.DictCursor, connect_timeout=3)
    except Exception as e:
        _mysql_failed(e)
        return None  # 连不上就退回内存（已记录 fallback；严格模式 503）


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
        except Exception as e:
            _mysql_failed(e)
            pass
    return alerts_store(), "memory"


def alerts_write(alert_id: int, status: str, note: str | None):
    if _db_on("ALERT_DB"):
        con = _alert_db()
        if not con:
            raise MySQLWriteUnavailable()
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
                    row = cur.fetchone()
                    return _norm_alert(row) if row else None
                return None
        except MySQLUnavailable:
            raise
        except Exception as e:
            _mysql_failed(e)
            raise MySQLWriteUnavailable() from e
    for a in alerts_store():
        if a["alert_id"] == alert_id:
            a["status"] = status
            if note is not None:
                a["note"] = str(note)[:500]
            a["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            return a
    return None


# 调度运行 / 监控接口（只读）：见 api/ops.py
import ops as _ops  # noqa: E402
_ops.register(app, lambda q, a=(): _sql(q, a), ok, err)  # lambda：测试可 monkeypatch app._sql
import lineage as _lineage  # noqa: E402
_lineage.register(app, ok, err)



# ---- API route modules ----
# Keep the original endpoint names/URL rules while moving HTTP handlers
# out of the application bootstrap module.
from routes.read import register as _register_read_routes
from routes.tickets import register as _register_ticket_routes
from routes.ops import register as _register_ops_routes

_register_read_routes(app)
_register_ticket_routes(app)
_register_ops_routes(app)

# ---- 可选 Redis 读缓存：设置 REDIS_URL 才启用；没设或 Redis 挂了，行为与原来完全一样 ----
# 只缓存 GET 的只读接口、只缓存 200 且没有退回文件的响应；key 含路径 + 排序后的查询参数 + 数据源模式。
# 写操作（告警确认 / 工单）成功后清掉相关前缀。响应头 X-Cache: HIT / MISS。
CACHE_TTL = {  # 秒；可用 CACHE_TTL_DEFAULT 整体覆盖历史类接口
    "/api/realtime/zones": 5,
    "/api/alerts": 10,
    "/api/tickets": 10,
}
CACHE_HISTORY = {"/api/overview", "/api/zones", "/api/zone/series", "/api/quality", "/api/map/zones", "/api/map/hourly",
                 "/api/site/suggest", "/api/forecast/metrics", "/api/baseline", "/api/data_range", "/api/stations"}
CACHE_INVALIDATE = {"/api/alerts": ("/api/alerts", "/api/tickets", "/api/overview"), "/api/tickets": ("/api/tickets", "/api/alerts")}
_redis_client = None
_redis_down_until = 0.0


def _cache_ttl(path: str) -> int | None:
    if path in CACHE_TTL:
        return CACHE_TTL[path]
    if path in CACHE_HISTORY:
        return int(os.getenv("CACHE_TTL_DEFAULT", "60"))
    return None


def _redis():
    """REDIS_URL 未设返回 None；连接失败后 30 秒内不再尝试，避免每个请求都卡在超时上。"""
    global _redis_client, _redis_down_until
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        return None
    import time as _t
    if _t.time() < _redis_down_until:
        return None
    if _redis_client is None:
        try:
            import redis
            _redis_client = redis.Redis.from_url(url, socket_timeout=0.2, socket_connect_timeout=0.2)
        except Exception as e:  # 没装 redis 包等
            app.logger.warning("Redis 缓存不可用：%s", e)
            _redis_down_until = _t.time() + 30
            return None
    return _redis_client


def _redis_failed(e):
    global _redis_down_until
    import time as _t
    _redis_down_until = _t.time() + 30
    app.logger.warning("Redis 出错，30 秒内不走缓存：%s", e)


def _cache_key(path: str) -> str:
    args = urlencode([(k, v) for k, values in sorted(request.args.lists()) for v in values])
    return f"szev:{'mysql' if use_mysql() else 'file'}:{path}?{args}"


@app.before_request
def _cache_lookup():
    if request.method != "GET" or _cache_ttl(request.path) is None:
        return None
    r = _redis()
    if r is None:
        return None
    key = _cache_key(request.path)
    g.cache_key = key
    try:
        hit = r.get(key)
    except Exception as e:
        _redis_failed(e)
        g.cache_key = None
        return None
    if hit is None:
        return None
    g.cache_hit = True
    resp = app.response_class(hit, status=200, mimetype="application/json")
    resp.headers["X-Cache"] = "HIT"
    return resp


@app.after_request
def _cache_store(resp):
    key = getattr(g, "cache_key", None)
    if key and not getattr(g, "cache_hit", False):
        resp.headers["X-Cache"] = "MISS"
        if resp.status_code == 200 and not getattr(g, "mysql_error", None) and resp.mimetype == "application/json":
            try:
                r = _redis()
                if r is not None:
                    r.setex(key, _cache_ttl(request.path), resp.get_data())
            except Exception as e:
                _redis_failed(e)
    elif request.method in ("POST", "PATCH", "PUT", "DELETE") and resp.status_code < 400:
        base = "/" + "/".join(request.path.strip("/").split("/")[:2])
        prefixes = CACHE_INVALIDATE.get(base)
        r = _redis() if prefixes else None
        if r is not None:
            try:
                for pre in prefixes:
                    keys = list(r.scan_iter(match=f"szev:*:{pre}*", count=500))
                    if keys:
                        r.delete(*keys)
            except Exception as e:
                _redis_failed(e)
    return resp

# 必须放在文件末尾：python app.py 时 app.run() 会阻塞，写在它后面的路由（地图、告警）不会注册。
if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    app.run(host="0.0.0.0", port=5000, debug=debug)
