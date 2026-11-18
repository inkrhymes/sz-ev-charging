"""ops HTTP routes.

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
from app import ALERT_STATES, REPO, alerts_read, alerts_write, err, ok, use_file, use_mysql

def stations():
    """站点明细：?status=stuck|high_missing|normal &taz_id= &sort=avg_occupancy|missing_rate|station_id &order=asc|desc &page=1 &page_size=10"""
    if not use_file():
        return ok({"total": 0, "page": 1, "page_size": 10, "items": []})
    rows = list(appmod._station_detail_rows())
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





def map_zones():
    """片区地图：每个片区的中心经纬度、站数、平均占用率，以及站点坐标。"""
    src = "mysql"
    lake = appmod._mysql_map()
    if not lake:
        src, lake = "lake", appmod._lake_map()
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
    for s in appmod._station_detail_rows():
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



def alerts_list():
    rows, _src = alerts_read()
    st = request.args.get("status")
    return ok([a for a in rows if not st or a["status"] == st])



def alerts_patch(alert_id):
    denied = appmod._require_admin()
    if denied:
        return denied
    body, problem = appmod._json_body()
    if problem:
        return problem
    status = body.get("status")
    if status == "processing":
        status = "ack"  # 兼容旧前端
    if not isinstance(status, str) or status not in ALERT_STATES:
        return err("status must be open|ack|closed")
    note = body.get("note")
    if note is not None and not isinstance(note, str):
        return err("note 必须是字符串")
    if note is not None and len(note) > 500:
        return err("note 不能超过 500 个字符")
    a = alerts_write(alert_id, status, note)
    return ok(a) if a else err("alert not found", 404)



def map_hourly():
    """地图时间轴：某一天每个片区 0–23 点的占用率（只读）。返回 {date, zones: {taz_id: [24 个值或 null]}}。"""
    day = (request.args.get("date") or "2022-09-01")[:10]
    try:
        d0 = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return err("date must be YYYY-MM-DD")
    out = defaultdict(lambda: [None] * 24)
    rows = None
    if use_mysql():
        rows = appmod._sql("SELECT taz_id, ts_hour, occupancy FROM ads_zone_occupancy_hour WHERE ts_hour >= %s AND ts_hour < %s",
                    (d0.strftime("%Y-%m-%d 00:00:00"), (d0 + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")))
        for r in rows or []:
            out[str(r["taz_id"])][int(appmod._ts(r["ts_hour"])[11:13])] = appmod._num(r["occupancy"], 4)
    if not rows and use_file():
        try:
            for r in appmod._zone_rows():
                if r["ts_hour"][:10] == day:
                    v = r.get("occupancy_avg")
                    out[str(r["taz_id"])][int(r["ts_hour"][11:13])] = round(float(v), 4) if v not in (None, "") else None
        except OSError:
            pass
    return ok({"date": day, "zones": dict(out)})




def register(app):
    app.add_url_rule("/api/stations", endpoint="stations", view_func=stations, methods=["GET"])
    app.add_url_rule("/api/map/zones", endpoint="map_zones", view_func=map_zones, methods=["GET"])
    app.add_url_rule("/api/alerts", endpoint="alerts_list", view_func=alerts_list, methods=["GET"])
    app.add_url_rule("/api/alerts/<int:alert_id>", endpoint="alerts_patch", view_func=alerts_patch, methods=["PATCH"])
    app.add_url_rule("/api/map/hourly", endpoint="map_hourly", view_func=map_hourly, methods=["GET"])
