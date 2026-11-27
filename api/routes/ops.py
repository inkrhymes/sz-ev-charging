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
from app import ALERT_STATES, AUDIT_ACTIONS, AUDIT_RESULTS, REPO, alerts_read, alerts_write, audit_read, audit_write, use_file, use_mysql
from utils.http import err, ok

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
    if a:
        st_cn = {"open": "待处理", "ack": "处理中", "closed": "已关闭"}.get(status, status)
        extra = f"，备注：{note}" if note else ""
        audit_write("alert_update", f"告警 AL-{int(alert_id):04d} 状态变更为「{st_cn}」{extra}",
                    target_type="alert", target_id=f"AL-{int(alert_id):04d}")
    return ok(a) if a else err("alert not found", 404)


def audit_list():
    """系统操作审计日志查询（支持按操作类型、结果、角色与关键词筛选分页）。"""
    action = (request.args.get("action") or "").strip() or None
    if action and action not in AUDIT_ACTIONS:
        return err("bad action")
    result = (request.args.get("result") or "").strip() or None
    if result and result not in AUDIT_RESULTS:
        return err("bad result")
    role = (request.args.get("role") or "").strip() or None
    if role and role not in {"admin", "readonly"}:
        return err("bad role")
    q = (request.args.get("q") or "").strip() or None
    try:
        page = max(1, int(request.args.get("page", 1)))
        page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    except ValueError:
        return err("bad page")
    return ok(audit_read(action=action, result=result, role=role, q=q, page=page, page_size=page_size))


def audit_export():
    """导出审计日志 CSV（UTF-8 BOM，便于 Excel 直接打开），并自动记录本次导出审计。"""
    import io

    action = (request.args.get("action") or "").strip() or None
    if action and action not in AUDIT_ACTIONS:
        return err("bad action")
    result = (request.args.get("result") or "").strip() or None
    if result and result not in AUDIT_RESULTS:
        return err("bad result")
    role = (request.args.get("role") or "").strip() or None
    if role and role not in {"admin", "readonly"}:
        return err("bad role")
    q = (request.args.get("q") or "").strip() or None

    data = audit_read(action=action, result=result, role=role, q=q, page=1, page_size=500)
    rows = data["items"]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["日志编号", "时间", "操作人", "角色", "操作类型", "对象类型", "关联对象", "操作详情", "结果", "来源IP"])
    for r in rows:
        w.writerow([
            f"LOG-{r['log_id']:04d}", r["created_at"], r["actor"],
            "管理员" if r["role"] == "admin" else "访客",
            r["action"], r["target_type"] or "", r["target_id"] or "",
            r["detail"], r["result"], r["ip"],
        ])
    audit_write("export", f"导出操作审计日志 CSV（共 {len(rows)} 条）", target_type="export", target_id="audit_log.csv")
    csv_bytes = ("\ufeff" + buf.getvalue()).encode("utf-8")
    resp = appmod.app.response_class(csv_bytes, status=200, mimetype="text/csv; charset=utf-8")
    resp.headers["Content-Disposition"] = 'attachment; filename="sz_ev_audit_logs.csv"'
    return resp


def _hour_span_label(hours: list[int]) -> str:
    if not hours:
        return "—"
    hs = sorted(set(int(h) % 24 for h in hours))
    # 找最长连续段，简洁展示如 18:00–22:00
    best_s, best_e = hs[0], hs[0]
    cur_s, cur_e = hs[0], hs[0]
    for h in hs[1:]:
        if h == cur_e + 1:
            cur_e = h
        else:
            if (cur_e - cur_s) > (best_e - best_s):
                best_s, best_e = cur_s, cur_e
            cur_s = cur_e = h
    if (cur_e - cur_s) > (best_e - best_s):
        best_s, best_e = cur_s, cur_e
    end_h = (best_e + 1) % 24
    return f"{best_s:02d}:00–{end_h:02d}:00"


def _best_valley_hours(hour_occ: dict[int, float], span: int = 4) -> list[int]:
    if not hour_occ:
        return [2, 3, 4, 5]
    best_hours = None
    best_val = None
    for start in range(0, 24 - span + 1):
        hs = list(range(start, start + span))
        vals = [hour_occ[h] for h in hs if h in hour_occ]
        if not vals:
            continue
        avg = sum(vals) / len(vals)
        if best_val is None or avg < best_val:
            best_val = avg
            best_hours = hs
    return best_hours or [2, 3, 4, 5]


def peak_advisory():
    """模块 5 辅线：电量错峰预警与高负荷提示（只输出错峰与巡检建议，不接入电网控网）。"""
    import re as _re

    taz_filter = (request.args.get("taz_id") or "").strip() or None
    date_from = (request.args.get("from") or "").strip() or None
    date_to = (request.args.get("to") or "").strip() or None
    for d in (date_from, date_to):
        if d and not _re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            return err("from/to 必须是 YYYY-MM-DD")
    try:
        occ_th = float(request.args.get("occ_threshold", 0.70))
        kw_per_pile = float(request.args.get("kw_per_pile", 7.0))
    except ValueError:
        return err("occ_threshold 和 kw_per_pile 必须是数字")
    if not (0.30 <= occ_th <= 0.95):
        return err("occ_threshold 须在 0.30 到 0.95 之间")
    if not (3.0 <= kw_per_pile <= 240.0):
        return err("kw_per_pile 须在 3 到 240 之间")

    # records: list of (taz_id, ts_hour, occ, load_kw)
    records: list[tuple[str, str, float, float]] = []
    source = "none"
    power_mode = "assumed"

    if use_mysql():
        cond, args = "", []
        if date_from:
            cond += " AND ts_hour >= %s"
            args.append(f"{date_from} 00:00:00")
        if date_to:
            cond += " AND ts_hour <= %s"
            args.append(f"{date_to} 23:59:59")
        rows = appmod._sql(
            f"SELECT taz_id, ts_hour, occupancy, load_kw_est FROM ads_zone_occupancy_hour WHERE 1=1{cond}",
            tuple(args),
        )
        if rows is not None:
            source = "mysql"
            pile_rows = appmod._sql("SELECT taz_id, SUM(total_piles) AS piles FROM dim_station GROUP BY taz_id") or []
            piles_map = {str(x["taz_id"]): float(x["piles"] or 0) for x in pile_rows}
            has_rated = False
            for r in rows:
                occ = appmod._num(r.get("occupancy"), 6)
                if occ is None:
                    continue
                taz = str(r["taz_id"])
                ts = appmod._ts(r["ts_hour"])
                lkw = appmod._num(r.get("load_kw_est"), 2)
                if lkw is not None:
                    has_rated = True
                else:
                    lkw = round(occ * piles_map.get(taz, 10.0) * kw_per_pile, 2)
                records.append((taz, ts, occ, lkw))
            power_mode = "rated" if has_rated else "assumed"

    if not records and use_file():
        source = "file"
        power_mode = "assumed"
        for r in appmod._zone_rows():
            ts = r.get("ts_hour") or ""
            d = ts[:10]
            if (date_from and d < date_from) or (date_to and d > date_to):
                continue
            try:
                occ = float(r.get("occupancy_avg"))
            except (TypeError, ValueError):
                continue
            busy_sum = float(r.get("busy_sum") or 0)
            avg_busy_piles = busy_sum / 12.0
            lkw = round(avg_busy_piles * kw_per_pile, 2)
            records.append((str(r["taz_id"]), ts, round(occ, 6), lkw))

    if not records:
        return ok({
            "source": source, "power_mode": power_mode,
            "summary": {
                "zone_cnt": 0, "warn_zone_cnt": 0, "high_risk_cnt": 0, "over_hours_total": 0,
                "peak_occupancy": None, "peak_load_kw": None, "peak_span": "—", "valley_span": "—",
                "occ_threshold": occ_th, "kw_per_pile": kw_per_pile, "selected_taz": taz_filter,
                "from": date_from, "to": date_to,
                "power_note": "仅输出错峰充电与巡检辅助建议，不接入电网控网",
            },
            "hourly_profile": [], "zones": [],
        })

    by_zone: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    for taz, ts, occ, lkw in records:
        by_zone[taz].append((ts, occ, lkw))

    zones_out = []
    over_hours_total = 0
    for taz, pts in by_zone.items():
        n = len(pts)
        occs = [p[1] for p in pts]
        loads = [p[2] for p in pts]
        over = sum(1 for v in occs if v >= occ_th)
        over_hours_total += over
        peak_occ = max(occs)
        avg_occ = sum(occs) / n
        peak_kw = max(loads)
        avg_kw = sum(loads) / n
        over_ratio = over / n

        h_bucket: dict[int, list[float]] = defaultdict(list)
        for ts, occ, _lkw in pts:
            try:
                h_bucket[int(ts[11:13])].append(occ)
            except (ValueError, IndexError):
                continue
        h_mean = {h: sum(vs) / len(vs) for h, vs in h_bucket.items() if vs}
        busy_hs = [h for h, v in h_mean.items() if v >= occ_th]
        if not busy_hs and h_mean:
            busy_hs = [h for h, _ in sorted(h_mean.items(), key=lambda kv: -kv[1])[:3]]
        peak_span = _hour_span_label(busy_hs)
        valley_hs = _best_valley_hours(h_mean, 4)
        valley_span = _hour_span_label(valley_hs)

        if peak_occ >= min(0.95, occ_th + 0.10) or over_ratio >= 0.25:
            risk = "high"
            advice = f"高峰集中在 {peak_span}（峰值占用 {peak_occ:.0%}，约 {peak_kw:.0f} kW），建议引导至 {valley_span} 错峰充电，巡检避开高峰"
        elif over > 0:
            risk = "medium"
            advice = f"{peak_span} 偏忙（超阈值 {over} 小时，峰值 {peak_occ:.0%}），建议日常巡检优先安排在 {valley_span}"
        else:
            risk = "normal"
            advice = f"负荷平稳（峰值占用 {peak_occ:.0%}，约 {peak_kw:.0f} kW），按常规计划巡检即可，低谷在 {valley_span}"

        zones_out.append({
            "taz_id": taz,
            "hours": n,
            "avg_occupancy": round(avg_occ, 4),
            "peak_occupancy": round(peak_occ, 4),
            "avg_load_kw": round(avg_kw, 2),
            "peak_load_kw": round(peak_kw, 2),
            "over_hours": over,
            "over_ratio": round(over_ratio, 4),
            "peak_span": peak_span,
            "valley_span": valley_span,
            "risk": risk,
            "advice": advice,
        })

    risk_rank = {"high": 0, "medium": 1, "normal": 2}
    zones_out.sort(key=lambda z: (risk_rank[z["risk"]], -z["over_hours"], -z["peak_occupancy"], -z["peak_load_kw"]))

    # 24 小时峰谷曲线：传了 taz_id 看单片区，未传看全市同期负荷汇总
    scope = [r for r in records if (not taz_filter or r[0] == taz_filter)]
    if not scope:
        scope = records
        taz_filter = None

    by_day_hour_load: dict[tuple[str, int], float] = defaultdict(float)
    by_hour_occ: dict[int, list[float]] = defaultdict(list)
    for _taz, ts, occ, lkw in scope:
        try:
            h = int(ts[11:13])
        except (ValueError, IndexError):
            continue
        by_day_hour_load[(ts[:10], h)] += lkw
        by_hour_occ[h].append(occ)

    by_hour_load: dict[int, list[float]] = defaultdict(list)
    for (_d, h), total_kw in by_day_hour_load.items():
        by_hour_load[h].append(total_kw)

    hour_mean_occ = {h: sum(vs) / len(vs) for h, vs in by_hour_occ.items() if vs}
    valley_set = set(_best_valley_hours(hour_mean_occ, 4))
    peak_hours_global = [h for h, v in hour_mean_occ.items() if v >= occ_th]
    if not peak_hours_global and hour_mean_occ:
        peak_hours_global = [h for h, _ in sorted(hour_mean_occ.items(), key=lambda kv: -kv[1])[:3]]

    hourly_profile = []
    for h in range(24):
        occs = by_hour_occ.get(h, [])
        loads = by_hour_load.get(h, [])
        avg_o = round(sum(occs) / len(occs), 4) if occs else None
        max_o = round(max(occs), 4) if occs else None
        avg_l = round(sum(loads) / len(loads), 2) if loads else None
        max_l = round(max(loads), 2) if loads else None
        hourly_profile.append({
            "hour": h,
            "label": f"{h:02d}:00",
            "avg_occupancy": avg_o,
            "peak_occupancy": max_o,
            "avg_load_kw": avg_l,
            "peak_load_kw": max_l,
            "is_peak": bool(avg_o is not None and avg_o >= occ_th),
            "is_valley": h in valley_set,
        })

    scope_occs = [p["peak_occupancy"] for p in hourly_profile if p["peak_occupancy"] is not None]
    scope_loads = [p["peak_load_kw"] for p in hourly_profile if p["peak_load_kw"] is not None]
    power_note = (
        "按实测额定功率汇总估算，仅输出错峰与巡检建议，不接入电网控网"
        if power_mode == "rated"
        else f"按在用桩数 × 参考单桩功率（{kw_per_pile:g} kW）估算近似负荷，仅输出错峰与巡检建议，不接入电网控网"
    )
    return ok({
        "source": source,
        "power_mode": power_mode,
        "summary": {
            "zone_cnt": len(zones_out),
            "warn_zone_cnt": sum(1 for z in zones_out if z["risk"] in ("high", "medium")),
            "high_risk_cnt": sum(1 for z in zones_out if z["risk"] == "high"),
            "over_hours_total": over_hours_total,
            "peak_occupancy": max(scope_occs) if scope_occs else None,
            "peak_load_kw": max(scope_loads) if scope_loads else None,
            "peak_span": _hour_span_label(peak_hours_global),
            "valley_span": _hour_span_label(sorted(valley_set)),
            "occ_threshold": occ_th,
            "kw_per_pile": kw_per_pile,
            "selected_taz": taz_filter,
            "from": date_from,
            "to": date_to,
            "power_note": power_note,
        },
        "hourly_profile": hourly_profile,
        "zones": zones_out,
    })



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
    app.add_url_rule("/api/ops/audit", endpoint="audit_list", view_func=audit_list, methods=["GET"])
    app.add_url_rule("/api/ops/audit/export", endpoint="audit_export", view_func=audit_export, methods=["GET"])
    app.add_url_rule("/api/ops/peak_advisory", endpoint="peak_advisory", view_func=peak_advisory, methods=["GET"])
    app.add_url_rule("/api/map/hourly", endpoint="map_hourly", view_func=map_hourly, methods=["GET"])
