"""read HTTP routes.

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
from app import DWD, strict_mysql, use_file, use_mysql\nfrom utils.http import err, ok

def health():
    """mode=配置的数据源；source=实际能用的数据源；fallback=配置了 mysql 但连不上、正在退回文件。"""
    mode = "mysql" if use_mysql() else ("file" if use_file() else "mock")
    data = {"service": "sz-ev-api", "mode": mode, "dwd": str(DWD), "source": mode if mode != "mysql" else "mysql",
            "fallback": False, "mysql_error": None, "strict_mysql": strict_mysql()}
    if mode == "mysql":
        try:
            conn = appmod._mysql_connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            finally:
                conn.close()
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            g.mysql_error = msg
            appmod.app.logger.warning("MySQL 健康检查失败：%s", msg)
            data.update(mysql_error=msg)
            if strict_mysql():
                return jsonify({"ok": False, "data": data, "error": "MySQL 不可用（API_STRICT_MYSQL=1）"}), 503
            data.update(source="file" if use_file() else "mock", fallback=True)
    return ok(data)



def data_range():
    """页面顶部「数据时间」：库里/文件里实际有数据的首尾日期（只读）。"""
    if use_mysql():
        r = appmod._sql("SELECT MIN(dt) AS d0, MAX(dt) AS d1 FROM ads_city_overview")
        if r and r[0].get("d0"):
            return ok({"from": str(r[0]["d0"])[:10], "to": str(r[0]["d1"])[:10]})
    if use_file():
        try:
            ts = [r["ts_hour"][:10] for r in appmod._zone_rows() if r.get("ts_hour")]
            if ts:
                return ok({"from": min(ts), "to": max(ts)})
        except OSError:
            pass
    return ok({"from": "2022-09-01", "to": "2022-09-14"})



def overview():
    dt = request.args.get("dt") or "2022-09-01"
    if use_mysql() and (r := appmod._mysql_overview(dt)) is not None:
        return ok(r)
    if use_file():
        q = appmod._quality_rows()
        z = appmod._zone_rows()
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



def site_suggest():
    """选址/加桩建议：ads_zone_site_suggest 最新一批，按 rank 升序取前 top 个（默认 20，最多 331）。
    只在 MySQL 模式下有数据；文件模式或表不存在/为空时返回空列表。"""
    try:
        top = max(1, min(int(request.args.get("top", 20)), 331))
    except ValueError:
        return err("top 必须是整数", 400)
    if not use_mysql():
        return ok({"source": "none", "run_dt": None, "items": []})
    r = appmod._sql("SELECT run_dt, taz_id, center_lon, center_lat, station_cnt, piles, rated_kw, avg_occupancy, peak_occupancy,"
             " busy_hours_ratio, demand_score, supply_score, gap_score, suggest_add_piles, `rank`, function_tag, reason"
             " FROM ads_zone_site_suggest WHERE run_dt=(SELECT MAX(run_dt) FROM ads_zone_site_suggest)"
             " ORDER BY `rank` IS NULL, `rank`, gap_score DESC LIMIT %s", (top,))
    if not r:
        return ok({"source": "none" if r is None else "mysql", "run_dt": None, "items": []})
    nums = {"center_lon": 6, "center_lat": 6, "rated_kw": 2, "avg_occupancy": 4, "peak_occupancy": 4,
            "busy_hours_ratio": 4, "demand_score": 4, "supply_score": 4, "gap_score": 4}
    items = [{k: (str(v) if k == "taz_id" else appmod._num(v, nums[k]) if k in nums else v)
              for k, v in x.items() if k != "run_dt"} for x in r]
    return ok({"source": "mysql", "run_dt": str(r[0]["run_dt"])[:10], "items": items})



def realtime_zones():
    """准实时：每个片区最新一个 15 分钟窗口（rt_zone_occupancy，由 stream/zone_occupancy_stream.py 写入）。
    只在 MySQL 模式下有数据；文件模式或表为空时返回空列表。"""
    if not use_mysql():
        return ok({"source": "none", "zones": []})
    r = appmod._sql("SELECT t.taz_id, t.window_start, t.window_end, t.occupancy, t.busy_sum, t.piles_sum, t.station_cnt,"
             " t.outlier_cnt, t.dropped_src_cnt, t.write_ts, t.latency_ms FROM rt_zone_occupancy t JOIN (SELECT taz_id, MAX(window_end) AS we"
             " FROM rt_zone_occupancy GROUP BY taz_id) m ON t.taz_id=m.taz_id AND t.window_end=m.we")
    if r is None:
        return ok({"source": "none", "zones": []})
    zones = sorted(({"taz_id": str(x["taz_id"]), "window_start": appmod._ts(x["window_start"]), "window_end": appmod._ts(x["window_end"]),
                     "occupancy": appmod._num(x["occupancy"], 4), "busy_sum": x["busy_sum"], "piles_sum": x["piles_sum"],
                     "station_cnt": x["station_cnt"],
                     "outlier_cnt": x.get("outlier_cnt"), "dropped_src_cnt": x.get("dropped_src_cnt"), "write_ts": appmod._ts(x["write_ts"])[:19], "latency_ms": x["latency_ms"]}
                    for x in r), key=lambda z: int(z["taz_id"]) if z["taz_id"].isdigit() else 0)
    return ok({"source": "mysql", "zones": zones})



def zones_list():
    if use_mysql() and (r := appmod._mysql_zones()) is not None:
        return ok(r)
    if use_file():
        ids = sorted({r["taz_id"] for r in appmod._zone_rows()}, key=lambda x: int(x) if str(x).isdigit() else 0)
    else:
        ids = ["1011", "1062", "335"]
    return ok(ids)



def zone_series():
    taz_id = request.args.get("taz_id")
    if use_mysql() and (r := appmod._mysql_series(taz_id)) is not None:
        return ok(r)
    if use_file():
        rows = appmod._zone_rows()
        if not taz_id:
            taz_id = rows[0]["taz_id"] if rows else "1011"
        points = []
        for r in rows:
            if r["taz_id"] != taz_id or not appmod._in_range(r["ts_hour"]):
                continue
            busy = float(r.get("busy_sum") or 0)
            piles = float(r.get("piles_sum") or 0) or 1
            occ = float(r.get("occupancy_avg") or 0)
            # 数据里没有额定功率，负荷不估算，留空（前端显示「暂无」）
            points.append(
                {
                    "taz_id": taz_id,
                    "ts_hour": r["ts_hour"],
                    "occupancy": round(occ, 6),
                    "load_kw_est": None,
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
            "load_kw_est": None,
        }
        for i in range(48)
    ]
    return ok({"taz_id": taz_id, "points": series})



def quality():
    if use_mysql() and (r := appmod._mysql_quality()) is not None:
        return ok(r)
    if use_file():
        q = appmod._quality_rows()
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



def forecast_metrics():
    """每日预测误差序列（forecast/evaluate.py 写入 ads_forecast_metric_daily）。非 MySQL 模式或无数据时 points 为空。"""
    pts, src = [], "none"
    if use_mysql():
        r = appmod._sql("SELECT ds, n, mae_lgbm, rmse_lgbm, mae_lag1, rmse_lag1, mae_lag24, model_version, drift "
                 "FROM ads_forecast_metric_daily ORDER BY ds")
        if r is not None:
            src = "mysql"
            pts = [{"ds": str(x["ds"])[:10], "n": x["n"], "mae_lgbm": appmod._num(x["mae_lgbm"], 6),
                    "rmse_lgbm": appmod._num(x["rmse_lgbm"], 6), "mae_lag1": appmod._num(x["mae_lag1"], 6),
                    "rmse_lag1": appmod._num(x["rmse_lag1"], 6), "mae_lag24": appmod._num(x.get("mae_lag24"), 6),
                    "model_version": x.get("model_version"), "drift": bool(x.get("drift"))} for x in r]
    return ok({"source": src, "points": pts})



def baseline():
    taz_id = request.args.get("taz_id")
    if use_mysql() and (r := appmod._mysql_baseline(taz_id)) is not None:
        db_preds, db_win = appmod._mysql_forecast_preds(r["taz_id"])
        preds = db_preds or appmod._forecast_preds()
        return ok({**r, "points": appmod._with_lgbm(r["points"], preds),
                   "metrics": appmod._forecast_metrics(), "forecast_window": db_win or appmod._forecast_window()})
    if use_file():
        rows = appmod._zone_rows()
        if not taz_id:
            taz_id = rows[0]["taz_id"] if rows else "1011"
        series = [r for r in rows if r["taz_id"] == taz_id and appmod._in_range(r["ts_hour"])]
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
        preds = appmod._forecast_preds()
        if not points and preds:
            points = appmod._pred_series(taz_id, preds)
        return ok({"taz_id": taz_id, "points": appmod._with_lgbm(points, preds),
                   "metrics": appmod._forecast_metrics(), "forecast_window": appmod._forecast_window()})
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
    return ok({"taz_id": taz_id, "points": appmod._with_lgbm(points, {}),
               "metrics": appmod._forecast_metrics(), "forecast_window": appmod._forecast_window()})




def register(app):
    app.add_url_rule("/api/health", endpoint="health", view_func=health, methods=["GET"])
    app.add_url_rule("/api/data_range", endpoint="data_range", view_func=data_range, methods=["GET"])
    app.add_url_rule("/api/overview", endpoint="overview", view_func=overview, methods=["GET"])
    app.add_url_rule("/api/site/suggest", endpoint="site_suggest", view_func=site_suggest, methods=["GET"])
    app.add_url_rule("/api/realtime/zones", endpoint="realtime_zones", view_func=realtime_zones, methods=["GET"])
    app.add_url_rule("/api/zones", endpoint="zones_list", view_func=zones_list, methods=["GET"])
    app.add_url_rule("/api/zone/series", endpoint="zone_series", view_func=zone_series, methods=["GET"])
    app.add_url_rule("/api/quality", endpoint="quality", view_func=quality, methods=["GET"])
    app.add_url_rule("/api/forecast/metrics", endpoint="forecast_metrics", view_func=forecast_metrics, methods=["GET"])
    app.add_url_rule("/api/baseline", endpoint="baseline", view_func=baseline, methods=["GET"])
