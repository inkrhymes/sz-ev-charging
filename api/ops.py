"""调度运行与监控接口（只读，读 MySQL ops_task_run / dq_run_log / dq_alert）。

  GET /api/ops/runs?from=YYYY-MM-DD&to=YYYY-MM-DD   每天一行的运行概况
  GET /api/ops/runs/<ds>                            某天每个任务的最近一次结果 + 全部尝试
  GET /api/runs、/api/runs/<ds>                     同上（别名）
  GET /api/ops/health_metrics?days=30               各层新鲜度、最近 7 次运行耗时、各层质检通过率、未关闭告警数
app.py 末尾调用 register(app, _sql, ok, err) 挂载。
"""
from __future__ import annotations

import datetime as dt

import re
from datetime import date, datetime

CHAIN = ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"]
EXTRA = ["forecast_daily", "forecast_evaluate", "weather_join", "site_suggest", "cache_flush"]  # publish 之后的扩展任务，没跑过就不出现
TASK_LABEL = {"ods_ingest": "原始数据入湖", "dq_ods": "原始层质检", "dwd_build": "清洗", "dq_dwd": "清洗层质检",
              "ads_build": "汇总计算", "dq_ads": "汇总层质检", "publish": "写入数据库",
              "forecast_daily": "次日预测", "forecast_evaluate": "预测评估", "weather_join": "关联天气", "site_suggest": "选址补桩建议", "cache_flush": "清接口缓存"}
SOURCE_LABEL = {"airflow": "Airflow", "run_day": "本地执行器", "backfill_airflow": "Airflow（补录）",
                "backfill_dq": "质检日志（补录）", "backfill_range": "全量入湖（补录，耗时为按天平摊的估算）"}
_DS = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COLS = ("id, dag_id, DATE_FORMAT(ds,'%%Y-%%m-%%d') AS ds, task_id, state, start_ts, end_ts, duration_s,"
        " block_reason, source")


def _ts(v):
    return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, (datetime, date)) else v


def _row(r: dict) -> dict:
    """一个步骤（steps[] 的元素）。"""
    return {"name": r["task_id"], "label": TASK_LABEL.get(r["task_id"], r["task_id"]), "status": r["state"],
            "started_at": _ts(r["start_ts"]), "ended_at": _ts(r["end_ts"]),
            "duration_s": None if r["duration_s"] is None else round(float(r["duration_s"]), 3),
            "error": r["block_reason"], "source": r["source"],
            "source_label": SOURCE_LABEL.get(r["source"], r["source"])}


def _steps(rows: list[dict]) -> list[dict]:
    """按链路顺序列出每个步骤的最近一次结果；主链里没记录的步骤给 not_recorded。"""
    last = latest_per_task(rows)
    out = []
    for t in CHAIN + EXTRA + sorted(set(last) - set(CHAIN) - set(EXTRA)):
        if t in last:
            out.append(_row(last[t]))
        elif t in CHAIN:
            out.append({"name": t, "label": TASK_LABEL[t], "status": "not_recorded", "started_at": None,
                        "ended_at": None, "duration_s": None, "error": None, "source": None, "source_label": None})
    return out


def latest_per_task(rows: list[dict]) -> dict[str, dict]:
    out = {}
    for r in sorted(rows, key=lambda x: (x["start_ts"], x["id"])):
        out[r["task_id"]] = r
    return out


def day_summary(ds: str, rows: list[dict]) -> dict:
    """某天的概况：按每个任务最近一次结果判定。
    status: blocked（质检拦下）> failed > running > success；complete 表示 7 个主链任务都有记录。"""
    last = latest_per_task(rows)
    states = {t: r["state"] for t, r in last.items()}
    blocked = [t for t in CHAIN + EXTRA if states.get(t) == "blocked"]
    failed = [t for t in CHAIN + EXTRA if states.get(t) == "failed"]
    if blocked:
        status, at = "blocked", blocked[0]
    elif failed:
        status, at = "failed", failed[0]
    elif "running" in states.values():
        status, at = "running", None
    else:
        status, at = "success", None
    durs = [float(r["duration_s"]) for r in last.values() if r["duration_s"] is not None]
    return {
        "ds": ds, "status": status,
        "started_at": _ts(min(r["start_ts"] for r in last.values())),
        "ended_at": _ts(max((r["end_ts"] or r["start_ts"]) for r in last.values())),
        "duration_s": round(sum(durs), 3),
        "error": last[at]["block_reason"] if at else None,
        "steps": _steps(rows),
        "blocked_at": at, "blocked_at_label": TASK_LABEL.get(at) if at else None,
        "complete": all(t in last for t in CHAIN),
        "tasks_total": len(last), "tasks_success": sum(s == "success" for s in states.values()),
        "attempts": len(rows),
    }


# ---------- 运行健康指标 /api/ops/health_metrics ----------
LAYERS = ["ods", "dwd", "ads"]
BUILD_TASK = {"ods": "ods_ingest", "dwd": "dwd_build", "ads": "ads_build", "publish": "publish",
              "forecast": "forecast_daily"}
DQ_SQL = ("SELECT ds, layer, run_id, MAX(created_at) AS checked_at, COUNT(*) AS n, SUM(passed) AS n_pass, "
          "SUM(severity='block' AND passed=0) AS block_fail FROM dq_run_log GROUP BY ds, layer, run_id")
# 全量区间补录（backfill_range）和纯质检补录（backfill_dq）是按天摊开的整段记录，不算单日运行，健康指标里不用
PER_DAY_RUN = "source NOT IN ('backfill_range','backfill_dq')"
BUILD_SQL = ("SELECT task_id, MAX(ds) AS ds, MAX(end_ts) AS built_at FROM ops_task_run "
             f"WHERE state='success' AND {PER_DAY_RUN} GROUP BY task_id")
ALERT_SQL = ("SELECT layer, severity, status, COUNT(*) AS n FROM dq_alert "
             "WHERE status IN ('open','ack') GROUP BY layer, severity, status")


def _latest_dq(dq_rows):
    """每个 (ds, layer) 只取最近一次质检。"""
    last = {}
    for r in dq_rows:
        k = (str(r["ds"]), r["layer"])
        if k not in last or str(r["checked_at"]) > str(last[k]["checked_at"]):
            last[k] = r
    return last


def health_from(dq_rows, build_rows, alert_rows, run_rows, published, now, days=30):
    last = _latest_dq(dq_rows or [])
    built = {r["task_id"]: r for r in (build_rows or [])}

    def age_h(v):
        if v is None:
            return None
        v = v if isinstance(v, dt.datetime) else dt.datetime.fromisoformat(str(v))
        return round((now - v).total_seconds() / 3600, 1)

    freshness = {}
    for layer in LAYERS:
        ks = sorted(ds for ds, l in last if l == layer)
        ok_ds = [ds for ds in ks if not int(last[(ds, layer)]["block_fail"] or 0)]
        b = built.get(BUILD_TASK[layer]) or {}
        freshness[layer] = {"latest_ds": ks[-1] if ks else None, "latest_passed_ds": ok_ds[-1] if ok_ds else None,
                            "last_checked_at": _ts(max((last[(d, layer)]["checked_at"] for d in ks), default=None)),
                            "last_built_at": _ts(b.get("built_at")), "built_age_hours": age_h(b.get("built_at"))}
    for name in ("publish", "forecast"):
        b = built.get(BUILD_TASK[name]) or {}
        p = (published or {}).get(name) or {}
        freshness[name] = {"latest_ds": str(b["ds"]) if b.get("ds") else None,
                           "data_until": _ts(p.get("data_until")), "last_built_at": _ts(b.get("built_at") or p.get("written_at")),
                           "built_age_hours": age_h(b.get("built_at") or p.get("written_at"))}

    dq = {}
    for layer in LAYERS:
        ks = sorted(ds for ds, l in last if l == layer)[-days:]
        rs = [last[(d, layer)] for d in ks]
        n = sum(int(r["n"]) for r in rs)
        npass = sum(int(r["n_pass"] or 0) for r in rs)
        days_ok = sum(1 for r in rs if not int(r["block_fail"] or 0))
        dq[layer] = {"days": len(rs), "days_passed": days_ok,
                     "day_pass_rate": round(days_ok / len(rs), 4) if rs else None,
                     "rules": n, "rules_passed": npass, "rule_pass_rate": round(npass / n, 4) if n else None,
                     "blocked_days": [d for d, r in zip(ks, rs) if int(r["block_fail"] or 0)]}

    by_day = {}
    for r in run_rows or []:
        by_day.setdefault(str(r["ds"]), []).append(r)
    recent = sorted((day_summary(d, rs) for d, rs in by_day.items()),
                    key=lambda x: x["started_at"] or "", reverse=True)[:7]
    last_runs = [{"ds": x["ds"], "status": x["status"], "started_at": x["started_at"], "ended_at": x["ended_at"],
                  "duration_s": x["duration_s"],
                  "steps": {st["name"]: st["duration_s"] for st in x["steps"] if st["status"] not in ("not_recorded", "skipped")}}
                 for x in recent]
    durs = [x["duration_s"] for x in last_runs if x["duration_s"] is not None]

    alerts = {"open": 0, "ack": 0, "by_layer": {}, "by_severity": {}}
    for r in alert_rows or []:
        n = int(r["n"])
        alerts[r["status"]] = alerts.get(r["status"], 0) + n
        if r["status"] == "open":
            alerts["by_layer"][r["layer"]] = alerts["by_layer"].get(r["layer"], 0) + n
            alerts["by_severity"][r["severity"]] = alerts["by_severity"].get(r["severity"], 0) + n

    return {"generated_at": _ts(now), "freshness": freshness,
            "last_runs": last_runs,
            "last_runs_duration_s": {"avg": round(sum(durs) / len(durs), 3) if durs else None,
                                     "max": max(durs) if durs else None, "min": min(durs) if durs else None},
            "dq_pass_rate": dq, "dq_window_days": days, "open_alerts": alerts}


def register(app, sql, ok, err):
    def _need(rows):
        return rows is None

    @app.get("/api/ops/runs")
    @app.get("/api/runs", endpoint="runs_alias")  # 别名，前端「调度运行」页直接读
    def ops_runs():
        f, t = request_args("from"), request_args("to")
        for v in (f, t):
            if v and not _DS.match(v):
                return err("from/to 必须是 YYYY-MM-DD")
        cond, args = "", []
        if f:
            cond += " AND ds >= %s"; args.append(f)
        if t:
            cond += " AND ds <= %s"; args.append(t)
        rows = sql(f"SELECT {COLS} FROM ops_task_run WHERE 1=1{cond} ORDER BY ds, start_ts, id", tuple(args))
        if _need(rows):
            return err("运行记录需要 MySQL（ops_task_run），当前读不到", 503)
        by_day: dict[str, list] = {}
        for r in rows:
            by_day.setdefault(r["ds"], []).append(r)
        days = [day_summary(ds, rs) for ds, rs in sorted(by_day.items())]
        cnt = {}
        for d in days:
            cnt[d["status"]] = cnt.get(d["status"], 0) + 1
        return ok({"from": f, "to": t, "runs": days, "counts": cnt, "chain": CHAIN, "labels": TASK_LABEL})

    @app.get("/api/ops/runs/<ds>")
    @app.get("/api/runs/<ds>", endpoint="run_day_alias")
    def ops_run_day(ds):
        if not _DS.match(ds):
            return err("ds 必须是 YYYY-MM-DD")
        rows = sql(f"SELECT {COLS} FROM ops_task_run WHERE ds=%s ORDER BY start_ts, id", (ds,))
        if _need(rows):
            return err("运行记录需要 MySQL（ops_task_run），当前读不到", 503)
        if not rows:
            return err(f"{ds} 没有运行记录", 404)
        return ok({**day_summary(ds, rows), "history": [_row(r) for r in rows]})

    @app.get("/api/ops/health_metrics")
    def ops_health_metrics():
        try:
            days = max(1, min(int(request_args("days") or 30), 400))
        except ValueError:
            return err("days 必须是整数")
        dq_rows, build_rows = sql(DQ_SQL), sql(BUILD_SQL)
        if dq_rows is None and build_rows is None:
            return err("健康指标需要 MySQL（dq_run_log / ops_task_run），当前读不到", 503)
        alert_rows = sql(ALERT_SQL)
        run_rows = sql(f"SELECT {COLS} FROM ops_task_run WHERE {PER_DAY_RUN} AND ds IN (SELECT ds FROM "
                       f"(SELECT ds FROM ops_task_run WHERE {PER_DAY_RUN} GROUP BY ds ORDER BY MAX(start_ts) DESC LIMIT 7) t) "
                       "ORDER BY ds, start_ts, id")
        pub = {}
        for name, q in (("publish", "SELECT MAX(ts_hour) AS data_until, NULL AS written_at FROM ads_zone_occupancy_hour"),
                        ("forecast", "SELECT MAX(ts_hour) AS data_until, MAX(created_at) AS written_at FROM ads_forecast_zone_hour")):
            r = sql(q)
            pub[name] = r[0] if r else {}
        return ok(health_from(dq_rows, build_rows, alert_rows, run_rows, pub, dt.datetime.now(), days))

    return app


def request_args(name):
    from flask import request
    return request.args.get(name) or None
