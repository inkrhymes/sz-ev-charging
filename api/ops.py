"""调度运行与监控接口（只读，读 MySQL ops_task_run / dq_run_log / dq_alert）。

  GET /api/ops/runs?from=YYYY-MM-DD&to=YYYY-MM-DD   每天一行的运行概况
  GET /api/ops/runs/<ds>                            某天每个任务的最近一次结果 + 全部尝试
app.py 末尾调用 register(app, _sql, ok, err) 挂载。
"""
from __future__ import annotations

import re
from datetime import date, datetime

CHAIN = ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"]
EXTRA = ["forecast_daily", "weather_join", "site_suggest"]  # publish 之后的扩展任务，没跑过就不出现
TASK_LABEL = {"ods_ingest": "原始数据入湖", "dq_ods": "原始层质检", "dwd_build": "清洗", "dq_dwd": "清洗层质检",
              "ads_build": "汇总计算", "dq_ads": "汇总层质检", "publish": "写入数据库",
              "forecast_daily": "次日预测", "weather_join": "关联天气", "site_suggest": "选址补桩建议"}
SOURCE_LABEL = {"airflow": "Airflow", "run_day": "本地执行器", "backfill_airflow": "Airflow（补录）",
                "backfill_dq": "质检日志（补录）", "backfill_range": "全量入湖（补录，耗时为按天平摊的估算）"}
_DS = re.compile(r"^\d{4}-\d{2}-\d{2}$")
COLS = ("id, dag_id, DATE_FORMAT(ds,'%%Y-%%m-%%d') AS ds, task_id, state, start_ts, end_ts, duration_s,"
        " block_reason, source")


def _ts(v):
    return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, (datetime, date)) else v


def _row(r: dict) -> dict:
    return {"task_id": r["task_id"], "label": TASK_LABEL.get(r["task_id"], r["task_id"]), "state": r["state"],
            "start_ts": _ts(r["start_ts"]), "end_ts": _ts(r["end_ts"]),
            "duration_s": None if r["duration_s"] is None else round(float(r["duration_s"]), 3),
            "block_reason": r["block_reason"], "source": r["source"],
            "source_label": SOURCE_LABEL.get(r["source"], r["source"]), "dag_id": r["dag_id"]}


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
        "blocked_at": at, "blocked_at_label": TASK_LABEL.get(at) if at else None,
        "block_reason": last[at]["block_reason"] if at else None,
        "complete": all(t in last for t in CHAIN),
        "tasks_total": len(last), "tasks_success": sum(s == "success" for s in states.values()),
        "duration_s": round(sum(durs), 3),
        "start_ts": _ts(min(r["start_ts"] for r in last.values())),
        "end_ts": _ts(max((r["end_ts"] or r["start_ts"]) for r in last.values())),
        "attempts": len(rows),
    }


def register(app, sql, ok, err):
    def _need(rows):
        return rows is None

    @app.get("/api/ops/runs")
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
        return ok({"from": f, "to": t, "days": days, "counts": cnt, "chain": CHAIN,
                   "labels": TASK_LABEL})

    @app.get("/api/ops/runs/<ds>")
    def ops_run_day(ds):
        if not _DS.match(ds):
            return err("ds 必须是 YYYY-MM-DD")
        rows = sql(f"SELECT {COLS} FROM ops_task_run WHERE ds=%s ORDER BY start_ts, id", (ds,))
        if _need(rows):
            return err("运行记录需要 MySQL（ops_task_run），当前读不到", 503)
        if not rows:
            return err(f"{ds} 没有运行记录", 404)
        last = latest_per_task(rows)
        order = CHAIN + EXTRA + sorted(set(last) - set(CHAIN) - set(EXTRA))
        tasks = []
        for tname in order:
            if tname in last:
                tasks.append(_row(last[tname]))
            elif tname in CHAIN:
                tasks.append({"task_id": tname, "label": TASK_LABEL[tname], "state": "not_recorded",
                              "start_ts": None, "end_ts": None, "duration_s": None, "block_reason": None,
                              "source": None, "source_label": None, "dag_id": None})
        return ok({**day_summary(ds, rows), "tasks": tasks, "history": [_row(r) for r in rows]})

    return app


def request_args(name):
    from flask import request
    return request.args.get(name) or None
