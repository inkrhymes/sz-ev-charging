"""调度运行记录：把每个任务的开始/结束/状态/拦截原因写进 MySQL ops_task_run。

Airflow DAG 和 run_day.py 都经过 orchestration/tasks.py 的 recorded()，两条路径写同一张表。
写库失败只打印，不影响任务本身；ORCH_RUN_LOG=0 可关闭。
"""
from __future__ import annotations

import os
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DDL = ROOT / "sql" / "ops" / "ops_task_run.sql"
DAG_ID = "sz_ev_daily"
_ready = False


def enabled() -> bool:
    return os.getenv("ORCH_RUN_LOG", "1") != "0"


def _connect():
    from quality import engine
    return engine._connect()


def ensure_table(cur) -> None:
    global _ready
    if not _ready:
        sql = "\n".join(l for l in DDL.read_text(encoding="utf-8").splitlines() if not l.strip().startswith("--"))
        cur.execute(sql)
        _ready = True


def _threshold(expected: str):
    m = re.search(r"-?\d+(\.\d+)?", str(expected or ""))
    return float(m.group()) if m else None


def explain_block(layer: str, ds: str, failed) -> str:
    """failed: [(rule_name, actual, expected), ...] → 用规则 phrase 生成的中文原因。"""
    from quality import engine
    try:
        rules = {r["name"]: r for r in engine.load_rules(layer)}
    except Exception:
        rules = {}
    parts = []
    for name, actual, expected in failed:
        rule = rules.get(name, {"severity": "block"})
        try:
            a = float(str(actual).split()[0])
        except (ValueError, IndexError):
            a = actual
        try:
            parts.append(engine.render_detail(rule, ds, a, _threshold(expected)))
        except Exception:
            parts.append(f"{name}: 实际 {actual}，要求 {expected}")
    return "；".join(parts)[:1000]


def record(task_id: str, ds: str, state: str, start: datetime, end: datetime | None,
           block_reason: str | None = None, source: str = "run_day", dag_id: str = DAG_ID, cur=None) -> None:
    dur = round((end - start).total_seconds(), 3) if end else None
    args = (dag_id, ds, task_id, state, start, end, dur, block_reason, source)
    sql = ("INSERT INTO ops_task_run (dag_id,ds,task_id,state,start_ts,end_ts,duration_s,block_reason,source)"
           " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
           " ON DUPLICATE KEY UPDATE state=VALUES(state), end_ts=VALUES(end_ts), duration_s=VALUES(duration_s),"
           " block_reason=VALUES(block_reason), source=VALUES(source)")
    if cur is not None:
        ensure_table(cur)
        cur.execute(sql, args)
        return
    if not enabled():
        return
    try:
        conn = _connect()
        try:
            with conn.cursor() as c:
                ensure_table(c)
                c.execute(sql, args)
        finally:
            conn.close()
    except Exception as e:  # 记录失败不影响调度
        print(f"[run_log] 写 ops_task_run 失败（不影响任务）: {e}", flush=True)


def reason_from_exc(task_id: str, ds: str, e: BaseException) -> str:
    failed = getattr(e, "failed", None)
    if failed and task_id.startswith("dq_"):
        return explain_block(task_id[3:], ds, [(r.rule_name, r.actual, r.expected) for r in failed])
    return f"{type(e).__name__}: {e}"[:1000]
