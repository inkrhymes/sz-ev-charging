#!/usr/bin/env python3
"""把已经跑过的日子补进 ops_task_run（可重复执行，按唯一键覆盖）。

来源（都是北京时间入库）：
  1. Airflow 元数据库 airflow.task_instance（TIMESTAMP 列，会话时区 +08:00 读出即北京时间）
     → source=backfill_airflow（已有实时记录 source=airflow 的同一任务跳过）
  2. sz_ev.dq_run_log 里每次质检（与 Airflow 那次重叠的跳过）  → source=backfill_dq
  3. data/lake/_runs 里最近一次全量区间入湖（一次处理 181 天）  → source=backfill_range，
     按天平摊耗时，写成 ods_ingest / dwd_build 两行（时长是估算值）
单日的 _runs 记录都已被 Airflow 覆盖，不再重复写。每次执行先删掉 source 以 backfill 开头的旧行再重写。
  python orchestration/backfill_run_log.py [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from orchestration import run_log  # noqa: E402

LAKE = Path(os.getenv("SZ_EV_LAKE", ROOT / "data" / "lake"))
if not LAKE.exists():
    LAKE = Path("/workspace/sz-ev-charging/data/lake")
AF_STATE = {"success": "success", "failed": "failed", "upstream_failed": "skipped", "skipped": "skipped",
            "running": "running", "up_for_retry": "failed"}


def airflow_rows(cur):
    try:
        cur.execute("SELECT dag_id, run_id, task_id, state, start_date, end_date FROM airflow.task_instance"
                    " WHERE dag_id=%s AND start_date IS NOT NULL", (run_log.DAG_ID,))
    except Exception as e:
        print(f"[backfill] 读不到 airflow 元数据库，跳过: {e}")
        return []
    out = []
    for dag_id, run_id, task_id, state, s, e in cur.fetchall():
        ds = run_id.split("__", 1)[-1][:10]
        out.append(dict(dag_id=dag_id, ds=ds, task_id=task_id, state=AF_STATE.get(state, state),
                        start=s, end=e, source="backfill_airflow"))
    return out


def dq_rows(cur):
    cur.execute("SELECT run_id, DATE_FORMAT(ds,'%%Y-%%m-%%d'), layer, MIN(created_at), MAX(created_at),"
                " SUM(passed=0 AND severity='block') FROM dq_run_log GROUP BY run_id, ds, layer", ())
    runs = cur.fetchall()
    cur.execute("SELECT run_id, rule_name, actual, expected FROM dq_run_log WHERE passed=0 AND severity='block'")
    failed = defaultdict(list)
    for rid, name, a, e in cur.fetchall():
        failed[rid].append((name, a, e))
    out = []
    for rid, ds, layer, s, e, nblk in runs:
        blocked = bool(nblk)
        out.append(dict(dag_id=run_log.DAG_ID, ds=ds, task_id=f"dq_{layer}", state="blocked" if blocked else "success",
                        start=s, end=e, source="backfill_dq",
                        reason=run_log.explain_block(layer, ds, failed[rid]) if blocked else None))
    return out


def range_rows():
    out, latest = [], {}
    for f in sorted((LAKE / "_runs").glob("*.json")):  # 同一区间只取最近一次
        j = json.loads(f.read_text(encoding="utf-8"))
        a = j.get("args", {})
        if a.get("start") != a.get("end") and a.get("layer") == "all":
            k = (a["start"], a["end"])
            if k not in latest or j["finished"] > latest[k]["finished"]:
                latest[k] = j
    for j in latest.values():
        a = j["args"]
        days = [d.strftime("%Y-%m-%d") for d in
                (datetime.fromisoformat(a["start"]) + timedelta(days=i)
                 for i in range((datetime.fromisoformat(a["end"]) - datetime.fromisoformat(a["start"])).days + 1))]
        m = j.get("metrics", {})
        stage, split = m.get("stage", {}).get("secs", 0), m.get("split", {}).get("secs", 0)
        fin = datetime.fromisoformat(j["finished"])
        st = fin - timedelta(seconds=stage + split)
        for ds in days:
            for task, secs, t0 in (("ods_ingest", stage, st), ("dwd_build", split, st + timedelta(seconds=stage))):
                out.append(dict(dag_id=run_log.DAG_ID, ds=ds, task_id=task, state="success", start=t0,
                                end=t0 + timedelta(seconds=secs / len(days)), source="backfill_range"))
    return out


def _has_table(cur) -> bool:
    cur.execute("SHOW TABLES LIKE 'ops_task_run'")
    return bool(cur.fetchall())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    conn = run_log._connect()
    try:
        with conn.cursor() as cur:
            live = []
            if _has_table(cur):  # 已有实时记录（Airflow 包装器写的）的任务不再从元数据库重复补
                cur.execute("SELECT DATE_FORMAT(ds,'%%Y-%%m-%%d'), task_id, start_ts, end_ts FROM ops_task_run"
                            " WHERE source='airflow'", ())
                live = [(d, t, s - timedelta(seconds=5), (e or s) + timedelta(seconds=5)) for d, t, s, e in cur.fetchall()]
            af = [r for r in airflow_rows(cur)
                  if not any(w[0] == r["ds"] and w[1] == r["task_id"] and w[2] <= r["start"] <= w[3] for w in live)]
            # Airflow 里失败的 dq 任务：从同时间段的 dq_run_log 取拦截原因，状态改为 blocked
            dq = dq_rows(cur)
            win = [(r["ds"], r["task_id"], r["start"] - timedelta(seconds=3), (r["end"] or r["start"]) + timedelta(seconds=3))
                   for r in af] + live
            keep_dq = []
            for r in dq:
                hit = next((w for w in win if w[0] == r["ds"] and w[1] == r["task_id"] and w[2] <= r["start"] <= w[3]), None)
                if hit:
                    for x in af:
                        if (x["ds"], x["task_id"]) == hit[:2] and x["state"] == "failed" and r["state"] == "blocked":
                            x["state"], x["reason"] = "blocked", r["reason"]
                else:
                    keep_dq.append(r)
            rows = af + keep_dq + range_rows()
            for x in af:  # Airflow 中因上游被拦而未执行的任务补原因
                if x["state"] == "skipped":
                    x.setdefault("reason", "上游任务未通过，本步未执行")
            print(f"[backfill] airflow {len(af)} 行，dq_run_log {len(keep_dq)} 行（另有 {len(dq) - len(keep_dq)} 行与 Airflow 重叠已跳过），"
                  f"区间入湖 {len(rows) - len(af) - len(keep_dq)} 行")
            if a.dry_run:
                return 0
            run_log.ensure_table(cur)
            cur.execute("DELETE FROM ops_task_run WHERE source LIKE 'backfill%%'", ())
            for r in rows:
                run_log.record(r["task_id"], r["ds"], r["state"], r["start"], r["end"], r.get("reason"),
                               r["source"], r["dag_id"], cur=cur)
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT ds) FROM ops_task_run")
            print("[backfill] ops_task_run 现有 %s 行，覆盖 %s 天" % cur.fetchone())
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
