"""调度运行记录：run_chain/Airflow 包装器写 ops_task_run，以及 /api/ops/runs 接口。"""
import datetime as dt
import importlib
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


# ---------- 写入端 ----------
@pytest.fixture()
def recorded(monkeypatch):
    from orchestration import run_log, tasks
    calls = []
    monkeypatch.setattr(run_log, "record", lambda *a, **k: calls.append(a))
    return tasks, calls


def test_run_chain_records_every_task(recorded, monkeypatch):
    tasks, calls = recorded
    monkeypatch.setattr(tasks, "CHAIN", [(n, lambda ds, **kw: None) for n, _ in tasks.CHAIN])
    res = tasks.run_chain("2022-09-01", write_db=True)
    assert [r[1] for r in res] == ["success"] * 7
    assert [(c[0], c[1], c[2]) for c in calls] == [(n, "2022-09-01", "success") for n, _ in tasks.CHAIN]
    assert all(c[6] == "run_day" for c in calls)


def test_block_records_reason_and_skips_downstream(recorded, monkeypatch):
    tasks, calls = recorded
    from quality.engine import DQBlockError, Result

    def block(ds, **kw):
        e = DQBlockError("dwd 2022-10-27: 1 条 block 规则失败")
        e.failed = [Result("dwd_station_went_empty", "station_went_empty", "block", False, "838", "<=50", "")]
        raise e
    chain = [(n, (block if n == "dq_dwd" else (lambda ds, **kw: None))) for n, _ in tasks.CHAIN]
    monkeypatch.setattr(tasks, "CHAIN", chain)
    tasks.run_chain("2022-10-27", write_db=True)
    st = {c[0]: c[2] for c in calls}
    assert st["dq_dwd"] == "blocked"
    assert st["ads_build"] == st["dq_ads"] == st["publish"] == "skipped"
    reason = next(c[5] for c in calls if c[0] == "dq_dwd")
    assert "838" in reason and "50" in reason and "10月27日" in reason


def test_no_db_writes_nothing(recorded, monkeypatch):
    tasks, calls = recorded
    monkeypatch.setattr(tasks, "CHAIN", [(n, lambda ds, **kw: None) for n, _ in tasks.CHAIN])
    tasks.run_chain("2022-09-01", write_db=False)
    assert calls == []


def test_airflow_wrapper_signature_has_no_kwargs(recorded):
    """Airflow 遇到 **kwargs 会塞整个 context；包装器只能接 ds/write_db。"""
    import inspect
    tasks, _ = recorded
    params = inspect.signature(tasks.recorded("ods_ingest", tasks.ods_ingest)).parameters
    assert list(params) == ["ds", "write_db"]


def test_record_failure_does_not_raise(monkeypatch):
    from orchestration import run_log
    monkeypatch.setattr(run_log, "_connect", lambda: (_ for _ in ()).throw(OSError("db down")))
    run_log.record("ods_ingest", "2022-09-01", "success", dt.datetime.now(), dt.datetime.now())


# ---------- 接口 ----------
def _r(i, ds, task, state, sec, src="airflow", reason=None, minute=0):
    s = dt.datetime(2026, 10, 25, 13, minute, i)
    return {"id": i, "dag_id": "sz_ev_daily", "ds": ds, "task_id": task, "state": state, "start_ts": s,
            "end_ts": s + dt.timedelta(seconds=sec), "duration_s": Decimal(str(sec)), "block_reason": reason,
            "source": src}


ROWS = [_r(i, "2022-10-26", t, "success", 2.0) for i, t in enumerate(
    ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"])] + [
    _r(10, "2022-10-27", "ods_ingest", "success", 20.0),
    _r(11, "2022-10-27", "dq_dwd", "failed", 1.0, minute=1, reason="boom"),
    _r(12, "2022-10-27", "dq_dwd", "blocked", 1.5, minute=2, reason="838 个站断数"),
    _r(13, "2022-10-27", "ads_build", "skipped", 0, minute=3, reason="上游任务未通过，本步未执行"),
]


@pytest.fixture()
def client(monkeypatch):
    m = importlib.import_module("app")

    def fake_sql(q, a=()):
        assert "ops_task_run" in q
        rows = ROWS
        if "ds=%s" in q:
            rows = [r for r in rows if r["ds"] == a[0]]
        else:
            lo = a[0] if "ds >= %s" in q else None
            hi = a[-1] if "ds <= %s" in q else None
            rows = [r for r in rows if (not lo or r["ds"] >= lo) and (not hi or r["ds"] <= hi)]
        return rows
    monkeypatch.setattr(m, "_sql", fake_sql)
    m.app.config["TESTING"] = True
    return m.app.test_client()


def test_runs_list(client):
    d = client.get("/api/ops/runs?from=2022-10-26&to=2022-10-27").get_json()["data"]
    assert d["counts"] == {"success": 1, "blocked": 1}
    a, b = d["runs"]
    assert a["status"] == "success" and a["complete"] and a["tasks_success"] == 7 and a["duration_s"] == 14.0
    assert a["started_at"] == "2026-10-25 13:00:00" and a["ended_at"] == "2026-10-25 13:00:08"
    assert set(a["steps"][0]) >= {"name", "status", "started_at", "ended_at", "duration_s", "error"}
    assert b["status"] == "blocked" and b["blocked_at"] == "dq_dwd" and b["error"] == "838 个站断数"
    assert b["complete"] is False and b["attempts"] == 4


def test_runs_filter_and_validation(client):
    assert [x["ds"] for x in client.get("/api/ops/runs?from=2022-10-27").get_json()["data"]["runs"]] == ["2022-10-27"]
    assert client.get("/api/ops/runs?from=10/27").status_code == 400


def test_run_day_detail(client):
    d = client.get("/api/ops/runs/2022-10-27").get_json()["data"]
    t = {x["name"]: x for x in d["steps"]}
    assert [x["name"] for x in d["steps"]][:7] == ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd",
                                                      "ads_build", "dq_ads", "publish"]
    assert t["dq_dwd"]["status"] == "blocked"          # 取最近一次尝试
    assert t["dq_dwd"]["error"] == "838 个站断数" and d["error"] == "838 个站断数"
    assert t["dq_ods"]["status"] == "not_recorded"
    assert t["dq_dwd"]["label"] == "清洗层质检"
    assert len(d["history"]) == 4 and d["history"][1]["status"] == "failed"
    assert client.get("/api/ops/runs/2022-01-01").status_code == 404
    assert client.get("/api/ops/runs/bad").status_code == 400


def test_runs_alias_same_as_ops(client):
    a = client.get("/api/runs?from=2022-10-26").get_json()
    assert a == client.get("/api/ops/runs?from=2022-10-26").get_json()
    assert client.get("/api/runs/2022-10-27").get_json() == client.get("/api/ops/runs/2022-10-27").get_json()


def test_runs_db_down_is_503(monkeypatch):
    m = importlib.import_module("app")
    monkeypatch.setattr(m, "_sql", lambda q, a=(): None)
    c = m.app.test_client()
    assert c.get("/api/ops/runs").status_code == 503


# ---------- 健康指标 ----------
def _dq(ds, layer, run, t, n, npass, bf):
    return {"ds": dt.date.fromisoformat(ds), "layer": layer, "run_id": run, "checked_at": dt.datetime(2026, 10, 25, 13, t),
            "n": n, "n_pass": Decimal(npass), "block_fail": Decimal(bf)}


def test_health_from_pure():
    import ops
    dq = [_dq("2022-10-26", "dwd", "a", 1, 10, 10, 0), _dq("2022-10-27", "dwd", "b", 2, 10, 7, 1),
          _dq("2022-10-27", "dwd", "c", 3, 10, 9, 0),   # 同一天重跑，取最近一次（已通过）
          _dq("2022-10-28", "dwd", "d", 4, 10, 8, 1), _dq("2022-10-28", "ods", "e", 4, 5, 5, 0)]
    build = [{"task_id": "dwd_build", "ds": dt.date(2022, 10, 28), "built_at": dt.datetime(2026, 10, 25, 12, 0)}]
    alerts = [{"layer": "dwd", "severity": "block", "status": "open", "n": 2},
              {"layer": "ods", "severity": "warn", "status": "open", "n": 3},
              {"layer": "dwd", "severity": "warn", "status": "ack", "n": 1}]
    h = ops.health_from(dq, build, alerts, ROWS, {"publish": {"data_until": dt.datetime(2023, 2, 28, 23)}},
                        dt.datetime(2026, 10, 25, 14, 30))
    f = h["freshness"]
    assert f["dwd"]["latest_ds"] == "2022-10-28" and f["dwd"]["latest_passed_ds"] == "2022-10-27"
    assert f["dwd"]["built_age_hours"] == 2.5 and f["ads"]["latest_ds"] is None
    assert f["publish"]["data_until"] == "2023-02-28 23:00:00"
    d = h["dq_pass_rate"]["dwd"]
    assert (d["days"], d["days_passed"], d["day_pass_rate"], d["rule_pass_rate"]) == (3, 2, 0.6667, 0.9)
    assert d["blocked_days"] == ["2022-10-28"] and h["dq_pass_rate"]["ods"]["day_pass_rate"] == 1.0
    assert h["open_alerts"] == {"open": 5, "ack": 1, "by_layer": {"dwd": 2, "ods": 3}, "by_severity": {"block": 2, "warn": 3}}
    assert [x["ds"] for x in h["last_runs"]] == ["2022-10-27", "2022-10-26"]
    assert "ads_build" not in h["last_runs"][0]["steps"]          # skipped 不计耗时
    assert h["last_runs_duration_s"]["max"] == 21.5 and h["last_runs_duration_s"]["min"] == 14.0


def test_health_endpoint_503_and_days(monkeypatch):
    m = importlib.import_module("app")
    m.app.config["TESTING"] = True
    monkeypatch.setattr(m, "_sql", lambda q, a=(): None)
    c = m.app.test_client()
    assert c.get("/api/ops/health_metrics").status_code == 503
    assert c.get("/api/ops/health_metrics?days=x").status_code == 400
