import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

MAIN = ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"]
AFTER = ["forecast_daily", "forecast_evaluate", "weather_join", "site_suggest"]
LAST = "cache_flush"


def test_chain_order_matches_dag_spec():
    from orchestration import tasks
    assert [n for n, _ in tasks.CHAIN] == MAIN
    assert [n for n, _ in tasks.AFTER_PUBLISH] == AFTER


# ---------- 解析测试：不装 Airflow 也能跑，用最小桩模块记录算子和依赖 ----------
class _Op:
    def __init__(self, task_id, python_callable, op_kwargs=None, retries=None, trigger_rule="all_success", **kw):
        self.task_id, self.fn, self.op_kwargs = task_id, python_callable, op_kwargs
        self.retries, self.trigger_rule, self.up = retries, trigger_rule, set()
        _Op.dag.tasks[task_id] = self

    def __rshift__(self, other):
        other.up.add(self.task_id)
        return other


class _Dag:
    def __init__(self, dag_id, **kw):
        self.dag_id, self.kw, self.tasks = dag_id, kw, {}

    def __enter__(self):
        _Op.dag = self
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture()
def parsed(monkeypatch):
    class Skip(Exception):
        pass

    class Fail(Exception):
        pass
    mods = {"airflow": types.ModuleType("airflow"), "airflow.operators": types.ModuleType("airflow.operators"),
            "airflow.operators.python": types.ModuleType("airflow.operators.python"),
            "airflow.exceptions": types.ModuleType("airflow.exceptions"),
            "airflow.utils": types.ModuleType("airflow.utils"),
            "airflow.utils.email": types.ModuleType("airflow.utils.email")}
    mods["airflow"].DAG = _Dag
    mods["airflow.operators.python"].PythonOperator = _Op
    mods["airflow.exceptions"].AirflowSkipException = Skip
    mods["airflow.exceptions"].AirflowFailException = Fail
    for k, v in mods.items():
        monkeypatch.setitem(sys.modules, k, v)
    import importlib.util
    spec = importlib.util.spec_from_file_location("sz_ev_daily_parse", ROOT / "orchestration" / "dags" / "sz_ev_daily.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m.dag, Skip, Fail


def test_dag_parses_with_after_publish_tasks(parsed):
    dag, _, _ = parsed
    t = dag.tasks
    assert dag.dag_id == "sz_ev_daily" and list(t) == MAIN + AFTER + [LAST]
    for a, b in zip(MAIN, MAIN[1:]):
        assert t[b].up == {a}
    assert t["forecast_daily"].up == {"publish"}
    assert t["forecast_evaluate"].up == {"publish", "forecast_daily"}
    assert t["forecast_evaluate"].trigger_rule == "none_failed"
    assert t["weather_join"].up == {"publish"} and t["site_suggest"].up == {"publish"}
    assert all(t[n].retries == 2 and t[n].op_kwargs == {"ds": "{{ ds }}"} for n in AFTER)
    assert t[LAST].up == {"publish", "forecast_evaluate", "weather_join", "site_suggest"}
    assert t[LAST].trigger_rule == "all_done"


@pytest.mark.parametrize("rc,expect", [(0, None), (2, "skip"), (3, "retry"), (1, "fail")])
def test_after_publish_exit_codes_in_dag(parsed, monkeypatch, rc, expect):
    dag, Skip, Fail = parsed
    from orchestration import run_log, tasks
    calls, recs = [], []
    monkeypatch.setattr(run_log, "record", lambda *a, **k: recs.append(a))
    monkeypatch.setattr(tasks.subprocess, "run", lambda cmd, **kw: (calls.append((cmd, kw)), types.SimpleNamespace(returncode=rc))[1])
    monkeypatch.setenv("MYSQL_HOST", "db-x")
    fn = dag.tasks["forecast_daily"].fn
    err = {None: None, "skip": Skip, "retry": tasks.TaskRetryable, "fail": Fail}[expect]
    if err:
        with pytest.raises(err):
            fn("2023-02-01")
    else:
        fn("2023-02-01")
    cmd, kw = calls[0]
    assert cmd[1:] == ["-m", "forecast.daily", "--ds", "2023-02-01"]
    assert kw["env"]["MYSQL_HOST"] == "db-x" and kw["env"]["SZ_EV_LAKE"]
    assert recs[0][2] == {None: "success", "skip": "skipped", "retry": "failed", "fail": "failed"}[expect]


def test_optional_scripts_skip_when_absent(monkeypatch, tmp_path):
    from orchestration import tasks
    fn = tasks._optional_script(tmp_path / "nope.py", "weather_join")
    with pytest.raises(tasks.TaskSkipped):
        fn("2023-02-01")


def test_run_chain_after_publish(monkeypatch):
    from orchestration import run_log, tasks
    monkeypatch.setattr(run_log, "record", lambda *a, **k: None)
    monkeypatch.setattr(tasks, "CHAIN", [(n, lambda ds, **kw: None) for n, _ in tasks.CHAIN])

    def boom(ds):
        raise RuntimeError("x")

    def skip(ds):
        raise tasks.TaskSkipped("缺输入")
    monkeypatch.setattr(tasks, "AFTER_PUBLISH", [("forecast_daily", boom), ("forecast_evaluate", lambda ds: None),
                                                 ("weather_join", skip), ("site_suggest", lambda ds: None)])
    monkeypatch.delenv("REDIS_URL", raising=False)
    res = tasks.run_chain("2023-02-01")
    st = {n: s for n, s, _ in res}
    assert [st[n] for n in AFTER] == ["failed", "skipped", "skipped", "success"]
    assert res[-1][:2] == ("cache_flush", "success")


def test_cache_flush_deletes_only_szev_keys(monkeypatch):
    fakeredis = pytest.importorskip("fakeredis")
    import redis
    from orchestration import tasks
    srv = fakeredis.FakeServer()
    monkeypatch.setattr(redis.Redis, "from_url", classmethod(lambda cls, url, **kw: fakeredis.FakeRedis(server=srv)))
    r = fakeredis.FakeRedis(server=srv)
    for i in range(1203):
        r.set(f"szev:mysql:/api/x?{i}", 1)
    r.set("other:key", 1)
    monkeypatch.setenv("REDIS_URL", "redis://fake:6379/0")
    tasks.cache_flush("2023-02-01")
    assert r.keys("szev:*") == [] and r.get("other:key") == b"1"


@pytest.mark.parametrize("url", ["", "redis://127.0.0.1:1/0"])
def test_cache_flush_noop_without_redis(monkeypatch, capsys, url):
    from orchestration import tasks
    monkeypatch.setenv("REDIS_URL", url)
    tasks.cache_flush("2023-02-01")          # 不抛异常即成功
    assert "跳过" in capsys.readouterr().out


def test_dag_imports_with_real_airflow():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(ROOT / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("sz_ev_daily")
    order = [t.task_id for t in dag.topological_sort()]
    assert order[:7] == MAIN and set(order[7:-1]) == set(AFTER) and order[-1] == LAST
    assert order.index("forecast_daily") < order.index("forecast_evaluate")
