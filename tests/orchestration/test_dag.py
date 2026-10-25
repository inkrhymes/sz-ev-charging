import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

MAIN = ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"]
AFTER = ["forecast_daily", "forecast_evaluate", "weather_join", "site_suggest"]


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
    assert dag.dag_id == "sz_ev_daily" and list(t) == MAIN + AFTER
    for a, b in zip(MAIN, MAIN[1:]):
        assert t[b].up == {a}
    assert t["forecast_daily"].up == {"publish"}
    assert t["forecast_evaluate"].up == {"publish", "forecast_daily"}
    assert t["forecast_evaluate"].trigger_rule == "none_failed"
    assert t["weather_join"].up == {"publish"} and t["site_suggest"].up == {"publish"}
    assert all(t[n].retries == 2 and t[n].op_kwargs == {"ds": "{{ ds }}"} for n in AFTER)


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
    st = {n: s for n, s, _ in tasks.run_chain("2023-02-01")}
    assert [st[n] for n in AFTER] == ["failed", "skipped", "skipped", "success"]


def test_dag_imports_with_real_airflow():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(ROOT / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("sz_ev_daily")
    order = [t.task_id for t in dag.topological_sort()]
    assert order[:7] == MAIN and set(order[7:]) == set(AFTER)
    assert order.index("forecast_daily") < order.index("forecast_evaluate")
