import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def test_chain_order_matches_dag_spec():
    from orchestration import tasks
    assert [n for n, _ in tasks.CHAIN] == ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd",
                                           "ads_build", "dq_ads", "publish"]


def test_dag_imports_with_order():
    pytest.importorskip("airflow")
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(ROOT / "orchestration" / "dags"), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag("sz_ev_daily")
    order = [t.task_id for t in dag.topological_sort()]
    assert order == ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"]
