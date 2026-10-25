"""数据关系：lineage.json 与代码同步；API 读的每张 MySQL 表都有上游；/api/lineage 契约。"""
import json
import subprocess
import sys
from pathlib import Path

import app as api_app

ROOT = Path(__file__).resolve().parents[1]
D = json.loads((ROOT / "lineage" / "lineage.json").read_text(encoding="utf-8"))


def test_lineage_json_up_to_date():
    r = subprocess.run([sys.executable, str(ROOT / "lineage" / "build_lineage.py"), "--check"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_every_api_mysql_table_has_upstream():
    api_reads = {e["source"] for e in D["edges"]
                 if e["source"].startswith("mysql:") and e["target"].startswith("api:")}
    assert "mysql:dq_alert" in api_reads and "mysql:rt_zone_occupancy" in api_reads
    has_up = {e["target"] for e in D["edges"] if not e["source"].startswith("api:")}
    missing = sorted(api_reads - has_up)
    assert not missing, f"API 读的表没有上游：{missing}"


def test_edges_reference_nodes():
    ids = {n["id"] for n in D["nodes"]}
    assert len(ids) == len(D["nodes"])
    for e in D["edges"]:
        assert e["source"] in ids and e["target"] in ids, e


def test_api_lineage_contract():
    c = api_app.app.test_client()
    j = c.get("/api/lineage").get_json()
    assert j["ok"] and j["data"]["counts"] == {"nodes": len(D["nodes"]), "edges": len(D["edges"])}
    n0 = j["data"]["nodes"][0]
    assert {"id", "kind", "name", "layer", "label"} <= set(n0)
    assert {"source", "target", "kind", "script", "tasks", "auto"} <= set(j["data"]["edges"][0])
    up = c.get("/api/lineage?node=mysql:dq_alert&direction=up").get_json()["data"]
    assert any(e["target"] == "mysql:dq_alert" for e in up["edges"])
    assert c.get("/api/lineage?node=nope").status_code == 404
    assert c.get("/api/lineage?node=mysql:dq_alert&direction=x").status_code == 400
