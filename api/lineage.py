"""/api/lineage：表级数据血缘（读 lineage/lineage.json，由 lineage/build_lineage.py 静态生成，不连库不跑 Spark）。"""
import json
import os
from collections import deque
from pathlib import Path

from flask import request

_cache = {"mtime": None, "data": None}


def _path() -> Path:
    repo = Path(os.getenv("REPO_ROOT", Path(__file__).resolve().parents[1]))
    return repo / "lineage" / "lineage.json"


def load():
    p = _path()
    m = p.stat().st_mtime
    if _cache["mtime"] != m:
        _cache["data"], _cache["mtime"] = json.loads(p.read_text(encoding="utf-8")), m
    return _cache["data"]


def subgraph(d, node, direction="both"):
    """从 node 出发沿上游(up)/下游(down)走到底，返回子图。"""
    out_, in_ = {}, {}
    for e in d["edges"]:
        out_.setdefault(e["source"], []).append(e)
        in_.setdefault(e["target"], []).append(e)
    keep, edges = {node}, []
    for adj, end in ((in_, "source"), (out_, "target")):
        if direction not in ("both", "up" if adj is in_ else "down"):
            continue
        q, seen = deque([node]), {node}
        while q:
            for e in adj.get(q.popleft(), []):
                edges.append(e)
                if e[end] not in seen:
                    seen.add(e[end]); q.append(e[end])
        keep |= seen
    uniq = {(e["source"], e["target"], e["script"]): e for e in edges}
    return [n for n in d["nodes"] if n["id"] in keep], list(uniq.values())


def register(app, ok, err):
    @app.get("/api/lineage")
    def lineage():
        try:
            d = load()
        except FileNotFoundError:
            return err("lineage.json 不存在，先跑 python lineage/build_lineage.py", 503)
        nodes, edges = d["nodes"], d["edges"]
        node = request.args.get("node")
        if node:
            if node not in {n["id"] for n in nodes}:
                return err(f"没有这个节点：{node}", 404)
            direction = request.args.get("direction", "both")
            if direction not in ("up", "down", "both"):
                return err("direction 只能是 up / down / both")
            nodes, edges = subgraph(d, node, direction)
        kind = request.args.get("kind")
        if kind:
            ks = set(kind.split(","))
            nodes = [n for n in nodes if n["kind"] in ks]
            ids = {n["id"] for n in nodes}
            edges = [e for e in edges if e["source"] in ids and e["target"] in ids]
        return ok({"version": d.get("version"), "generated_by": d.get("generated_by"),
                   "counts": {"nodes": len(nodes), "edges": len(edges)},
                   "nodes": nodes, "edges": edges})
