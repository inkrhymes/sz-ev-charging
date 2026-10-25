#!/usr/bin/env python3
"""表级数据血缘：静态读代码生成 lineage/lineage.json，不运行任何作业。

自动推断：
  - MySQL 表清单：sql/**/*.sql 里的 CREATE TABLE。
  - 脚本读写：etl/ spark/jobs/ forecast/ stream/ quality/ orchestration/ 里的 SQL 字符串
    （INSERT/REPLACE/UPDATE/DELETE、INSERT OVERWRITE TABLE 算写，FROM/JOIN 算读）。
    同一条 SQL 里既写又读，就按这条 SQL 连边；字典 {"mysql表": "SELECT ... FROM hive表"} 按键值连边；
    其余按「脚本读到的表 × 脚本写的表」连边。f-string 动态表名（DELETE FROM {t}）时，
    把脚本里出现的整串 MySQL 表名都当成写入目标。
  - API：api/app.py、api/ops.py 的路由函数，连同它调用的模块内函数和常量，里面出现的 MySQL 表名算读；
    非 GET 路由里的 UPDATE/INSERT 算写。
手工补充：lineage/overrides.yml（数据湖路径、原始文件、脚本所属任务）。

用法：python lineage/build_lineage.py            # 重新生成 lineage.json
      python lineage/build_lineage.py --check    # 只比对，代码变了没重新生成时退出码 1
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "lineage" / "lineage.json"
OVERRIDES = ROOT / "lineage" / "overrides.yml"
SCAN_DIRS = ["etl", "spark/jobs", "forecast", "stream", "quality", "orchestration"]
API_FILES = ["api/app.py", "api/ops.py"]

W_MYSQL = re.compile(r"\b(?:INSERT\s+(?:IGNORE\s+)?INTO|REPLACE\s+INTO|DELETE\s+FROM|UPDATE)\s+`?(\w+)`?", re.I)
W_HIVE = re.compile(r"\bINSERT\s+OVERWRITE\s+TABLE\s+(\w+\.\w+)", re.I)
READ = re.compile(r"(?<!DELETE )\b(?:FROM|JOIN)\s+`?([\w.]+)`?", re.I)
DYN_WRITE = re.compile(r"(?:INSERT\s+INTO|REPLACE\s+INTO|DELETE\s+FROM)\s+$", re.I)


def mysql_tables() -> set[str]:
    out = set()
    for p in [*(ROOT / "sql").rglob("*.sql"), *(ROOT / "quality" / "sql").rglob("*.sql")]:
        out |= set(re.findall(r"CREATE TABLE (?:IF NOT EXISTS )?`?(\w+)`?", p.read_text(encoding="utf-8"), re.I))
    return out


def _docstrings(tree) -> set[int]:
    ids = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef, ast.AsyncFunctionDef)) and n.body:
            b = n.body[0]
            if isinstance(b, ast.Expr) and isinstance(b.value, ast.Constant) and isinstance(b.value.value, str):
                ids.add(id(b.value))
    return ids


def _strings(node, skip: set[int]) -> list[str]:
    """节点下的所有字符串常量；f-string 把常量部分拼起来，变量处用 {} 占位。"""
    out = []
    for n in ast.walk(node):
        if isinstance(n, ast.JoinedStr):
            out.append("".join(v.value if isinstance(v, ast.Constant) else "{}" for v in n.values))
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip:
            out.append(n.value)
    return out


def _sql_rw(s: str, my: set[str], hive: set[str]):
    w = {f"mysql:{t}" for t in W_MYSQL.findall(s) if t in my} | {f"hive:{t}" for t in W_HIVE.findall(s) if t in hive}
    r = set()
    for t in READ.findall(s):
        if t in my:
            r.add(f"mysql:{t}")
        elif t in hive:
            r.add(f"hive:{t}")
    return w, r - w


def hive_tables(files) -> set[str]:
    out = set()
    for f in files:
        txt = f.read_text(encoding="utf-8")
        out |= set(re.findall(r"(?:INSERT\s+OVERWRITE\s+TABLE|CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)\s*((?:ods|dwd|ads)\.\w+)", txt, re.I))
    return out


def scan_script(path: Path, my: set[str], hive: set[str]):
    """返回 [(上游, 下游)]：脚本内推断出的表级依赖。"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    skip = _docstrings(tree)
    strs = _strings(tree, skip)
    reads, writes, pairs = set(), set(), set()
    for s in strs:
        w, r = _sql_rw(s, my, hive)
        writes |= w; reads |= r
        pairs |= {(a, b) for a in r for b in w}
    if any(DYN_WRITE.search(s.split("{}")[0]) for s in strs if "{}" in s):
        writes |= {f"mysql:{s}" for s in strs if s in my}
    for n in ast.walk(tree):     # {"mysql表": "SELECT .. FROM hive表"}
        if isinstance(n, ast.Dict):
            for k, v in zip(n.keys, n.values):
                if isinstance(k, ast.Constant) and k.value in my and v is not None:
                    tgt = f"mysql:{k.value}"
                    for s in _strings(v, skip):
                        pairs |= {(a, tgt) for a in _sql_rw(s, my, hive)[1]}
    paired_targets = {b for _, b in pairs}
    for w in writes - paired_targets:      # 只对 MySQL 兜底；Hive 都是单条 INSERT OVERWRITE，临时视图的来源写在 overrides
        if w.startswith("mysql:"):
            pairs |= {(r, w) for r in reads if r != w}
    return pairs, writes


def _module_index(tree):
    idx = {}
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            idx[n.name] = n
        elif isinstance(n, ast.Assign):
            for t in n.targets:
                if isinstance(t, ast.Name):
                    idx[t.id] = n
    return idx


def _routes(tree):
    for n in ast.walk(tree):
        if not isinstance(n, ast.FunctionDef):
            continue
        for d in n.decorator_list:
            if isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.args \
                    and isinstance(d.args[0], ast.Constant) and d.func.attr in ("get", "post", "patch", "put", "delete", "route"):
                path = d.args[0].value
                if "<path:" in path or not path.startswith("/api/"):
                    continue
                method = d.func.attr.upper()
                if method == "ROUTE":
                    ms = [k for k in d.keywords if k.arg == "methods"]
                    method = "GET" if not ms else ast.literal_eval(ms[0].value)[0]
                yield path, method, n


def scan_api(my: set[str]):
    """返回 {(path, method): (reads, writes)}"""
    out = {}
    for rel in API_FILES:
        tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
        idx, skip = _module_index(tree), _docstrings(tree)
        # 嵌套函数（ops.register 里定义的）也能被同文件引用
        for n in ast.walk(tree):
            if isinstance(n, ast.FunctionDef):
                idx.setdefault(n.name, n)
        for path, method, fn in _routes(tree):
            seen, todo, strs = set(), [fn], []
            while todo:
                node = todo.pop()
                if id(node) in seen:
                    continue
                seen.add(id(node))
                strs += _strings(node, skip)
                for nm in ast.walk(node):
                    if isinstance(nm, ast.Name) and nm.id in idx:
                        todo.append(idx[nm.id])
            reads, writes = set(), set()
            for s in strs:
                found = {t for t in re.findall(r"\b(\w+)\b(?![.\w])", s) if t in my}  # dwd_zone_hour.csv 这类文件名不算
                w = set(W_MYSQL.findall(s)) & my
                reads |= found - w
                if method != "GET":
                    writes |= w
            key = (path, method)
            r0, w0 = out.get(key, (set(), set()))
            out[key] = (r0 | reads, w0 | writes)
    return out


def _node(nid: str, labels: dict) -> dict:
    kind, name = nid.split(":", 1)
    layer = {"raw": "raw", "stream": "stream", "file": "file", "api": "api", "mysql": "mysql"}.get(kind)
    if kind in ("lake", "hive"):
        layer = re.split(r"[./]", name)[0]
    elif kind == "mysql":
        layer = ("ads" if name.startswith(("ads_", "dim_")) else name.split("_")[0]) if name.split("_")[0] in ("ads", "dim", "ods", "dwd", "rt", "dq", "ops") else "mysql"
    return {"id": nid, "kind": kind, "name": name, "layer": layer, "label": labels.get(nid, name)}


def build() -> dict:
    ov = yaml.safe_load(OVERRIDES.read_text(encoding="utf-8"))
    my = mysql_tables()
    files = [p for d in SCAN_DIRS for p in sorted((ROOT / d).glob("*.py"))
             if p.name != "__init__.py" and str(p.relative_to(ROOT)) not in ov.get("skip_scan", [])]
    hive = hive_tables(files)
    tasks = ov.get("tasks", {})
    edges = {}

    def add(src, dst, via, kind="write", auto=True):
        key = (src, dst, via)
        if key not in edges:
            edges[key] = {"source": src, "target": dst, "kind": kind, "script": via,
                          "tasks": tasks.get(via, []) if via.endswith(".py") else [], "auto": auto}

    unmapped = []
    for p in files:
        rel = str(p.relative_to(ROOT))
        if rel in ov.get("manual_only", []):
            continue
        pairs, writes = scan_script(p, my, hive)
        if writes and rel not in tasks:
            unmapped.append(rel)
        for a, b in sorted(pairs):
            add(a, b, rel)
    for a, b, via in ov.get("drop_edges", []):
        edges.pop((a, b, via), None)
    for a, b, via in ov.get("edges", []):
        add(a, b, via, auto=False)
    for (path, method), (reads, writes) in sorted(scan_api(my).items()):
        api = f"api:{path}"
        for t in sorted(reads):
            add(f"mysql:{t}", api, f"{method} {path}", kind="read")
        for t in sorted(writes):
            add(api, f"mysql:{t}", f"{method} {path}", kind="write")
        for src in ov.get("api_inputs", {}).get(path, []) if method == "GET" else []:
            add(src, api, f"{method} {path}", kind="read", auto=False)
    add("file:lineage/lineage.json", "api:/api/lineage", "GET /api/lineage", kind="read", auto=False)

    labels = ov.get("labels", {}) | {"file:lineage/lineage.json": "血缘文件（本脚本生成）", "stream:airflow_task_events": "调度任务开始/结束事件"}
    ids = sorted({e["source"] for e in edges.values()} | {e["target"] for e in edges.values()})
    bad = [i for i in ids if i.split(":", 1)[0] not in ("raw", "lake", "hive", "mysql", "stream", "file", "api")]
    if bad:
        raise SystemExit(f"未知节点类型：{bad}")
    nodes = [_node(i, labels) for i in ids]
    used_mysql = {n["name"] for n in nodes if n["kind"] == "mysql"}
    return {
        "version": 1,
        "generated_by": "lineage/build_lineage.py",
        "nodes": nodes,
        "edges": sorted(edges.values(), key=lambda e: (e["source"], e["target"], e["script"])),
        "unused_mysql_tables": sorted(my - used_mysql),
        "scripts_without_task": sorted(unmapped),
    }


def dump(d: dict) -> str:
    return json.dumps(d, ensure_ascii=False, indent=1) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    text = dump(build())
    if a.check:
        if not OUT.exists() or OUT.read_text(encoding="utf-8") != text:
            print("lineage.json 已过期，运行 python lineage/build_lineage.py 重新生成")
            return 1
        print("lineage.json 是最新的")
        return 0
    tmp = OUT.with_suffix(".json.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(OUT)
    d = json.loads(text)
    print(f"节点 {len(d['nodes'])}，边 {len(d['edges'])} -> {OUT.relative_to(ROOT)}")
    if d["scripts_without_task"]:
        print("没写所属任务的脚本：", d["scripts_without_task"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
