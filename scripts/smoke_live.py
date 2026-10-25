#!/usr/bin/env python3
"""对正在运行的 API 做只读冒烟检查（不改任何数据）。

用法：python scripts/smoke_live.py [--base http://localhost:5000]
权限检查用不存在的告警 id（999999999），且权限校验在查库之前，不会写库。
任一项失败则退出码为 1。只用标准库。
"""
import argparse
import json
import sys
import urllib.error
import urllib.request

FAKE_ID = 999999999


def call(base, path, method="GET", token=None, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw, status, headers = r.read(), r.status, r.headers
    except urllib.error.HTTPError as e:
        raw, status, headers = e.read(), e.code, e.headers
    try:
        js = json.loads(raw or b"null")
    except ValueError:
        js = None
    return status, headers, js


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:5000")
    base = ap.parse_args().base.rstrip("/")
    results = []

    def check(name, fn):
        try:
            ok, detail = fn()
        except Exception as e:  # 连不上等
            ok, detail = False, f"{type(e).__name__}: {e}"
        results.append((name, ok, detail))

    def health():
        s, h, j = call(base, "/api/health")
        d = (j or {}).get("data") or {}
        return (s == 200 and d.get("source") == "mysql" and d.get("fallback") is False,
                f"HTTP {s}，source={d.get('source')}，fallback={d.get('fallback')}")
    check("健康检查读的是 MySQL 且没有退回文件", health)

    zones = []

    def zones_chk():
        s, h, j = call(base, "/api/zones")
        zones[:] = (j or {}).get("data") or []
        src = h.get("X-Data-Source")
        return (s == 200 and len(zones) == 331 and src == "mysql",
                f"HTTP {s}，片区 {len(zones)} 个，X-Data-Source={src}")
    check("片区数为 331，响应头 X-Data-Source=mysql", zones_chk)

    def series():
        taz = zones[0] if zones else "102"
        s, h, j = call(base, f"/api/zone/series?taz_id={taz}")
        d = (j or {}).get("data")
        pts = d.get("points") if isinstance(d, dict) else d
        n = len(pts or [])
        return s == 200 and n > 0, f"HTTP {s}，片区 {taz} 有 {n} 个点"
    check("片区曲线不为空", series)

    def baseline():
        taz = zones[0] if zones else "102"
        s, h, j = call(base, f"/api/baseline?taz_id={taz}")
        win = ((j or {}).get("data") or {}).get("forecast_window") or {}
        if not win.get("from"):
            return False, f"HTTP {s}，没有 forecast_window"
        q = f"/api/baseline?taz_id={taz}&from={win['from'][:10]}&to={win['to'][:10]}"
        s, h, j = call(base, q.replace(" ", "%20"))
        pts = ((j or {}).get("data") or {}).get("points") or []
        n = sum(1 for p in pts if p.get("y_hat_lgbm") is not None)
        return s == 200 and n > 0, f"HTTP {s}，{win['from'][:10]}～{win['to'][:10]} 有预测值的点 {n}/{len(pts)}"
    check("预测时段内 y_hat_lgbm 有值", baseline)

    def mapz():
        s, h, j = call(base, "/api/map/zones")
        return s == 200, f"HTTP {s}"
    check("地图接口 /api/map/zones 返回 200", mapz)

    def alerts():
        s, h, j = call(base, "/api/alerts")
        return s == 200, f"HTTP {s}"
    check("告警列表 GET 返回 200", alerts)

    def ro():
        s, h, j = call(base, f"/api/alerts/{FAKE_ID}", "PATCH", "demo-readonly", {"status": "ack"})
        return s == 403, f"HTTP {s}（期望 403）"
    check("访客 token 改告警被拒（403）", ro)

    def anon():
        s, h, j = call(base, f"/api/alerts/{FAKE_ID}", "PATCH", None, {"status": "ack"})
        return s == 401, f"HTTP {s}（期望 401）"
    check("不带 token 改告警被拒（401）", anon)

    print(f"冒烟检查：{base}")
    w = max(len(n) for n, _, _ in results)
    for name, ok, detail in results:
        print(f"{'通过' if ok else '失败'}  {name.ljust(w)}  {detail}")
    bad = sum(1 for _, ok, _ in results if not ok)
    print(f"合计：{len(results) - bad} 项通过，{bad} 项失败")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
