"""把 locust --csv 的 *_stats.csv 汇总成 markdown 表：QPS / P50 / P95 / P99 / 错误率。用法：python perf/summarize.py perf/out/nocache [perf/out/cache]"""
import csv
import sys


def load(prefix):
    out = {}
    for r in csv.DictReader(open(f"{prefix}_stats.csv")):
        n = int(r["Request Count"]); f = int(r["Failure Count"])
        out[r["Name"]] = (float(r["Requests/s"]), r["50%"], r["95%"], r["99%"], 100.0 * f / n if n else 0.0, n)
    return out


runs = [load(p) for p in sys.argv[1:]]
names = [k for k in runs[0] if k != "Aggregated"] + ["Aggregated"]
hdr = "| 接口 | " + " | ".join(f"{p.split('/')[-1]} QPS | P50 | P95 | P99 | 错误率" for p in sys.argv[1:]) + " |"
print(hdr); print("|" + "---|" * (1 + 5 * len(runs)))
for k in names:
    cells = []
    for r in runs:
        q, p50, p95, p99, er, _ = r.get(k, (0, "-", "-", "-", 0, 0))
        cells += [f"{q:.1f}", f"{p50}", f"{p95}", f"{p99}", f"{er:.2f}%"]
    print(f"| {k} | " + " | ".join(cells) + " |")
