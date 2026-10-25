#!/usr/bin/env python3
"""不依赖 Airflow 的本地执行器：按 ds 跑与 DAG 相同的任务链。

  python orchestration/run_day.py --ds 2022-09-01
  python orchestration/run_day.py --start 2022-09-01 --end 2022-09-03   # 补跑，闭区间
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from orchestration import tasks  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ds")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args(argv)
    days = [a.ds] if a.ds else [d.strftime("%Y-%m-%d") for d in pd.date_range(a.start, a.end)]
    rc = 0
    for ds in days:
        t0 = time.perf_counter()
        res = tasks.run_chain(ds, write_db=not a.no_db)
        print(f"== {ds} 总耗时 {time.perf_counter() - t0:.2f}s")
        for name, st, sec in res:
            print(f"   {name:<11} {st:<8} {sec:6.2f}s")
        if any(st == "failed" for _, st, _ in res):
            rc = 1
            break  # 按天顺序补跑：某天失败即停，避免后续日期基于坏数据
    return rc


if __name__ == "__main__":
    sys.exit(main())
