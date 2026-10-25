#!/usr/bin/env python3
"""配置驱动的数据质量规则引擎。

用法:
  python quality/engine.py --layer ods --ds 2022-09-01 --path <csv|parquet|dir> [--prev-path ...] [--no-db]
  python quality/engine.py --layer ads --ds 2022-09-01 --sql "SELECT ... WHERE dt='{ds}'"

规则文件: quality/rules/<layer>.yml。任一 block 级规则失败 -> 抛 DQBlockError / 退出码 2。
结果写入 MySQL dq_run_log（环境变量 DQ_DB_URL 或 MYSQL_* 默认值；--no-db 跳过）。
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
RULES_DIR = Path(__file__).resolve().parent / "rules"
DDL = Path(__file__).resolve().parent / "sql" / "dq_run_log.sql"
ALERT_DDL = Path(__file__).resolve().parent / "sql" / "dq_alert.sql"


class DQBlockError(RuntimeError):
    pass


@dataclass
class Result:
    rule_name: str
    rule_type: str
    severity: str
    passed: bool
    actual: str
    expected: str
    message: str = ""


# ---------- 数据读取 ----------
def read_frame(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        files = sorted(list(p.glob("*.parquet")) + list(p.glob("*.csv")))
        if not files:
            return pd.DataFrame()
        return pd.concat([read_frame(f) for f in files], ignore_index=True)
    if not p.exists():
        return pd.DataFrame()
    if p.suffix == ".parquet":
        return pd.read_parquet(p)
    return pd.read_csv(p)


def read_sql(sql: str) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


# ---------- 规则实现 ----------
def _r(rule, passed, actual, expected, msg=""):
    return Result(rule["name"], rule["type"], rule.get("severity", "block"),
                  bool(passed), str(actual), str(expected), msg)


def chk_not_null_rate(df, rule, **_):
    cols = rule["columns"]
    mode = rule.get("mode", "any")  # any: 任一列非空即算有值; all: 每列都非空
    if df.empty:
        return _r(rule, False, "empty", f">={rule['min']}", "空分区")
    nn = df[cols].notna()
    ok = nn.any(axis=1) if mode == "any" else nn.all(axis=1)
    rate = round(float(ok.mean()), 4)
    return _r(rule, rate >= rule["min"], rate, f">={rule['min']}",
              f"缺数比例 {1 - rate:.2%}")


def chk_group_null_rate(df, rule, **_):
    """按分组统计缺数比例超过阈值的组数，组数 <= max_groups 视为通过。"""
    cols, key = rule["columns"], rule["group_by"]
    miss = df[cols].isna().all(axis=1).groupby(df[key]).mean()
    n = int((miss > rule["threshold"]).sum())
    return _r(rule, n <= rule["max_groups"], n, f"<={rule['max_groups']}",
              f"缺数比例>{rule['threshold']:.0%} 的{key}数")


def chk_range(df, rule, **_):
    s = df[rule["column"]].dropna()
    lo, hi = rule.get("min"), rule.get("max")
    bad = pd.Series(False, index=s.index)
    if lo is not None:
        bad |= s < lo
    if hi is not None:
        bad |= s > hi
    n = int(bad.sum())
    return _r(rule, n <= rule.get("max_violations", 0), n,
              f"[{lo},{hi}] 越界<= {rule.get('max_violations', 0)}")


def chk_row_count_min(df, rule, **_):
    n = len(df)
    return _r(rule, n >= rule["min"], n, f">={rule['min']}")


def chk_row_count_delta(df, rule, prev_df=None, **_):
    if prev_df is None or prev_df.empty:
        return _r(rule, True, len(df), "无前一天，跳过", "skip")
    prev, cur = len(prev_df), len(df)
    delta = abs(cur - prev) / max(prev, 1)
    return _r(rule, delta <= rule["max_ratio"], f"{delta:.2%} ({prev}->{cur})",
              f"<={rule['max_ratio']:.0%}")


def chk_stuck_value(df, rule, **_):
    """同一 key 下 column 连续 N 个时段取值相同（忽略缺失）的 key 数。"""
    key, col, ts, n_slots = rule["group_by"], rule["column"], rule.get("order_by", "ts"), rule["n_slots"]
    hits = 0
    for _, g in df.sort_values(ts).groupby(key):
        s = g[col].dropna()
        if len(s) < n_slots:
            continue
        run = (s != s.shift()).cumsum()
        if run.value_counts().max() >= n_slots:
            hits += 1
    return _r(rule, hits <= rule.get("max_groups", 0), hits,
              f"<={rule.get('max_groups', 0)}", f"连续{n_slots}个时段读数不变的{key}数")


def _never_reporting(rule) -> set:
    """全量体检里整段无数据（missing_rate>=1）的站：排除名单，来自 lake 的 dwd/quality_station。"""
    lake = Path(os.getenv("SZ_EV_LAKE", ROOT / "data" / "lake"))
    qdir = lake / rule.get("exclude_from", "dwd/quality_station")
    full = qdir / rule.get("exclude_run", "run=2022-09-01_2023-02-28")
    src = full if full.exists() else qdir
    q = read_frame(src) if src.exists() else pd.DataFrame()
    if q.empty or "missing_rate" not in q.columns:
        return set()
    return set(q.loc[q["missing_rate"] >= 1, "station_id"])


def chk_station_went_empty(df, rule, prev_df=None, **_):
    """前一天有数据、当天整天全空的站数（排除整段无数据的站）。无前一天时跳过。"""
    key, cols = rule.get("group_by", "station_id"), rule.get("columns", ["busy", "idle"])
    if prev_df is None or prev_df.empty or df.empty:
        return _r(rule, True, 0, "无前一天，跳过", "skip")
    never = _never_reporting(rule)
    emp = df[cols].isna().all(axis=1).groupby(df[key]).all()
    had = ~prev_df[cols].isna().all(axis=1).groupby(prev_df[key]).all()
    went = (set(emp[emp].index) | (set(had[had].index) - set(df[key]))) & set(had[had].index)
    went -= never
    n, mx = len(went), rule.get("max_groups", 0)
    return _r(rule, n <= mx, n, f"<={mx}", f"前一天有数、当天全空的{key}数（已排除 {len(never)} 个整段无数据站）")


def chk_flag_groups(df, rule, **_):
    """按分组统计 flag 列为真的组数（如小紫按天算出的 is_stuck）。"""
    key, col = rule.get("group_by", "station_id"), rule["column"]
    if col not in df.columns:
        return _r(rule, True, 0, "列不存在，跳过", "skip")
    n = int(df[col].astype(bool).groupby(df[key]).any().sum())
    mx = rule.get("max_groups", 0)
    return _r(rule, n <= mx, n, f"<={mx}", f"{col} 为真的{key}数")


def chk_duplicate_key(df, rule, **_):
    n = int(df.duplicated(subset=rule["keys"]).sum())
    return _r(rule, n == 0, n, "0")


def chk_freshness(df, rule, ds=None, **_):
    if df.empty:
        return _r(rule, False, "empty", f">={ds}")
    mx = pd.to_datetime(df[rule["column"]]).max()
    need = pd.Timestamp(ds) + pd.Timedelta(rule.get("min_end_offset", "0h"))
    return _r(rule, mx >= need, mx, f">={need}")


CHECKS = {
    "not_null_rate": chk_not_null_rate,
    "group_null_rate": chk_group_null_rate,
    "range": chk_range,
    "row_count_min": chk_row_count_min,
    "row_count_delta": chk_row_count_delta,
    "stuck_value": chk_stuck_value,
    "station_went_empty": chk_station_went_empty,
    "flag_groups": chk_flag_groups,
    "duplicate_key": chk_duplicate_key,
    "freshness": chk_freshness,
}


def load_rules(layer: str, rules_dir: Path = RULES_DIR) -> list[dict]:
    cfg = yaml.safe_load((rules_dir / f"{layer}.yml").read_text(encoding="utf-8"))
    return cfg["rules"]


def run_rules(df, rules, ds=None, prev_df=None) -> list[Result]:
    out = []
    for rule in rules:
        if rule.get("enabled", True) is False:
            continue
        fn = CHECKS[rule["type"]]
        try:
            out.append(fn(df, rule, ds=ds, prev_df=prev_df))
        except Exception as e:  # 规则本身出错按失败处理
            out.append(_r(rule, False, "error", "", f"{type(e).__name__}: {e}"))
    return out


# ---------- MySQL ----------
def _connect():
    import pymysql
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"), password=os.getenv("MYSQL_PASSWORD", "rootpass"),
        database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4", autocommit=True,
        client_flag=pymysql.constants.CLIENT.FOUND_ROWS)


def _num(x):
    try:
        return float(str(x).split()[0].rstrip("%")) / (100 if str(x).split()[0].endswith("%") else 1)
    except (ValueError, IndexError):
        return None


def _threshold(rule):
    for k in ("min", "max", "max_groups", "max_ratio", "max_violations"):
        if rule.get(k) is not None:
            return float(rule[k])
    return 0.0 if rule["type"] == "duplicate_key" else None


def _ddl(path):
    sql = path.read_text(encoding="utf-8")
    return sql[sql.index("CREATE"):].rstrip().rstrip(";")


def write_alerts(results, layer, ds, rules):
    """未通过的规则写入 dq_alert；同一 ds/层/规则已有告警则只刷新数值，不重置处理状态。"""
    bad = [r for r in results if not r.passed]
    if not bad:
        return 0
    by_name = {r["name"]: r for r in rules}
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_ddl(ALERT_DDL))
            for r in bad:
                lvl = "阻断" if r.severity == "block" else "告警"
                detail = f"[{lvl}] {ds} {layer} 层规则 {r.rule_name} 未通过：实测 {r.actual}，要求 {r.expected}。{r.message}"[:500]
                args = (_num(r.actual), _threshold(by_name.get(r.rule_name, {"type": ""})), detail)
                n = cur.execute("UPDATE dq_alert SET metric_value=%s, threshold=%s, detail=%s WHERE ds=%s AND layer=%s"
                                " AND rule_name=%s AND station_id IS NULL", (*args, ds, layer, r.rule_name))
                if not n:
                    cur.execute("INSERT INTO dq_alert (ds,layer,rule_name,severity,metric_value,threshold,detail)"
                                " VALUES (%s,%s,%s,%s,%s,%s,%s)", (ds, layer, r.rule_name, r.severity, *args))
    finally:
        conn.close()
    return len(bad)


def write_log(results, layer, ds, run_id):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_ddl(DDL))
            cur.executemany(
                "INSERT INTO dq_run_log (run_id,ds,layer,rule_name,rule_type,severity,passed,actual,expected,message)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(run_id, ds, layer, r.rule_name, r.rule_type, r.severity, int(r.passed),
                  r.actual[:255], r.expected[:255], r.message[:512]) for r in results])
    finally:
        conn.close()


def check(layer, ds, df, prev_df=None, write_db=True, run_id=None, rules_dir=RULES_DIR, log=print):
    """执行规则、记录日志；block 失败抛 DQBlockError。返回结果列表。"""
    run_id = run_id or f"{layer}-{ds}-{uuid.uuid4().hex[:8]}"
    rules = load_rules(layer, rules_dir)
    results = run_rules(df, rules, ds=ds, prev_df=prev_df)
    for r in results:
        tag = "PASS" if r.passed else ("BLOCK" if r.severity == "block" else "WARN")
        log(f"[dq:{layer}:{ds}] {tag:5} {r.rule_name} actual={r.actual} expected={r.expected} {r.message}")
    if write_db:
        try:
            write_log(results, layer, ds, run_id)
            write_alerts(results, layer, ds, rules)
        except Exception as e:
            log(f"[dq] 写 dq_run_log 失败（不影响判定）: {e}")
    failed = [r for r in results if not r.passed and r.severity == "block"]
    if failed:
        raise DQBlockError(f"{layer} {ds}: {len(failed)} 条 block 规则失败: "
                           + ", ".join(r.rule_name for r in failed))
    return results


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True)
    ap.add_argument("--ds", required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--path")
    g.add_argument("--sql")
    ap.add_argument("--prev-path")
    ap.add_argument("--no-db", action="store_true")
    a = ap.parse_args(argv)
    df = read_frame(a.path) if a.path else read_sql(a.sql.format(ds=a.ds))
    prev = read_frame(a.prev_path) if a.prev_path else None
    try:
        check(a.layer, a.ds, df, prev, write_db=not a.no_db)
    except DQBlockError as e:
        print(f"[dq] BLOCKED: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
