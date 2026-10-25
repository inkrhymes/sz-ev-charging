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
import re
import os
import sys
import uuid
from dataclasses import dataclass, asdict, field
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
    groups: list = field(default_factory=list)  # 站点级明细（如累计卡死站），写 dq_alert 时每站一行


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
    # 只认全量 run 目录；同层其它单日 run=* 目录是旧结果，不能回退去读整个目录
    q = read_frame(full) if full.exists() else pd.DataFrame()
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


def chk_newly_flagged_groups(df, rule, prev_df=None, **_):
    """当天 flag 为真、前一天不为真的组数（如新变卡死的站）。无前一天时跳过。"""
    key, col = rule.get("group_by", "station_id"), rule["column"]
    if col not in df.columns:
        return _r(rule, True, 0, "列不存在，跳过", "skip")
    if prev_df is None or prev_df.empty or col not in prev_df.columns:
        return _r(rule, True, 0, "无前一天，跳过", "skip")
    today = set(df.loc[df[col].astype(bool), key])
    before = set(prev_df.loc[prev_df[col].astype(bool), key])
    n, mx = len(today - before), rule.get("max_groups", 0)
    return _r(rule, n <= mx, n, f"<={mx}", f"当天{col}为真、前一天不为真的{key}数")


def _lake_daily_flags(rule, ds):
    """读 lake 中 dt<=ds 的所有分区，返回 (station_id, taz_id, 该站该天 flag 是否为真) 明细。"""
    lake = Path(os.getenv("SZ_EV_LAKE", ROOT / "data" / "lake"))
    base = lake / rule.get("source", "dwd/charge_5min_clean")
    key, col = rule.get("group_by", "station_id"), rule["column"]
    parts = []
    for d in sorted(base.glob("dt=*")):
        dt = d.name[3:]
        if dt > str(ds):
            continue
        f = read_frame(d)
        if f.empty or col not in f.columns:
            continue
        g = f.groupby(key).agg(taz_id=("taz_id", "first"), flag=(col, "max")).reset_index()
        g["dt"] = dt
        parts.append(g)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _md(d):
    t = pd.Timestamp(str(d))
    return f"{t.month}月{t.day}日"


def render_stuck_detail(station, taz, ds, days, first_dt, last_dt):
    """累计卡死站的大白话说明。"""
    zone = f"片区 {taz}" if taz not in (None, "") else "片区未知"
    return (f"站 {station}（{zone}）到 {_md(ds)} 为止，已经有 {int(days)} 天读数一动不动，"
            f"第一次出现在 {_md(first_dt)}，最近一次在 {_md(last_dt)}，建议派人去现场看看。【提醒】")[:500]


# 旧版站点级 detail：从中解析截至日/首次/最近日期，供 --refresh-details 重写
_OLD_STUCK = re.compile(r"截至 (\d{4}-\d{2}-\d{2}) 累计卡死 \d+ 天.*?首次 (\d{4}-\d{2}-\d{2})、最近 (\d{4}-\d{2}-\d{2})")


def chk_cumulative_flag_days(df, rule, ds=None, history=None, **_):
    """截至 ds（含）累计 flag 为真天数 >= min_days 的站；明细放进 groups，写 dq_alert 时每站一行。"""
    key, need = rule.get("group_by", "station_id"), rule["min_days"]
    h = history if history is not None else _lake_daily_flags(rule, ds)
    if h.empty:
        return _r(rule, True, 0, "无历史分区，跳过", "skip")
    agg = h[h["flag"].astype(bool)].groupby(key).agg(days=("dt", "nunique"), taz_id=("taz_id", "last"),
                                                     first_dt=("dt", "min"), last_dt=("dt", "max"))
    hit = agg[agg["days"] >= need].reset_index()
    groups = [{"station_id": str(r[key]), "taz_id": None if pd.isna(r["taz_id"]) else str(int(r["taz_id"])),
               "metric": int(r["days"]),
               "detail": render_stuck_detail(r[key], None if pd.isna(r["taz_id"]) else int(r["taz_id"]),
                                             ds, int(r["days"]), r["first_dt"], r["last_dt"])}
              for _, r in hit.iterrows()]
    res = _r(rule, not groups, len(groups), f"累计卡死>={need}天的{key}数=0",
             f"截至 {ds} 累计 {rule['column']} 天数>={need} 的{key}数")
    res.groups = groups
    return res


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
    "newly_flagged_groups": chk_newly_flagged_groups,
    "cumulative_flag_days": chk_cumulative_flag_days,
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
        client_flag=pymysql.constants.CLIENT.FOUND_ROWS,
        init_command="SET time_zone = '+08:00'")  # created_at/updated_at 按北京时间


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


def _fmt_num(x):
    if x is None:
        return "?"
    x = float(x)
    return str(int(x)) if x == int(x) else f"{x:g}"


def _fmt_pct(x):
    return "?" if x is None else f"{float(x) * 100:.1f}%".replace(".0%", "%")


def render_detail(rule, ds, actual, threshold):
    """按规则 yml 里的 phrase 生成给普通人看的中文说明；没有 phrase 时用通用说法。"""
    d = pd.Timestamp(ds)
    ctx = {"date": f"{d.month}月{d.day}日", "actual": _fmt_num(actual), "threshold": _fmt_num(threshold),
           "actual_pct": _fmt_pct(actual), "threshold_pct": _fmt_pct(threshold)}
    tpl = (rule or {}).get("phrase") or "{date}有一项数据检查没通过（实测 {actual}，警戒线 {threshold}）"
    lvl = "需要处理，已暂停后续计算" if (rule or {}).get("severity") == "block" else "提醒"
    return f"{tpl.format(**ctx)}。【{lvl}】"[:500]


def write_alerts(results, layer, ds, rules):
    """未通过的规则写入 dq_alert；同一 ds/层/规则已有告警则只刷新数值，不重置处理状态。"""
    bad = [r for r in results if not r.passed]
    good = [r for r in results if r.passed]
    if not results:
        return 0
    by_name = {r["name"]: r for r in rules}
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_ddl(ALERT_DDL))
            close_passed_alerts(cur, good, layer, ds)
            for r in bad:
                rule = by_name.get(r.rule_name, {"type": "", "severity": r.severity})
                act, thr = _num(r.actual), _threshold(rule)
                args = (act, thr, render_detail(rule, ds, act, thr))
                n = cur.execute("UPDATE dq_alert SET metric_value=%s, threshold=%s, detail=%s WHERE ds=%s AND layer=%s"
                                " AND rule_name=%s AND station_id IS NULL", (*args, ds, layer, r.rule_name))
                if not n:
                    cur.execute("INSERT INTO dq_alert (ds,layer,rule_name,severity,metric_value,threshold,detail)"
                                " VALUES (%s,%s,%s,%s,%s,%s,%s)", (ds, layer, r.rule_name, r.severity, *args))
            for r in bad:
                write_station_alerts(cur, r, layer, ds, by_name.get(r.rule_name, {}))
    finally:
        conn.close()
    return len(bad)


CLOSE_NOTE = "复查已通过（自动关闭）"


def close_passed_alerts(cur, passed, layer, ds):
    """规则复查通过：同 ds/层/规则下仍为 open 的规则级告警自动关闭，note 追加说明。
    不动 ack/closed 行，不动站点级（station_id 非空）行。返回关闭行数。"""
    n = 0
    for r in passed:
        n += cur.execute(
            "UPDATE dq_alert SET status='closed',"
            " note=CASE WHEN note IS NULL OR note='' THEN %s ELSE CONCAT(note, '；', %s) END"
            " WHERE ds=%s AND layer=%s AND rule_name=%s AND station_id IS NULL AND status='open'",
            (CLOSE_NOTE, CLOSE_NOTE, ds, layer, r.rule_name)) or 0
    return n


def write_station_alerts(cur, r, layer, ds, rule):
    """站点级告警：每站一行，按 (layer, rule_name, station_id) 幂等。
    已存在则只刷新 metric_value/threshold/detail，不动 ds（首次触发日）、status、note。返回新插入行数。"""
    new = 0
    thr = rule.get("min_days")
    for g in r.groups:
        n = cur.execute("UPDATE dq_alert SET metric_value=%s, threshold=%s, detail=%s, taz_id=%s"
                        " WHERE layer=%s AND rule_name=%s AND station_id=%s",
                        (g["metric"], thr, g["detail"][:500], g["taz_id"], layer, r.rule_name, g["station_id"]))
        if not n:
            cur.execute("INSERT INTO dq_alert (ds,layer,rule_name,severity,station_id,taz_id,metric_value,threshold,detail)"
                        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (ds, layer, r.rule_name, r.severity, g["station_id"], g["taz_id"], g["metric"], thr,
                         g["detail"][:500]))
            new += 1
    return new



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


def refresh_details():
    """把已有规则级告警的 detail 按 phrase 重新生成；只改 detail，不动 status/note/数值。"""
    import yaml
    rules = {}
    for f in RULES_DIR.glob("*.yml"):
        for r in yaml.safe_load(f.read_text(encoding="utf-8"))["rules"]:
            rules[r["name"]] = r
    conn = _connect()
    n = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT alert_id, ds, rule_name, severity, metric_value, threshold FROM dq_alert"
                        " WHERE station_id IS NULL")
            for aid, ds, name, sev, mv, thr in cur.fetchall():
                rule = rules.get(name, {"severity": sev})
                n += cur.execute("UPDATE dq_alert SET detail=%s, updated_at=updated_at WHERE alert_id=%s",
                                 (render_detail(rule, ds, mv, thr), aid))
            # 站点级累计卡死告警：用存量 metric_value + 旧 detail 里的日期重写；已是新格式的跳过
            cur.execute("SELECT alert_id, station_id, taz_id, metric_value, detail FROM dq_alert"
                        " WHERE station_id IS NOT NULL")
            for aid, sid, taz, mv, det in cur.fetchall():
                m = _OLD_STUCK.search(det or "")
                if m:
                    asof, f, l = m.groups()
                    n += cur.execute("UPDATE dq_alert SET detail=%s, updated_at=updated_at WHERE alert_id=%s",
                                     (render_stuck_detail(sid, taz, asof, mv, f, l), aid))
    finally:
        conn.close()
    return n


def main(argv=None):
    if argv is None and sys.argv[1:2] == ["--refresh-details"] or argv == ["--refresh-details"]:
        print(f"[dq] refreshed {refresh_details()} alert details (rule-level + station-level)")
        return 0
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
