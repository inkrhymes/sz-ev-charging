import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from quality import engine  # noqa: E402


def _ods(n_st=3, slots=288, missing=0.0):
    ts = pd.date_range("2022-09-01", periods=slots, freq="5min").astype(str)
    rows = []
    for s in range(n_st):
        for i, t in enumerate(ts):
            busy = (i + s) % 7
            rows.append({"station_id": s, "ts": t, "busy": busy, "idle": 10 - busy})
    df = pd.DataFrame(rows)
    if missing:
        idx = df.sample(frac=missing, random_state=0).index
        df.loc[idx, ["busy", "idle"]] = None
    return df


def test_all_pass():
    res = engine.check("ods", "2022-09-01", _ods(), write_db=False)
    assert all(r.passed for r in res)


def test_warn_does_not_block():
    res = engine.check("ods", "2022-09-01", _ods(missing=0.3), write_db=False)
    warn = [r for r in res if not r.passed]
    assert warn and all(r.severity == "warn" for r in warn)


def test_block_raises_on_duplicates():
    df = _ods()
    with pytest.raises(engine.DQBlockError, match="ods_pk_unique"):
        engine.check("ods", "2022-09-01", pd.concat([df, df.head(5)]), write_db=False)


def test_block_on_empty_partition():
    with pytest.raises(engine.DQBlockError):
        engine.check("ods", "2022-09-01", pd.DataFrame(columns=["station_id", "ts", "busy", "idle"]), write_db=False)


def test_row_count_delta_vs_prev_day():
    df = _ods()
    with pytest.raises(engine.DQBlockError, match="row_count_vs_prev_day"):
        engine.check("ods", "2022-09-01", df.head(100), prev_df=df, write_db=False)


def test_stuck_value_counts_groups():
    df = _ods(n_st=2)
    df.loc[df.station_id == 0, "busy"] = 3
    rule = {"name": "s", "type": "stuck_value", "column": "busy", "group_by": "station_id",
            "n_slots": 288, "max_groups": 0, "severity": "warn"}
    r = engine.run_rules(df, [rule])[0]
    assert r.actual == "1" and not r.passed


def test_cli_exit_code(tmp_path):
    p = tmp_path / "part.parquet"
    df = _ods()
    pd.concat([df, df.head(3)]).to_parquet(p)
    assert engine.main(["--layer", "ods", "--ds", "2022-09-01", "--path", str(p), "--no-db"]) == 2
    df.to_parquet(p)
    assert engine.main(["--layer", "ods", "--ds", "2022-09-01", "--path", str(p), "--no-db"]) == 0


def test_station_went_empty_excludes_never_reporting(tmp_path, monkeypatch):
    import pandas as pd
    from quality import engine
    q = tmp_path / "dwd" / "quality_station"
    q.mkdir(parents=True)
    pd.DataFrame({"station_id": [1, 2, 3], "missing_rate": [0.0, 0.1, 1.0]}).to_parquet(q / "p.parquet")
    monkeypatch.setenv("SZ_EV_LAKE", str(tmp_path))
    nan = float("nan")
    prev = pd.DataFrame({"station_id": [1, 2, 3], "busy": [1, 1, nan], "idle": [1, 1, nan]})
    cur = pd.DataFrame({"station_id": [1, 2, 3], "busy": [nan, 1, nan], "idle": [nan, 1, nan]})
    rule = {"name": "e", "type": "station_went_empty", "max_groups": 0, "severity": "block"}
    r = engine.chk_station_went_empty(cur, rule, prev_df=prev)
    assert r.actual == "1" and not r.passed
    assert engine.chk_station_went_empty(cur, rule, prev_df=None).passed


def test_flag_groups_counts_stations():
    import pandas as pd
    from quality import engine
    df = pd.DataFrame({"station_id": [1, 1, 2, 3], "is_stuck": [0, 1, 1, 0]})
    r = engine.chk_flag_groups(df, {"name": "s", "type": "flag_groups", "column": "is_stuck", "max_groups": 1, "severity": "warn"})
    assert r.actual == "2" and not r.passed


# ---------- 卡死规则（2db48a3 固定 7 天窗口口径） ----------
def _stuck_day(stuck_ids, n_st=4):
    return pd.DataFrame([{"station_id": s, "taz_id": 100 + s, "ts": "2022-09-02 00:00",
                          "is_stuck": int(s in stuck_ids)} for s in range(n_st)])


def test_dwd_rules_thresholds_are_p95():
    rules = {r["name"]: r for r in engine.load_rules("dwd")}
    assert rules["dwd_stuck_stations"]["max_groups"] == 221
    assert rules["dwd_newly_stuck_stations"]["max_groups"] == 63
    assert rules["dwd_stuck_cumulative_30d"]["min_days"] == 30
    for n in ("dwd_stuck_stations", "dwd_newly_stuck_stations", "dwd_stuck_cumulative_30d"):
        assert rules[n]["severity"] == "warn"


def test_newly_flagged_groups():
    rule = {"name": "n", "type": "newly_flagged_groups", "column": "is_stuck", "max_groups": 1, "severity": "warn"}
    r = engine.chk_newly_flagged_groups(_stuck_day({0, 1, 2}), rule, prev_df=_stuck_day({0}))
    assert r.actual == "2" and not r.passed
    r = engine.chk_newly_flagged_groups(_stuck_day({0, 1}), rule, prev_df=_stuck_day({0}))
    assert r.passed
    assert engine.chk_newly_flagged_groups(_stuck_day({0, 1, 2}), rule).message == "skip"


def _write_lake(tmp_path, days):
    base = tmp_path / "dwd" / "charge_5min_clean"
    for i, ids in enumerate(days):
        d = base / f"dt=2022-09-{i + 1:02d}"
        d.mkdir(parents=True)
        _stuck_day(ids).to_parquet(d / "part.parquet")


def test_cumulative_flag_days_per_station(tmp_path, monkeypatch):
    _write_lake(tmp_path, [{0, 1}, {0}, {0, 2}, {0}])
    monkeypatch.setenv("SZ_EV_LAKE", str(tmp_path))
    rule = {"name": "c", "type": "cumulative_flag_days", "column": "is_stuck", "min_days": 3, "severity": "warn"}
    r = engine.chk_cumulative_flag_days(None, rule, ds="2022-09-04")
    assert not r.passed and r.actual == "1"
    g = r.groups[0]
    assert g["station_id"] == "0" and g["taz_id"] == "100" and g["metric"] == 4 and "已经有 4 天读数一动不动" in g["detail"] and "[告警]" not in g["detail"] and ">=" not in g["detail"]
    # 不读 ds 之后的分区
    r2 = engine.chk_cumulative_flag_days(None, rule, ds="2022-09-02")
    assert r2.passed and r2.groups == []


class _FakeCur:
    """模拟 dq_alert：key=(layer, rule, station)。"""
    def __init__(self):
        self.rows = {}

    def execute(self, sql, args):
        if sql.startswith("UPDATE"):
            metric, thr, detail, taz, layer, rule, st = args
            row = self.rows.get((layer, rule, st))
            if not row:
                return 0
            row.update(metric_value=metric, threshold=thr, detail=detail, taz_id=taz)
            return 1
        ds, layer, rule, sev, st, taz, metric, thr, detail = args
        self.rows[(layer, rule, st)] = dict(ds=ds, severity=sev, taz_id=taz, metric_value=metric,
                                            threshold=thr, detail=detail, status="open", note=None)
        return 1


def test_station_alerts_idempotent_keep_status():
    cur = _FakeCur()
    rule = {"name": "c", "min_days": 30}
    r = engine.Result("c", "cumulative_flag_days", "warn", False, "1", "")
    r.groups = [{"station_id": "7", "taz_id": "101", "metric": 30, "detail": "累计卡死 30 天"}]
    assert engine.write_station_alerts(cur, r, "dwd", "2022-10-01", rule) == 1
    row = cur.rows[("dwd", "c", "7")]
    row.update(status="ack", note="已派单")
    r.groups[0].update(metric=31, detail="累计卡死 31 天")
    assert engine.write_station_alerts(cur, r, "dwd", "2022-10-02", rule) == 0
    assert len(cur.rows) == 1
    assert row["metric_value"] == 31 and row["status"] == "ack" and row["note"] == "已派单"
    assert row["ds"] == "2022-10-01"


def test_old_stuck_detail_parsed_and_rerendered():
    from quality import engine as e
    old = "[告警] 站 1018（片区 576）截至 2023-02-28 累计卡死 60 天（>= 30 天），首次 2022-09-03、最近 2023-02-28，建议派维修工单核查。"
    asof, f, l = e._OLD_STUCK.search(old).groups()
    txt = e.render_stuck_detail("1018", "576", asof, 60, f, l)
    assert txt == ("站 1018（片区 576）到 2月28日 为止，已经有 60 天读数一动不动，"
                   "第一次出现在 9月3日，最近一次在 2月28日，建议派人去现场看看。【提醒】")
