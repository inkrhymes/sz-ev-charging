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
