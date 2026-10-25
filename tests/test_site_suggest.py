"""选址建议：export_ads_parquet --only site_suggest 的宽松列映射（假 Parquet）+ /api/site/suggest。"""
import datetime as dt
import importlib
import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark" / "jobs"))
import export_ads_parquet as e  # noqa: E402


def test_mapping_aliases_missing_extras(tmp_path, capsys):
    d = tmp_path / "zone_site_suggest" / "run_dt=2023-02-28"; d.mkdir(parents=True)
    pd.DataFrame({"taz_id": [11, 22, 33], "lon": [114.1, 114.2, 114.3], "lat": [22.5, 22.6, 22.7],
                  "station_count": [3, 1, 2], "total_piles": [30.0, 4.0, None], "gap_score": [0.2, 0.9, 0.5],
                  "add_piles": [2, 8, 4], "reason": ["a", "b", None], "weird_col": [1, 2, 3]}
                 ).to_parquet(d / "part-0.parquet")
    rows = e.site_suggest_rows(tmp_path / "zone_site_suggest")
    out = capsys.readouterr().out
    assert "weird_col" in out and "peak_occupancy" in out
    r = {x[1]: dict(zip(e.SITE_COLS, x)) for x in rows}
    assert len(rows) == 3 and r["22"]["run_dt"] == dt.date(2023, 2, 28)
    assert r["11"]["center_lon"] == 114.1 and r["11"]["station_cnt"] == 3 and r["11"]["piles"] == 30
    assert r["33"]["piles"] is None and r["33"]["reason"] is None and r["11"]["peak_occupancy"] is None
    assert r["22"]["suggest_add_piles"] == 8
    assert (r["22"]["rank"], r["33"]["rank"], r["11"]["rank"]) == (1, 2, 3)   # 无 rank 列 → 按 gap_score 倒序


def test_load_full_replace(tmp_path):
    f = tmp_path / "s.parquet"
    pd.DataFrame({"run_dt": ["2023-02-28"], "taz_id": ["5"], "rank": [1]}).to_parquet(f)
    calls = []

    class Cur:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, q, a=None): calls.append(q)
        def executemany(self, q, rows): calls.append((q, rows))

    class Conn:
        def cursor(self): return Cur()
        def commit(self): calls.append("commit")

    assert e.load_site_suggest(Conn(), f) == 1
    assert calls[0] == "DELETE FROM ads_zone_site_suggest" and calls[-1] == "commit"
    assert calls[1][1][0][:2] == (dt.date(2023, 2, 28), "5")


def test_missing_path(tmp_path):
    with pytest.raises(SystemExit):
        e.site_suggest_rows(tmp_path / "nope")


def test_api_site_suggest(monkeypatch):
    m = importlib.import_module("app")
    monkeypatch.setenv("API_SOURCE", "mysql")
    row = {"run_dt": dt.date(2023, 2, 28), "taz_id": 22, "center_lon": Decimal("114.2"), "center_lat": Decimal("22.6"),
           "station_cnt": 1, "piles": 4, "rated_kw": None, "avg_occupancy": Decimal("0.81234"), "peak_occupancy": None,
           "busy_hours_ratio": None, "demand_score": None, "supply_score": None, "gap_score": Decimal("0.9"),
           "suggest_add_piles": 8, "rank": 1, "function_tag": "居住", "reason": "b"}
    seen = {}
    monkeypatch.setattr(m, "_sql", lambda q, a=(): seen.update(a=a) or [row])
    c = m.app.test_client()
    r = c.get("/api/site/suggest?top=5")
    j = r.get_json()["data"]
    assert r.status_code == 200 and seen["a"] == (5,) and j["run_dt"] == "2023-02-28"
    it = j["items"][0]
    assert it["taz_id"] == "22" and it["avg_occupancy"] == 0.8123 and it["rank"] == 1 and "run_dt" not in it
    assert r.headers.get("X-Data-Source") == "mysql"
    assert c.get("/api/site/suggest?top=x").status_code == 400
    monkeypatch.setenv("API_SOURCE", "file")
    assert c.get("/api/site/suggest").get_json()["data"]["items"] == []
