"""kafka_replay.py with a tiny synthetic dwd lake and a fake producer (no broker)."""
from __future__ import annotations

import io, json, sys
import pandas as pd
import pytest

import kafka_replay as kr

KEYS = ["station_id", "taz_id", "ts", "busy", "idle", "total_piles", "occupancy", "quality_flag"]


@pytest.fixture()
def lake(tmp_path):
    for dt in ("2022-09-01", "2022-09-02"):
        d = tmp_path / "dwd/charge_5min_clean" / f"dt={dt}"; d.mkdir(parents=True)
        ts = pd.date_range(dt, periods=3, freq="5min")
        rows = [(s, s // 10, t, 1.0, 3.0, 4, 0.25, "ok") for t in ts for s in (1003, 1001, 1002)]
        df = pd.DataFrame(rows, columns=KEYS).sample(frac=1, random_state=0)  # shuffled on disk
        df.loc[df.index[0], "occupancy"] = float("nan")
        df.to_parquet(d / "part-0.parquet", index=False)
    return tmp_path


class FakeProducer:
    def __init__(self): self.sent = []; self.flushes = 0
    def send(self, topic, key, value): self.sent.append((topic, key, value))
    def flush(self): self.flushes += 1


def test_order_keys_schema(lake):
    p = FakeProducer()
    n = kr.replay(lake, p)
    assert n == 18 and len(p.sent) == 18
    order = [(v["ts"], v["station_id"]) for _, _, v in p.sent]
    assert order == sorted(order)
    assert p.sent[0][2]["ts"] == "2022-09-01T00:00:00+08:00" and p.sent[-1][2]["ts"].startswith("2022-09-02T00:10")
    assert all(t == "charge_5min" and k == str(v["station_id"]).encode() for t, k, v in p.sent)
    assert all(list(v) == KEYS for _, _, v in p.sent)
    assert sum(v["occupancy"] is None for _, _, v in p.sent) == 2  # NaN -> null
    json.dumps([v for _, _, v in p.sent], allow_nan=False)


def test_filters(lake):
    p = FakeProducer()
    assert kr.replay(lake, p, start="2022-09-02", end="2022-09-02", stations={1002}) == 3
    assert {v["station_id"] for _, _, v in p.sent} == {1002}


def test_speed_sleeps_by_sim_time(lake):
    slept = []; now = [0.0]
    def sleep(s): slept.append(s); now[0] += s
    kr.replay(lake, FakeProducer(), end="2022-09-01", speed=300, sleep=sleep, clock=lambda: now[0])
    assert slept == pytest.approx([1.0, 1.0])  # 5 sim-min / 300 = 1 s per step


def test_dry_run_cli(lake, monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(kr, "LAKE", lake)
    monkeypatch.setitem(sys.modules, "kafka", None)  # proves dry-run never imports the client
    out = tmp_path / "o.jsonl"
    assert kr.main(["--start", "2022-09-01", "--end", "2022-09-01", "--dry-run", str(out)]) == 9
    lines = [json.loads(x) for x in out.read_text().splitlines()]
    assert len(lines) == 9 and lines[0]["station_id"] == 1001
    kr.main(["--end", "2022-09-01", "--stations", "1003", "--dry-run"])
    assert len(capsys.readouterr().out.splitlines()) == 3
