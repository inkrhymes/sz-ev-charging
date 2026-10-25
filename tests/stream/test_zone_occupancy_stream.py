"""stream/zone_occupancy_stream.py 的 file 模式端到端测试：造 3 站 2 片区的小数据，availableNow 跑一遍，核对窗口聚合。
没装 pyspark / 没有 Java 时跳过。"""
import csv
import glob
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if not (os.getenv("JAVA_HOME") or shutil.which("java")):
    pytest.skip("no Java", allow_module_level=True)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from stream import zone_occupancy_stream as zs  # noqa: E402


def msg(st, taz, ts, busy, piles, flag="ok"):
    return {"station_id": st, "taz_id": taz, "ts": ts, "busy": busy, "idle": piles - busy,
            "total_piles": piles, "occupancy": busy / piles if piles else None, "quality_flag": flag}


def test_file_mode_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("SZ_RT_BAD_FLAGS", "bad")
    inp = tmp_path / "in"; inp.mkdir()
    rows = [
        msg("s1", "11", "2022-09-01 10:00:00", 2, 10), msg("s2", "11", "2022-09-01 10:00:00", 4, 10),
        msg("s1", "11", "2022-09-01 10:05:00", 6, 10), msg("s3", "22", "2022-09-01 10:05:00", 1, 4),
        msg("s2", "11", "2022-09-01 10:05:00", 9, 10, flag="BAD"),   # 坏数据，应丢弃
        msg("s3", "22", "2022-09-01 10:10:00", 3, 0),                 # total_piles=0，应丢弃
    ]
    (inp / "part1.json").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    out = tmp_path / "out"
    zs.main(["--source", "file", "--input", str(inp), "--sink", "csv", "--out", str(out),
             "--checkpoint", str(tmp_path / "ckpt"), "--once"])
    got = {}
    for f in sorted(glob.glob(str(out / "*.csv"))):
        for r in csv.DictReader(open(f)):
            got[(r["taz_id"], r["window_end"])] = r   # 后面的批次覆盖前面的 = upsert 语义
    w = got[("11", "2022-09-01 10:15:00")]          # 窗口 [10:00,10:15)：s1@10:00, s2@10:00, s1@10:05
    assert (int(w["busy_sum"]), int(w["piles_sum"]), int(w["station_cnt"]), int(w["msg_cnt"])) == (12, 30, 2, 3)
    assert abs(float(w["occupancy"]) - 0.4) < 1e-6
    assert int(w["latency_ms"]) >= 0 and w["write_ts"]
    w = got[("11", "2022-09-01 10:05:00")]          # 窗口 [09:50,10:05)：只有 10:00 两条
    assert (int(w["busy_sum"]), int(w["piles_sum"])) == (6, 20)
    w = got[("22", "2022-09-01 10:20:00")]          # [10:05,10:20)：只有 s3@10:05，10:10 那条被丢
    assert (int(w["busy_sum"]), int(w["piles_sum"]), int(w["msg_cnt"])) == (1, 4, 1)
    assert all(int(r["piles_sum"]) > 0 for r in got.values())
    # 每个事件落 3 个窗口：片区 11 窗口结束 10:05/10:10/10:15/10:20；片区 22 为 10:10/10:15/10:20
    assert sorted(k[1][11:16] for k in got if k[0] == "11") == ["10:05", "10:10", "10:15", "10:20"]
    assert sorted(k[1][11:16] for k in got if k[0] == "22") == ["10:10", "10:15", "10:20"]


def test_to_rows_latency():
    import datetime as dt
    now = dt.datetime(2026, 1, 1, 0, 0, 1)
    r = zs.to_rows([{"taz_id": 11, "window_start": None, "window_end": None, "occupancy": 0.5, "busy_sum": 1,
                     "piles_sum": 2, "station_cnt": 1, "msg_cnt": 1, "ingest_ts_max": dt.datetime(2026, 1, 1)}], 7, now)
    assert r[0][0] == "11" and r[0][10] == 1000 and r[0][11] == 7


def test_bad_flags_configurable(monkeypatch):
    monkeypatch.setenv("SZ_RT_BAD_FLAGS", "")
    assert zs.bad_flags() == []
    monkeypatch.setenv("SZ_RT_BAD_FLAGS", " Bad, X ")
    assert zs.bad_flags() == ["bad", "x"]
