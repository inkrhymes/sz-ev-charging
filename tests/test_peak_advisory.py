"""模块 5 辅线：电量错峰预警与高负荷提示接口（/api/ops/peak_advisory）测试。"""
from __future__ import annotations

import re

import pytest

BANNED_UI_WORDS = re.compile(r"口径|基线|窗口|血缘|stub|（无）|undefined|NaN|\[object Object\]")


def test_peak_advisory_file_mode_citywide_and_zone(monkeypatch):
    import app as api_app

    monkeypatch.setenv("API_SOURCE", "file")
    c = api_app.app.test_client()

    r = c.get("/api/ops/peak_advisory?occ_threshold=0.60&kw_per_pile=7")
    assert r.status_code == 200
    d = r.get_json()["data"]
    assert d["source"] == "file" and d["power_mode"] == "assumed"

    s = d["summary"]
    assert s["zone_cnt"] == 18
    assert s["warn_zone_cnt"] > 0
    assert s["peak_load_kw"] > 0
    assert "不接入电网控网" in s["power_note"]
    assert len(d["hourly_profile"]) == 24
    assert any(p["is_valley"] for p in d["hourly_profile"])

    zones = d["zones"]
    assert len(zones) == 18
    for z in zones:
        assert z["risk"] in {"high", "medium", "normal"}
        assert z["peak_span"] and z["valley_span"] and z["advice"]
        assert not BANNED_UI_WORDS.search(z["advice"]), z["advice"]

    # 单片区筛选 + 不同参考功率（60 kW 直流快充）
    r_z = c.get("/api/ops/peak_advisory?taz_id=1106&from=2022-09-01&to=2022-09-07&occ_threshold=0.50&kw_per_pile=60")
    assert r_z.status_code == 200
    dz = r_z.get_json()["data"]
    assert dz["summary"]["selected_taz"] == "1106"
    assert dz["summary"]["kw_per_pile"] == 60.0
    assert len(dz["hourly_profile"]) == 24


def test_peak_advisory_validation(monkeypatch):
    import app as api_app

    monkeypatch.setenv("API_SOURCE", "file")
    c = api_app.app.test_client()
    assert c.get("/api/ops/peak_advisory?from=2022/09/01").status_code == 400
    assert c.get("/api/ops/peak_advisory?occ_threshold=1.2").status_code == 400
    assert c.get("/api/ops/peak_advisory?occ_threshold=abc").status_code == 400
    assert c.get("/api/ops/peak_advisory?kw_per_pile=1").status_code == 400


def test_peak_advisory_mysql_mode_rated_power(monkeypatch):
    import app as api_app

    monkeypatch.setenv("API_SOURCE", "mysql")
    fake_rows = [
        {"taz_id": "645", "ts_hour": "2022-09-01 03:00:00", "occupancy": 0.15, "load_kw_est": 45.0},
        {"taz_id": "645", "ts_hour": "2022-09-01 19:00:00", "occupancy": 0.88, "load_kw_est": 264.0},
        {"taz_id": "645", "ts_hour": "2022-09-01 20:00:00", "occupancy": 0.84, "load_kw_est": 252.0},
    ]
    fake_piles = [{"taz_id": "645", "piles": 10}]

    def fake_sql(q, a=()):
        if "ads_zone_occupancy_hour" in q:
            return fake_rows
        if "dim_station" in q:
            return fake_piles
        return []

    monkeypatch.setattr(api_app, "_sql", fake_sql)
    c = api_app.app.test_client()
    r = c.get("/api/ops/peak_advisory?occ_threshold=0.70")
    assert r.status_code == 200
    d = r.get_json()["data"]
    assert d["source"] == "mysql" and d["power_mode"] == "rated"
    assert d["summary"]["high_risk_cnt"] == 1
    assert d["summary"]["peak_load_kw"] == 264.0
    assert d["zones"][0]["taz_id"] == "645" and d["zones"][0]["risk"] == "high"
