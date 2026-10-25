"""可选 Redis 读缓存（fakeredis）：没设 REDIS_URL 时完全不变；设了以后 MISS→HIT，key 含查询参数，只缓存 200，写操作清前缀。"""
import importlib

import pytest

fakeredis = pytest.importorskip("fakeredis")


@pytest.fixture()
def m(monkeypatch):
    mod = importlib.import_module("app")
    monkeypatch.setenv("API_SOURCE", "file")
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.setattr(mod, "_redis_client", None)
    monkeypatch.setattr(mod, "_redis_down_until", 0.0)
    mod.app.config["TESTING"] = True
    return mod


def test_no_redis_unchanged(m):
    r = m.app.test_client().get("/api/zones")
    assert r.status_code == 200 and "X-Cache" not in r.headers


def test_hit_miss_and_params(m, monkeypatch):
    fr = fakeredis.FakeRedis()
    monkeypatch.setenv("REDIS_URL", "redis://fake")
    monkeypatch.setattr(m, "_redis_client", fr)
    c = m.app.test_client()
    a = c.get("/api/zones")
    b = c.get("/api/zones")
    assert a.headers["X-Cache"] == "MISS" and b.headers["X-Cache"] == "HIT" and a.get_json() == b.get_json()
    assert b.headers["Access-Control-Allow-Origin"] == "*"
    assert c.get("/api/overview?dt=2022-09-01").headers["X-Cache"] == "MISS"
    assert c.get("/api/overview?dt=2022-09-02").headers["X-Cache"] == "MISS"   # 参数不同 → 不同 key
    assert c.get("/api/overview?dt=2022-09-01").headers["X-Cache"] == "HIT"
    assert 0 < fr.ttl("szev:file:/api/zones?") <= 60
    c.get("/api/realtime/zones")
    assert 0 < fr.ttl("szev:file:/api/realtime/zones?") <= 5
    assert "X-Cache" not in c.get("/api/health").headers                      # 不在缓存名单
    assert c.get("/api/map/hourly?date=bad").status_code == 400
    assert fr.get("szev:file:/api/map/hourly?date=bad") is None               # 非 200 不缓存


def test_write_invalidates(m, monkeypatch):
    fr = fakeredis.FakeRedis()
    monkeypatch.setenv("REDIS_URL", "redis://fake")
    monkeypatch.setattr(m, "_redis_client", fr)
    fr.set("szev:file:/api/alerts?", b"{}"); fr.set("szev:file:/api/zones?", b"{}")
    with m.app.test_request_context("/api/alerts/7/ack", method="PATCH"):
        resp = m.app.response_class("{}", status=200)
        m._cache_store(resp)
    assert fr.get("szev:file:/api/alerts?") is None and fr.get("szev:file:/api/zones?") is not None


def test_redis_down_falls_through(m, monkeypatch):
    class Broken:
        def get(self, k): raise ConnectionError("down")
    monkeypatch.setenv("REDIS_URL", "redis://fake")
    monkeypatch.setattr(m, "_redis_client", Broken())
    r = m.app.test_client().get("/api/zones")
    assert r.status_code == 200 and "X-Cache" not in r.headers
