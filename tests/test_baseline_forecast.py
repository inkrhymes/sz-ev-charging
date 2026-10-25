import importlib

import pytest


@pytest.fixture()
def app_mod():
    m = importlib.import_module("app")
    m.app.config["TESTING"] = True
    return m


def _d(r):
    assert r.status_code == 200
    return r.get_json()["data"]


def test_metrics_present(app_mod):
    d = _d(app_mod.app.test_client().get("/api/baseline?taz_id=1106&from=2022-09-01&to=2022-09-01"))
    m = d["metrics"]
    assert set(m) == {"mae_lgbm", "mae_lag1", "rmse_lgbm", "rmse_lag1", "split_ts"}
    assert m["mae_lgbm"] < m["mae_lag1"] and m["split_ts"].startswith("2023-02-01")
    assert d["forecast_window"]["from"].startswith("2023-02-27")


def test_outside_window_null(app_mod):
    pts = _d(app_mod.app.test_client().get("/api/baseline?taz_id=1106&from=2022-09-01&to=2022-09-01"))["points"]
    assert pts and all(p["y_hat_lgbm"] is None and p["abs_err_lgbm"] is None for p in pts)


def test_inside_window_has_lgbm(app_mod):
    pts = _d(app_mod.app.test_client().get("/api/baseline?taz_id=1106&from=2023-02-27&to=2023-02-28"))["points"]
    assert len(pts) == 48
    assert pts[0]["ts_hour"] == "2023-02-27 00:00:00"
    for p in pts:
        assert p["y_hat_lgbm"] is not None
        assert abs(p["abs_err_lgbm"] - abs(p["y_true"] - p["y_hat_lgbm"])) < 1e-5


def test_missing_files_graceful(app_mod, tmp_path, monkeypatch):
    monkeypatch.setattr(app_mod, "FORECAST", tmp_path)
    d = _d(app_mod.app.test_client().get("/api/baseline?taz_id=1106&from=2022-09-01&to=2022-09-01"))
    assert all(v is None for v in d["metrics"].values())
    assert d["forecast_window"] is None
    assert d["points"] and d["points"][0]["y_hat_lgbm"] is None
