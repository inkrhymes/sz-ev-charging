"""forecast/daily.py + evaluate.py：造 3 片区 21 天的小湖，不连真实 MySQL（假连接记录 SQL）。"""
import importlib

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm")
from forecast import daily, evaluate  # noqa: E402


def _lake(tmp_path, days=21, zones=("1", "2", "3")):
    rng = np.random.default_rng(0)
    for d in pd.date_range("2023-01-01", periods=days):
        ts = pd.date_range(d, periods=24, freq="h")
        rows = [{"taz_id": z, "ts_hour": t, "occupancy_avg": 0.3 + 0.2 * np.sin(t.hour / 24 * 6.28) + 0.01 * int(z)
                 + rng.normal(0, 0.01)} for z in zones for t in ts]
        rows.append({"taz_id": "9", "ts_hour": ts[0], "occupancy_avg": None})  # 一直没数据的片区
        p = tmp_path / "ads" / "zone_hour" / f"dt={d:%Y-%m-%d}"
        p.mkdir(parents=True)
        pd.DataFrame(rows).to_parquet(p / "part-0.parquet", index=False)
    return tmp_path


class FakeConn:
    def __init__(self):
        self.sql = []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, q, a=()):
        self.sql.append(q.split()[0])

    def executemany(self, q, rows):
        self.sql.append(("INSERTMANY", len(rows)))

    def commit(self):
        self.sql.append("COMMIT")

    def close(self):
        pass


def test_daily_predicts_next_day_all_zones_idempotent(tmp_path):
    lake = _lake(tmp_path)
    r1 = daily.run("2023-01-20", train_days=14, write_db=False, lake_dir=lake)
    r2 = daily.run("2023-01-20", train_days=14, write_db=False, lake_dir=lake)
    out = lake / "ads" / "forecast_zone_hour" / "dt=2023-01-21"
    assert len(list(out.glob("*"))) == 1 and r1["rows"] == r2["rows"] == 4 * 24
    p = pd.read_parquet(out)
    assert p["ts_hour"].min() == pd.Timestamp("2023-01-21 00:00") and p["ts_hour"].max() == pd.Timestamp("2023-01-21 23:00")
    assert p.loc[p.taz_id == "9", "y_hat_lgbm"].isna().all()
    good = p[p.taz_id != "9"]
    assert good["y_hat_lgbm"].between(0, 1).all() and (good["trained_until"] == pd.Timestamp("2023-01-20 23:00")).all()
    assert "hol" in r1["model_version"] and r1["peak_rss_mb"] > 0


def test_daily_missing_input_exit_code(tmp_path, monkeypatch):
    monkeypatch.setenv("SZ_EV_LAKE", str(_lake(tmp_path, days=5)))
    assert daily.main(["--ds", "2023-01-05", "--no-db"]) == 2
    assert daily.main(["--ds", "2023-03-01", "--no-db"]) == 2


def test_daily_write_mysql_delete_then_insert(tmp_path, monkeypatch):
    monkeypatch.setattr("forecast.db.ensure_tables", lambda c: None)
    c = FakeConn()
    p = pd.DataFrame({"taz_id": ["1"], "ts_hour": [pd.Timestamp("2023-01-21")], "y_hat_lgbm": [0.3], "y_hat_lag1": [np.nan],
                      "model_version": ["v"], "trained_until": [pd.Timestamp("2023-01-20 23:00")]})
    assert daily.write_mysql(p, "2023-01-21", conn=c) == 1
    assert c.sql == ["DELETE", ("INSERTMANY", 1), "COMMIT"]


def test_extra_feature_hook(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    monkeypatch.setitem(daily.EXTRA_FEATURES, "weather", lambda ts, taz: pd.DataFrame({"temp": 20.0}, index=ts.index))
    assert daily.run("2023-01-20", train_days=14, write_db=False, lake_dir=lake)["rows"] == 96


def test_holiday_flags():
    from forecast.calendar_cn import holiday_frame
    h = holiday_frame(pd.Series(pd.to_datetime(["2023-01-23 10:00", "2023-01-28 10:00", "2022-10-03 00:00", "2023-02-06 00:00"])))
    assert h["is_holiday"].tolist() == [1, 0, 1, 0]
    assert h["is_adjusted_workday"].tolist() == [0, 1, 0, 0]
    assert h["is_offday"].tolist() == [1, 0, 1, 0]


def test_evaluate_metrics_and_history(tmp_path):
    lake = _lake(tmp_path)
    for d in ("2023-01-18", "2023-01-19", "2023-01-20"):
        daily.run(d, train_days=14, write_db=False, lake_dir=lake)
    r = evaluate.run("2023-01-20", write_db=False, lake_dir=lake)
    assert r["n"] == 72 and r["mae_lgbm"] < 0.1 and r["mae_lag24"] is not None
    assert evaluate.main(["--ds", "2023-01-02", "--no-db"]) == 2


def _hist(pairs):
    return pd.DataFrame({"ds": [f"d{i}" for i in range(len(pairs))], "mae_lgbm": [a for a, _ in pairs],
                         "mae_lag1": [b for _, b in pairs]})


def test_drift_three_consecutive_worse_than_lag1():
    m = {"ds": "2023-02-10", "mae_lgbm": 0.05, "mae_lag1": 0.04}
    assert len(evaluate.drift(m, _hist([(0.05, 0.04), (0.05, 0.04)]))) == 1
    assert evaluate.drift(m, _hist([(0.03, 0.04), (0.05, 0.04)])) == []
    assert evaluate.drift(m, _hist([(0.05, 0.04)])) == []  # 历史不够 3 天


def test_drift_spike_over_median():
    hist = _hist([(0.02, 0.05)] * 10)
    a = evaluate.drift({"ds": "x", "mae_lgbm": 0.031, "mae_lag1": 0.05}, hist)
    assert len(a) == 1 and a[0]["threshold"] == pytest.approx(0.03) and "中位数" in a[0]["detail"]
    assert evaluate.drift({"ds": "x", "mae_lgbm": 0.031, "mae_lag1": 0.05}, hist.head(6)) == []


def test_evaluate_write_mysql_alert(monkeypatch):
    monkeypatch.setattr("forecast.db.ensure_tables", lambda c: None)
    c = FakeConn()
    m = {"ds": "2023-02-10", "n": 1, "mae_lgbm": 0.1, "rmse_lgbm": 0.1, "mae_lag1": 0.05, "rmse_lag1": 0.05,
         "mae_lag24": None, "model_version": "v"}
    evaluate.write_mysql(m, [{"metric_value": 0.1, "threshold": 0.05, "detail": "x"}], conn=c)
    assert c.sql == ["DELETE", "INSERT", "DELETE", "INSERT", "COMMIT"]


@pytest.fixture()
def app_mod(monkeypatch):
    m = importlib.import_module("app")
    monkeypatch.setenv("API_SOURCE", "mysql")
    m.app.config["TESTING"] = True
    return m


def test_api_forecast_metrics(app_mod, monkeypatch):
    import datetime as dt
    monkeypatch.setattr(app_mod, "_sql", lambda q, a=(): [{"ds": dt.date(2023, 2, 1), "n": 10, "mae_lgbm": 0.05,
                                                            "rmse_lgbm": 0.07, "mae_lag1": 0.06, "rmse_lag1": 0.08,
                                                            "mae_lag24": 0.055, "model_version": "v", "drift": 0}])
    d = app_mod.app.test_client().get("/api/forecast/metrics").get_json()["data"]
    assert d["source"] == "mysql" and d["points"][0]["ds"] == "2023-02-01" and d["points"][0]["drift"] is False
    monkeypatch.setenv("API_SOURCE", "file")
    assert app_mod.app.test_client().get("/api/forecast/metrics").get_json()["data"] == {"source": "none", "points": []}


def test_api_baseline_prefers_db_forecast(app_mod, monkeypatch):
    import datetime as dt
    monkeypatch.setattr(app_mod, "_sql", lambda q, a=(): [{"taz_id": "11"}] if "LIMIT 1" in q else [
        {"taz_id": "11", "ts_hour": dt.datetime(2023, 2, 2, 1), "y_true": 0.5, "y_hat_lag1": 0.4, "abs_err": 0.1}])
    monkeypatch.setattr(app_mod, "_mysql_forecast_preds", lambda taz: (
        {("11", "2023-02-02 01:00:00"): {"y_hat_lgbm": 0.45}}, {"from": "2023-02-01 00:00:00", "to": "2023-02-28 23:00:00"}))
    d = app_mod.app.test_client().get("/api/baseline?taz_id=11").get_json()["data"]
    assert d["points"][0]["y_hat_lgbm"] == 0.45 and d["points"][0]["abs_err_lgbm"] == 0.05
    assert d["forecast_window"]["to"] == "2023-02-28 23:00:00" and "metrics" in d
    monkeypatch.setattr(app_mod, "_mysql_forecast_preds", lambda taz: ({}, None))  # 表空 → 退回 predictions.csv
    d = app_mod.app.test_client().get("/api/baseline?taz_id=11").get_json()["data"]
    assert "y_hat_lgbm" in d["points"][0]
