"""API 压测脚本（locust）。权重按大屏真实使用估计：打开页面先拉总览/片区列表/地图，之后用户主要在片区之间切换看曲线，
准实时面板每几秒轮询一次，质量、告警、选址、预测页偶尔看。

  locust -f perf/locustfile.py --host http://127.0.0.1:5055 --headless -u 50 -r 10 -t 60s --csv perf/out/run
"""
import os
import random

from locust import HttpUser, between, task

DATES = [f"2022-{m:02d}-{d:02d}" for m in (9, 10, 11, 12) for d in (1, 8, 15, 22)] + ["2023-01-10", "2023-02-20"]


class DashboardUser(HttpUser):
    # LOCUST_NO_WAIT=1：去掉思考时间，测吞吐上限
    wait_time = between(0, 0) if os.getenv("LOCUST_NO_WAIT") == "1" else between(0.5, 2)
    zones = ["11"]

    def on_start(self):
        r = self.client.get("/api/zones", name="/api/zones")
        try:
            z = r.json()["data"]
            if z:
                DashboardUser.zones = z
        except Exception:
            pass

    @task(10)
    def zone_series(self):
        taz = random.choice(self.zones)
        d = random.choice(DATES)
        self.client.get(f"/api/zone/series?taz_id={taz}&from={d}&to={d}", name="/api/zone/series")

    @task(8)
    def realtime(self):
        self.client.get("/api/realtime/zones", name="/api/realtime/zones")

    @task(5)
    def overview(self):
        self.client.get(f"/api/overview?dt={random.choice(DATES)}", name="/api/overview")

    @task(4)
    def map_zones(self):
        self.client.get("/api/map/zones", name="/api/map/zones")

    @task(3)
    def zones(self):
        self.client.get("/api/zones", name="/api/zones")

    @task(3)
    def alerts(self):
        self.client.get("/api/alerts", name="/api/alerts")

    @task(2)
    def quality(self):
        self.client.get("/api/quality", name="/api/quality")

    @task(2)
    def site_suggest(self):
        self.client.get(f"/api/site/suggest?top={random.choice([10, 20])}", name="/api/site/suggest")

    @task(1)
    def forecast_metrics(self):
        self.client.get("/api/forecast/metrics", name="/api/forecast/metrics")
