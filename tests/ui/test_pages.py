"""页面冒烟测试：逐页打开，检查没有 JS 报错、没有失败的接口请求、页面上没有禁用词。

需要 playwright（CI 的 ui 任务会装）；没装就整体跳过，不影响普通 pytest。
环境变量：
  UI_BASE   页面地址，默认 http://127.0.0.1:5096/index.html
  API_BASE  接口地址，默认 http://127.0.0.1:5000
  CHROME    可选，本机 Chrome 路径（不设就用 playwright 自带的 chromium）
"""
import os
import re

import pytest

pw = pytest.importorskip("playwright.sync_api")

UI_BASE = os.environ.get("UI_BASE", "http://127.0.0.1:5096/index.html")
API_BASE = os.environ.get("API_BASE", "http://127.0.0.1:5000")
PAGES = ["overview", "zone", "baseline", "map", "site", "realtime", "lineage", "alerts", "quality", "tickets"]
BANNED = re.compile(r"口径|基线|窗口|stub|（无）|undefined|NaN|\[object Object\]")

pytestmark = pytest.mark.skipif(not os.environ.get("UI_TESTS"), reason="设 UI_TESTS=1 才跑页面测试（要先起页面和接口）")


@pytest.fixture(scope="module")
def browser():
    with pw.sync_playwright() as p:
        kw = {"args": ["--no-sandbox"]}
        if os.environ.get("CHROME"):
            kw["executable_path"] = os.environ["CHROME"]
        b = p.chromium.launch(**kw)
        yield b
        b.close()


def open_page(browser):
    pg = browser.new_page(viewport={"width": 1440, "height": 900})
    pg.add_init_script(f'localStorage.setItem("API_BASE", "{API_BASE}")')
    probs = {"js": [], "api": [], "write": []}
    pg.on("pageerror", lambda e: probs["js"].append(str(e)))
    pg.on("console", lambda m: m.type == "error" and "favicon" not in (m.location or {}).get("url", "") and "Failed to load resource" not in m.text and probs["js"].append(m.text))
    pg.on("response", lambda r: "/api/" in r.url and r.status >= 400 and probs["api"].append(f"{r.status} {r.url}"))
    pg.on("requestfailed", lambda r: "/api/" in r.url and probs["api"].append(f"failed {r.url}"))
    pg.on("request", lambda r: r.method not in ("GET", "OPTIONS", "HEAD") and probs["write"].append(f"{r.method} {r.url}"))
    return pg, probs


@pytest.mark.parametrize("route", PAGES)
def test_page_clean(browser, route):
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/{route}")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1500)
    assert pg.locator(f"#view-{route}").is_visible()
    text = pg.evaluate("document.body.innerText")
    assert "离线样例" not in pg.inner_text("#srcText"), "页面退回了离线样例，接口没连上"
    assert not probs["js"], probs["js"]
    assert not probs["api"], probs["api"]
    assert not probs["write"], probs["write"]
    assert not BANNED.findall(text), BANNED.findall(text)
    pg.close()


def test_map_slider_and_ticket_flow(browser):
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/map")
    pg.wait_for_load_state("networkidle")
    pg.fill("#mapHour", "3")
    pg.dispatch_event("#mapHour", "input")
    pg.wait_for_timeout(1500)
    assert "03:00" in pg.inner_text("#mapHourText")
    assert pg.locator("#mapGreyNote").is_visible()
    pg.click("#mapPlay")
    pg.wait_for_timeout(1800)
    pg.click("#mapPlay")
    assert pg.inner_text("#mapHourText") != "03:00"
    pg.goto(f"{UI_BASE}#/tickets")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    assert pg.locator("#ticketTime").inner_text().strip()
    links = pg.locator("a[data-flow]")
    if links.count():
        links.first.click()
        assert pg.locator(".flow .st").count() == 5
    assert not probs["js"] and not probs["api"] and not probs["write"], probs
    pg.close()


def test_runs_page_empty_or_list(browser):
    """调度运行页：接口还没上线时显示空状态（允许 /api/ops/runs 和 /api/runs 404），有记录时出表格；都不能有 JS 报错。"""
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/runs")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    body = pg.inner_text("#runsBody")
    assert "加载中" not in body and body.strip()
    assert not probs["js"], probs["js"]
    assert not [u for u in probs["api"] if "/runs" not in u], probs["api"]
    assert not BANNED.findall(pg.evaluate("document.body.innerText"))
    pg.close()


def test_runs_page_renders_fixture(browser):
    """用假数据（不读线上）确认运行记录表能画出来。"""
    import json
    pg, probs = open_page(browser)
    fake = {"ok": True, "data": [{"ds": "2023-02-28", "status": "success", "started_at": "2026-10-25 02:00:00", "ended_at": "2026-10-25 02:12:30",
                                   "steps": [{"name": "清洗", "status": "success"}, {"name": "汇总", "status": "success"}]},
                                  {"ds": "2023-02-27", "status": "failed", "started_at": "2026-10-24 02:00:00", "duration_s": 95,
                                   "steps": [{"name": "清洗", "status": "failed"}], "error": "源文件缺失"}]}
    day = {"ok": True, "data": {"ds": "2023-02-27", "status": "failed", "steps": [
        {"name": "清洗", "status": "failed", "started_at": "2026-10-24 02:00:00", "ended_at": "2026-10-24 02:01:35", "duration_s": 95, "error": "源文件缺失"}]}}
    pg.route("**/api/ops/runs", lambda r: r.fulfill(status=200, content_type="application/json", body=json.dumps(fake)))
    pg.route("**/api/ops/runs/*", lambda r: r.fulfill(status=200, content_type="application/json", body=json.dumps(day)))
    pg.goto(f"{UI_BASE}#/runs")
    pg.wait_for_timeout(1500)
    t = pg.inner_text("#runsBody")
    assert "2023-02-28" in t and "成功" in t and "失败" in t and "12.5 分钟" not in t and "源文件缺失" in t
    pg.click('a[data-run="2023-02-27"]')
    pg.wait_for_timeout(800)
    d = pg.inner_text("#runsBody")
    assert "02:01:35" in d and "步骤" in d
    assert not probs["js"], probs["js"]
    pg.screenshot(path="/tmp/runs_fixture.png")
    pg.close()


def test_site_page_fixture(browser):
    """选址补桩：用假数据确认汇总、排名表、展开说明和地图点都能出来。"""
    import json
    pg, probs = open_page(browser)
    def row(i, add, reason):
        return {"taz_id": str(100 + i), "center_lon": 114.0 + i / 100, "center_lat": 22.6, "station_cnt": 3, "piles": 10, "rated_kw": 60,
                "avg_occupancy": 0.3, "peak_occupancy": 0.8, "busy_hours_ratio": 0.2, "demand_score": 80, "supply_score": 20,
                "gap_score": 90 - i, "suggest_add_piles": add, "rank": i + 1, "function_tag": "居住", "reason": reason}
    items = [row(0, 3, "晚上经常排满，建议加 3 根桩。"), row(1, 2, "建议加 2 根桩。"), row(2, 0, "有时比较紧张，先观察。"), row(3, 0, "桩够用，维持现状。")]
    pg.route("**/api/site/suggest*", lambda r: r.fulfill(status=200, content_type="application/json",
             body=json.dumps({"ok": True, "data": {"source": "mysql", "run_dt": "2026-10-25", "items": items}})))
    pg.goto(f"{UI_BASE}#/site")
    pg.wait_for_timeout(1500)
    s = pg.inner_text("#siteSum")
    assert "2 个片区" in s and "一共建议加 5 根桩" in s and "1 个片区" in s
    pg.click('a[data-sz="100"]')
    assert "晚上经常排满" in pg.inner_text("#siteBody")
    n = pg.evaluate("echarts.getInstanceByDom(document.getElementById('chartSite')).getOption().series.map(x => x.data.length)")
    assert n == [2, 1, 1]
    assert not probs["js"], probs["js"]
    assert not BANNED.findall(pg.evaluate("document.body.innerText"))
    pg.close()


def test_ticket_flow_uses_started_at(browser):
    """工单有 started_at 时，经过里的「开始处理」用它，统计里多出等待/处理两项；没有时照旧。"""
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/overview")
    pg.wait_for_timeout(1000)
    html = pg.evaluate("""() => ticketFlow({ticket_id: 1, station_id: 1, status: "done", created_at: "2026-10-01 08:00:00",
        started_at: "2026-10-01 09:00:00", updated_at: "2026-10-01 11:00:00"})""")
    assert "2026-10-01 09:00:00" in html and "等了 1 小时" in html and "动手后用了 2 小时" in html
    old = pg.evaluate("""() => ticketFlow({ticket_id: 2, station_id: 1, status: "done", created_at: "2026-10-01 08:00:00", updated_at: "2026-10-01 11:00:00"})""")
    assert "没有单独记时间" in old
    stats = pg.evaluate("""() => { ticketList = [{ticket_id: 1, status: "done", created_at: "2026-10-01 08:00:00", started_at: "2026-10-01 09:00:00",
        updated_at: "2026-10-01 11:00:00"}]; renderTicketTime(); return document.getElementById("ticketTime").innerText; }""")
    assert "平均等多久开始处理" in stats and "开始处理后平均多久完成" in stats
    assert not probs["js"], probs["js"]
    pg.close()


def test_forecast_daily_error_chart(browser):
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/baseline")
    pg.wait_for_load_state("networkidle")
    pg.click("#btnFc")
    pg.wait_for_timeout(2500)
    assert pg.locator("#fcDailyCard").is_visible()
    n = pg.evaluate("echarts.getInstanceByDom(document.getElementById('chartFcDaily')).getOption().xAxis[0].data.length")
    extra = pg.inner_text("#fcDailyExtra")
    assert (n > 0 and f"共 {n} 天" in extra) or "还没有" in pg.evaluate("JSON.stringify(echarts.getInstanceByDom(document.getElementById('chartFcDaily')).getOption().graphic)")
    assert not probs["js"], probs["js"]
    pg.click("#btnFc")
    assert not pg.locator("#fcDailyCard").is_visible()
    pg.close()


def test_runs_page_is_paged(browser):
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/runs")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1500)
    assert pg.locator("#runsBody a[data-run]").count() <= 10
    text = pg.inner_text("#runsBody")
    assert "not_recorded" not in text and "success" not in text
    assert not probs["js"], probs["js"]
    pg.close()


def test_fmt_dur_hours_and_minutes(browser):
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/tickets")
    pg.wait_for_load_state("networkidle")
    f = lambda m: pg.evaluate(f"fmtDur({m} * 60000)")
    assert f(27) == "27 分钟"
    assert f(165) == "2 小时 45 分"
    assert f(120) == "2 小时"
    assert f(192) == "3 小时 12 分"
    assert f(50 * 60 + 30) == "2 天 2 小时"
    text = pg.inner_text("#view-tickets")
    import re as _re
    assert not _re.search(r"\d+\.\d+ 小时", text), "处理用时不该再出现小数小时"
    pg.close()


def test_realtime_page_fixture_and_refresh(browser):
    """实时占用：先空着显示提示，回放开始后自动刷新出数据。"""
    import json
    pg, probs = open_page(browser)
    state = {"zones": [], "calls": 0}
    def z(t, occ, ws="2026-10-25 16:30:00", we="2026-10-25 16:45:00"):
        return {"taz_id": t, "window_start": ws, "window_end": we, "occupancy": occ, "busy_sum": int(occ * 20), "piles_sum": 20,
                "station_cnt": 2, "outlier_cnt": 0, "dropped_src_cnt": 0, "write_ts": "2026-10-25 16:45:03", "latency_ms": 800}
    def handle(r):
        state["calls"] += 1
        r.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True, "data": {"source": "mysql", "zones": state["zones"]}}))
    pg.route("**/api/realtime/zones*", handle)
    pg.goto(f"{UI_BASE}#/realtime")
    pg.wait_for_timeout(1500)
    assert "实时数据还没开始，回放开始后这里会自动刷新" in pg.inner_text("#rtBody")
    state["zones"] = [z("101", 0.2), z("102", 0.75), z("103", 0.4, "2026-10-25 16:15:00", "2026-10-25 16:30:00")]
    pg.wait_for_timeout(6000)  # 等一次自动刷新
    s = pg.inner_text("#rtSum")
    assert "16:30–16:45" in s and "3 个" in s and "其中 2 个" in s
    rows = pg.inner_text("#rtBody")
    assert rows.index("片区 102") < rows.index("片区 103") < rows.index("片区 101")
    assert "还没更新" in rows
    n = pg.evaluate("echarts.getInstanceByDom(document.getElementById('chartRt')).getOption().series[0].data.length")
    assert n == 3 and state["calls"] >= 2
    calls = state["calls"]
    pg.goto(f"{UI_BASE}#/overview"); pg.wait_for_timeout(6000)
    assert state["calls"] == calls, "离开实时页后应停止刷新"
    assert not probs["js"], probs["js"]
    assert not BANNED.findall(pg.evaluate("document.body.innerText"))
    pg.close()


def test_lineage_page_fixture(browser):
    """数据血缘：默认简要图用中文名、页面列；点节点标亮上下游并出中文详情；可切换全部细节；搜索、出错和空数据提示。"""
    import json
    from urllib.parse import urlparse, parse_qs
    pg, probs = open_page(browser)
    N = lambda i, k, l, lab: {"id": i, "kind": k, "layer": l, "name": lab, "label": lab}
    nodes = [N("raw:station_information", "raw", "raw", "UrbanEV 站点信息 CSV"), N("lake:ods/station_info", "lake", "ods", "ods/station_info"),
             N("hive:ods.station_info", "hive", "ods", "ods.station_info"), N("mysql:ads_zone_occupancy_hour", "mysql", "ads", "ads_zone_occupancy_hour"),
             N("api:/api/zones", "api", "api", "/api/zones"), N("mysql:no_map_table", "mysql", "ads", "no_map_table")]
    E = lambda s, t, sc, tasks, auto=True: {"source": s, "target": t, "kind": "write", "script": sc, "tasks": tasks, "auto": auto}
    edges = [E("raw:station_information", "lake:ods/station_info", "etl/full_pipeline.py", ["ods_ingest"]),
             E("lake:ods/station_info", "hive:ods.station_info", "etl/full_pipeline.py", ["ods_ingest"]),
             E("hive:ods.station_info", "mysql:ads_zone_occupancy_hour", "spark/jobs/ads_metrics.py", ["publish"], False),
             E("mysql:ads_zone_occupancy_hour", "api:/api/zones", "GET /api/zones", [])]
    mode = {"v": "ok"}; asked = []
    def handle(r):
        q = parse_qs(urlparse(r.request.url).query)
        if mode["v"] == "err":
            return r.fulfill(status=500, content_type="application/json", body=json.dumps({"ok": False, "error": "boom"}))
        if mode["v"] == "empty":
            d = {"nodes": [], "edges": []}
        elif q.get("node"):
            asked.append((q["node"][0], q.get("direction")))
            d = {"nodes": nodes[:5], "edges": edges}
        else:
            d = {"nodes": nodes, "edges": edges}
        r.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True, "data": d}))
    pg.route("**/api/lineage*", handle)
    pg.goto(f"{UI_BASE}#/lineage"); pg.wait_for_timeout(1500)
    js = "echarts.getInstanceByDom(document.getElementById('chartLineage')).getOption()"
    labels = dict(pg.evaluate(js + ".series[0].data.map(d => [d.id, d.label.formatter])"))
    assert "api:/api/zones" not in labels  # 简要图不画接口
    assert labels["mysql:ads_zone_occupancy_hour"] == "片区每小时占用率"
    assert labels["page:zone"] == "片区分析"
    assert labels["mysql:no_map_table"] == "no_map_table"  # 没有中文名就用原名
    heads = pg.evaluate(js + ".graphic[0].elements.map(e => e.style.text)")
    assert heads == ["原始文件", "整理后的文件", "数据仓库", "实时消息", "业务库", "页面"]
    xs = dict(pg.evaluate(js + ".series[0].data.map(d => [d.id, d.x])"))
    assert xs["raw:station_information"] < xs["lake:ods/station_info"] < xs["hive:ods.station_info"] < xs["mysql:ads_zone_occupancy_hour"] < xs["page:zone"]
    assert not re.search(r"[A-Za-z]", pg.inner_text("#view-lineage")), "默认不应看到英文"
    pg.fill("#lnSearch", "片区每小时占用率"); pg.press("#lnSearch", "Enter"); pg.wait_for_timeout(800)
    assert asked[-1] == ("mysql:ads_zone_occupancy_hour", ["both"])
    assert pg.inner_text("#lnPanelTitle") == "片区每小时占用率"
    body = pg.inner_text("#lnPanelBody")
    for t in ("类型", "业务库表", "原始名称", "ads_zone_occupancy_hour", "生成片区小时表", "spark/jobs/ads_metrics.py", "发布到业务库", "手工补充", "片区列表读取"):
        assert t in body, t
    op = dict(pg.evaluate(js + ".series[0].data.map(d => [d.id, d.itemStyle.opacity])"))
    assert op["mysql:no_map_table"] < 0.5 and op["raw:station_information"] == 1 and op["page:zone"] == 1
    pg.click("#lnToggle"); pg.wait_for_timeout(400)
    labels = dict(pg.evaluate(js + ".series[0].data.map(d => [d.id, d.label.formatter])"))
    assert labels["api:/api/zones"] == "片区列表接口" and "page:zone" in labels
    assert pg.inner_text("#lnToggle") == "只看简要" and not pg.is_visible("#lnPanel")
    pg.click("#lnToggle"); pg.wait_for_timeout(300)
    assert not probs["js"], probs["js"]
    assert not BANNED.findall(pg.evaluate("document.body.innerText"))
    for m, txt in (("err", "血缘数据暂时取不到"), ("empty", "还没有血缘数据")):
        mode["v"] = m
        pg.goto(f"{UI_BASE}#/overview"); pg.reload(); pg.goto(f"{UI_BASE}#/lineage"); pg.wait_for_timeout(1200)
        assert txt in pg.inner_text("#lnState")
    pg.close()
