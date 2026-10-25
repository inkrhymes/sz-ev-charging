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
PAGES = ["overview", "zone", "baseline", "map", "alerts", "quality", "tickets"]
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
    """调度运行页：接口还没上线时显示空状态（允许 /api/runs 404），有记录时出表格；都不能有 JS 报错。"""
    pg, probs = open_page(browser)
    pg.goto(f"{UI_BASE}#/runs")
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(1000)
    body = pg.inner_text("#runsBody")
    assert "加载中" not in body and body.strip()
    assert not probs["js"], probs["js"]
    assert not [u for u in probs["api"] if "/api/runs" not in u], probs["api"]
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
    pg.route("**/api/runs*", lambda r: r.fulfill(status=200, content_type="application/json", body=json.dumps(fake)))
    pg.goto(f"{UI_BASE}#/runs")
    pg.wait_for_timeout(1500)
    t = pg.inner_text("#runsBody")
    assert "2023-02-28" in t and "成功" in t and "失败" in t and "12.5 分钟" not in t and "源文件缺失" in t
    assert not probs["js"], probs["js"]
    pg.screenshot(path="/tmp/runs_fixture.png")
    pg.close()
