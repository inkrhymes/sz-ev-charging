# Flask API 约定（小粉 · v0.1）

面向字节面试可讲：分层表 → 指标 SQL → 薄 REST → 大屏。半个月 8 个接口，全部读 ADS（工单可改状态）。

Base：`http://localhost:5000`  
统一响应：`{ "ok": true, "data": ... }`；错误：`{ "ok": false, "error": "..." }`

| # | Method | Path | 读表 | 大屏用途 |
|---|--------|------|------|----------|
| 1 | GET | `/api/health` | — | 探活 |
| 2 | GET | `/api/overview?dt=YYYY-MM-DD` | ads_city_overview | 顶部 KPI |
| 3 | GET | `/api/zone/series?taz_id=&from=&to=` | ads_zone_occupancy_hour | 时序/热力 |
| 4 | GET | `/api/quality` | ads_quality_summary | 质检数字 |
| 5 | GET | `/api/baseline?taz_id=&from=&to=` | ads_baseline_naive | naive 基线 |
| 6 | GET | `/api/tickets` | ads_ticket | 工单列表 |
| 7 | PATCH | `/api/tickets/<id>` body `{"status":"processing\|done"}` | ads_ticket | 流转 stub |
| 8 | POST | `/api/auth/login` body `{"user":"demo","role":"readonly\|admin"}` | stub | 只读登录 |

联调约定：无库时 `MOCK=1` 返回固定样例，方便 @小黄 先画大屏；ETL 入库后关 MOCK 接真 SQL。

## 数据模式（2026-10-25）

- 默认：读 `data/demo/dwd/*.csv`（小紫 ETL 实测），`/api/health` 里 `mode=file`
- `MOCK=1`：强制假数据；假数据口径对齐质检报告（25/卡死4/高缺失6/缺失26.42%）
- MySQL 入库后可再加 `mode=mysql`（TODO）

- `GET /api/zones`：返回片区 taz_id 列表（大屏下拉框使用）。
