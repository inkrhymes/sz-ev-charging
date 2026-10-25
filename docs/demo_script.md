# 演示录屏脚本（1～2 分钟）

负责人：小黄。目标：字节面试能顺着讲「分层 → 指标 → 大屏 → 工单」。

## 开场（10s）
1. 打开 `web/index.html`（或经 Flask 静态托管）。
2. 说明项目：深圳充电站运行管理可视化系统；技术栈 Flask + MySQL + ECharts。

## 主链路（50～70s）
1. **KPI 条**：站数 / 片区 / 日均占用率 / 高峰小时 / 卡死站 / 高缺失站 —— 对应 `ads_city_overview` → `/api/overview`。
2. **占用率时序**：切片区，指出占用率与估算负荷 —— `/api/zone/series`。
3. **质检数字**：点出缺失率、卡死站、高缺失站 —— 清洗工作量可量化 —— `/api/quality`。
4. **naive 基线**：真实 vs lag1，用朴素基线做对照，暂不引入 LSTM —— `/api/baseline`。
5. **工单 stub**：把一条 open → processing → done —— 演示运维工单流转。
6. **只读登录**：demo / readonly 拿 token（可选 5s）。

## 收尾（10s）
- 一键：`docker compose up -d`；演示子集 20～30 站规则见 `docs/demo_subset_rules.txt`。
- 量化口播：25 站／14 天窗、整行缺失 26.42%、卡死 4、高缺失 6、接口 8 个、录屏 1～2 分钟（见 docs/quality_report.md）。

## 注意
- API 未起时大屏自动回落本地假数据，录屏前先 `MOCK=1` 起 API，角标显示 LIVE API 更稳。
