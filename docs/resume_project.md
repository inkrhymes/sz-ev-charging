# 简历项目描述（草稿）

**深圳市电动汽车充电站运行管理可视化系统**｜个人项目｜Python / Flask / MySQL / ECharts
仓库：https://github.com/inkrhymes/sz-ev-charging

- 基于公开数据集 UrbanEV（深圳，5 分钟粒度，CC0），选取 25 个站点作演示子集，搭建 ods → dwd → ads 三层数据链路，覆盖采集、清洗、分析、可视化全流程。
- 编写数据质检脚本，识别缺失、数值卡死和高缺失站点（演示子集缺失率 26.42%，卡死 4 站，高缺失 6 站），输出质检报告。
- 用 Flask 写轻量 REST API（文件模式，支持跨域），前端用 ECharts 做运行大屏，包含 KPI、时序曲线、片区汇总、质检面板和基线对比。
- 用 Docker Compose 编排 MySQL 8.0 与 API，编写一键启停脚本，串联建表、装载 10 万行演示数据、回填 ADS 指标和启动服务，新环境克隆后可一条命令复现，31 项验收全部通过。
- 预留运营调度辅助接口（工单、只读登录、lag-1 朴素基线），方便后续扩展预测模型。

> 注：数字与 docs/quality_report.md、docs/acceptance_checklist.md 保持一致。
