## 预测与质量保障（供并入 docs/architecture.md）

**预测（forecast/）**：从 dwd 片区小时数据（演示 CSV 或全量 Parquet `data/lake/dwd/charge_5min_clean/dt=*/`，一个函数 `load_zone_hour` 切换）构建时间、滞后（1/2/3/24 h）、滚动均值（3/24 h）与片区类别特征，训练 LightGBM 预测片区下一小时占用率，以「上一小时」为基线，按时间严格切分评估 MAE/RMSE（整体 + 分片区），产出 `metrics.json` 与 `predictions.csv`；后续计划落表 `ads_forecast_zone_hour`，经 `/api/baseline` 增加 `y_hat_lgbm` 字段给前端。

**测试（tests/）**：pytest 共 28 项——质检规则单测（缺失率、读数不动、缺数阈值 0.30、IQR 异常，基于合成数据直接跑 `etl/clean_to_dwd.py`）、接口测试（Flask test client，file 模式覆盖 health/overview/zones/zone series/stations 分页筛选/baseline/工单/登录/CORS/首页，与验收清单数字一致）、预测特征防泄漏测试（只用过去值、不跨片区、时间切分严格、Parquet 聚合、最少 7 天校验）。

**CI（.github/workflows/ci.yml，暂存于 ci/ci.yml）**：push/PR 到 main 时自动跑 pytest 与预测冒烟；第二个 job 用 `scripts/start.sh` 起 Docker Compose 全栈并探测关键接口（允许失败，不阻断主检查）。
