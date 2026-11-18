# 深圳市电动汽车充电站运行管理可视化系统

本项目为个人数据工程项目，基于公开数据集 [UrbanEV](https://doi.org/10.1038/s41597-025-04874-4)（深圳电动汽车充电站 5 分钟粒度运行数据，CC0）对 1,682 个站、181 天（2022-09-01 至 2023-02-28）的全量数据，构建「数据湖 → Spark/Hive 指标 → MySQL → API → 页面」的完整链路，带每日调度、数据质检告警和运维工单。

快速开始使用仓库附带的演示子集。集成测试验证 2022-09-01 的总览包含 25 个站，片区列表包含 18 个片区。下文标注“全量”的指标来自 1,682 个站、181 天的研究数据。

仓库地址：https://github.com/inkrhymes/sz-ev-charging

---

## 1. 架构

```
UrbanEV 原始 5 分钟 CSV（1,682 站 / 181 天）
  → Parquet 数据湖：ods（原样落地）/ dwd（清洗、卡死与缺失判定，含 quality_station、pile_power）
  → PySpark + Hive：ads 指标（片区小时占用率、负荷估算、朴素基线、城市总览）
  → MySQL 8（sz_ev 库，ads_* / dim_station / dq_alert / ads_ticket）
  → Flask API（localhost:5000）
  → ECharts 页面（总览、片区、基线与预测、地图、告警、数据质量、工单）
```

- **调度：** Airflow DAG `sz_ev_daily`，在 ods / dwd / ads 三层之间各设一道质检门；发布后接着跑日前预测、预测评估等任务，最后一步 `cache_flush` 清接口缓存（不管前面成败都会跑）。
- **质检被拦的日子：** 当天新数据不会覆盖进库，**保留上一次发布的数据**，并由 `dq_alert` 里的告警标出来；页面在这些日子会提示「显示的是上一次的数据」。例：2022-10-27 有 838 个站前一天有数、当天全空，超过阈值 50，在 dwd 质检被拦。
- **预测：** 两种用法，都和简单做法对比（见 `forecast/README.md`）：
  - 下一小时：LightGBM 预测片区下一小时占用率，对比「直接用上一小时」。
  - 日前：每天用当天为止的数据预测第二天 24 小时，对比「沿用最后一个已知值」和「照抄昨天同一时刻」。
  - 两者预测的时间跨度不同，误差数字不能直接互相比较。
- **工单：** 管理员从告警派工单，工单关联 `alert_id`，状态 open → processing → done。

## 2. 关键数字

| 项目 | 数值 | 出处 |
|------|------|------|
| 全量小时表 | 1,338,568 行（MySQL 与 Hive 一致） | `docs/perf/spark_baseline.md` |
| 全量 ETL（数据湖） | 2 分 03 秒（拆分并行前 7 分 25 秒） | ETL 提交记录 |
| Spark 全量 181 天 ads | 119 秒 | `docs/perf/spark_baseline.md` |
| Airflow 单日 | 7 个任务约 2 分钟 | `docs/orchestration_design.md` |
| 质检告警 | 661 条 | `docs/orchestration_design.md` |
| 下一小时预测：LightGBM vs 上一小时 | MAE 0.0255 vs 0.0285；RMSE 0.0461 vs 0.0578（2 月 28 天测试） | `forecast/README.md` |
| 日前预测（提前 1 天，24 小时） | LightGBM MAE 0.0626 / RMSE 0.0945；沿用最后一个值 0.0907 / 0.1493；照抄昨天同一时刻 MAE 0.0785（2023-02-01 至 02-28，28 天） | MySQL `ads_forecast_metric_daily`，`/api/forecast/metrics` |
| 全市平均占用率 | 0.267（全部片区、全部小时的平均） | MySQL `ads_zone_occupancy_hour` |
| 选址建议 | 11 个片区建议加桩，共 15 根；排第一的是 TAZ 645（建议加 5 根） | MySQL `ads_zone_site_suggest`，`/api/site/suggest` |
| 接口压测（MySQL，50 个模拟用户） | 实验使用 4 个 Gunicorn worker（Docker 默认 1 个）。常规负载下，启用 Redis 后 P50 从 12 ms 降至 3 ms。用户连续请求、不限速的 30 秒内，启用 Redis 后吞吐从约 329 升至 1,387 QPS，P50 从 150 ms 降至 33 ms，错误率为 0，缓存命中率约 88%。 | [压测配置与结果](docs/perf/api_loadtest.md) |
| 覆盖索引 | 查询约 220 ms → 约 2 ms | `docs/perf/spark_baseline.md` |
| Kafka 端到端回放（2022-09-01 一天） | 48.4 万条 16 秒发完（约 3 万条/秒）；流作业消费条数与发送一致，写出 325 个片区、93,727 行，平均占用率：流计算 0.273，同一天离线小时表 0.271 | `docs/streaming_design.md` 第 7 节 |
| 自动化测试 | Python 3.11 和 3.12 文件模式测试、Playwright 页面检查，以及 Docker Compose 下的 MySQL、Redis 和 API 集成测试 | [查看主分支 CI 记录](https://github.com/inkrhymes/sz-ev-charging/actions/workflows/ci.yml?query=branch%3Amain) |

## 3. 快速开始

准备 Git、Bash、Python 3.11 或 3.12（含 venv）、Docker 和 Docker Compose v2。以下命令在 Bash 中执行，Windows 可使用 Git Bash。

```bash
git clone https://github.com/inkrhymes/sz-ev-charging.git
cd sz-ev-charging
cp .env.example .env
# 首次使用仓库提供的本地演示配置。调整端口或凭据后重新加载自己的 .env。
set -a
source ./.env
set +a
bash scripts/start.sh
docker compose ps
curl -fsS "http://localhost:${API_PORT:-5000}/api/health"
```

启动脚本创建 Python 虚拟环境，启动 MySQL，执行建表和迁移，首次向空库装入演示数据，然后构建并启动 API。已有指标数据时会跳过演示数据装载。默认大屏地址为 <http://localhost:5000/>。健康检查应返回 `ok: true`，数据库模式下确认 `data.source` 为 `mysql` 且 `data.fallback` 为 `false`。

### 配置与排查

- MySQL 数据默认保存在 `data/mysql`。可通过 `MYSQL_DATA_DIR` 更改存储目录，用 `SKIP_DEMO_SEED=1` 跳过演示数据装载。
- Redis 读缓存默认关闭。在自己的 `.env` 设置 `REDIS_URL=redis://redis:6379/0` 后，重新加载环境变量并运行启动脚本。执行 `docker compose exec redis redis-cli ping` 应得到 `PONG`。连续请求两次 `curl -si "http://localhost:${API_PORT:-5000}/api/zones"`，可查看 `X-Cache` 命中状态。`CACHE_VERSION` 控制缓存版本。
- 设置 `API_TIMING_HEADER=1` 后重建 API 容器，可通过 `X-Response-Time-Ms` 观察请求耗时。`API_STRICT_MYSQL=1` 让读库失败返回 HTTP 503。
- 演示登录默认关闭。仅在本地演示工单写操作时启用 `AUTH_DEMO_ENABLED=1`。大屏与 API 默认同源，分开部署时通过 `CORS_ALLOWED_ORIGINS` 指定前端来源。
- 用 `docker compose port api 5000` 查看映射端口，用 `docker compose logs --tail=100 api mysql` 排查启动问题。用 `docker compose --profile cache down` 停止服务，MySQL 数据保存在宿主目录。

### 后端与测试

Docker 使用 Gunicorn 加载 `app:app`。默认使用一个 worker，以兼容文件演示模式中的进程内状态。读接口、工单和运维路由位于 `api/routes/`。公共请求处理位于 `api/utils/request_policy.py`，负责 CORS、计时、安全响应头和 1 MiB 请求体上限。超限请求返回 HTTP 413，未处理的 API 异常在服务端记录，客户端收到 HTTP 500 和 JSON 错误信息。

在已激活的 Python 虚拟环境中运行文件模式测试：

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q tests
```

GitHub Actions 在 Python 3.11 和 3.12 上运行测试，用 Playwright 验证大屏页面，并通过 Docker Compose 验证 MySQL 数据读取、工单写入与 Redis 缓存命中。Airflow 的独立启动流程见 [调度说明](docs/orchestration_design.md)。

## 4. 截图

| 页面 | 截图 |
|------|------|
| 总览 | ![overview](docs/screenshots/overview.png) |
| 片区 | ![zones](docs/screenshots/zones.png) |
| 片区（质检被拦的日子） | ![zones_blocked_day](docs/screenshots/zones_blocked_day.png) |
| 基线 | ![baseline](docs/screenshots/baseline.png) |
| 基线与预测 | ![baseline_forecast](docs/screenshots/baseline_forecast.png) |
| 地图 | ![map](docs/screenshots/map.png) |
| 选址补桩（11 个片区共建议加 15 根桩，排第一的是 TAZ 645） | ![site_suggest](docs/screenshots/site_suggest.png) |
| 告警 | ![alerts](docs/screenshots/alerts.png) |
| 已关闭告警 | ![alerts_closed](docs/screenshots/alerts_closed.png) |
| 数据质量 | ![quality](docs/screenshots/quality.png) |
| 数据质量（清洗前后对比） | ![quality_before_after](docs/screenshots/quality_before_after.png) |
| 调度运行（每天每一步的耗时和状态） | ![runs_day](docs/screenshots/runs_day.png) |
| 工单 | ![tickets](docs/screenshots/tickets.png) |
| 工单（管理员，1280 宽） | ![tickets_admin_1280](docs/screenshots/tickets_admin_1280.png) |
| 工单时间线（WO-0007 派单后等了 27 分钟才开始处理，处理用了 2 小时 45 分钟） | ![ticket_timeline](docs/screenshots/ticket_timeline.png) |
| 数据关系（六列分层，点节点标亮上下游） | ![lineage](docs/screenshots/lineage.png) |
| 实时占用回放 | ![realtime](docs/screenshots/realtime.png) |
| 实时回放关键帧（约第 5 / 90 / 170 帧） | ![realtime_f5](docs/screenshots/realtime_f5.png) ![realtime_f90](docs/screenshots/realtime_f90.png) ![realtime_f170](docs/screenshots/realtime_f170.png) |

实时页回放录屏（2022-09-02，288 倍速，3 分钟）：[docs/media/realtime_replay.mp4](docs/media/realtime_replay.mp4)

## 5. 相关文档

| 文档 | 内容 |
|------|------|
| `docs/architecture.md` | 整体架构、测试与预测 |
| `docs/orchestration_design.md` | Airflow DAG 与质检门 |
| `docs/data_lake_dictionary.md` | 数据湖各表字段、分区与重新生成方法 |
| `docs/warehouse_design.md` | Hive / MySQL 表设计 |
| `docs/perf/spark_baseline.md` | Spark 耗时与索引性能 |
| `docs/api_contract.md` | 接口约定 |
| `docs/lineage.md` | 表级数据关系（`/api/lineage`） |

---

## 9. 数据来源与许可

演示数据来源于 UrbanEV 公开数据集（深圳充电站运行记录，5 分钟粒度，许可为 CC0）。数据集主页：[Dryad doi:10.5061/dryad.np5hqc04z](https://datadryad.org/dataset/doi:10.5061/dryad.np5hqc04z)；配套代码：[IntelligentSystemsLab/UrbanEV](https://github.com/IntelligentSystemsLab/UrbanEV)。本仓库仅收录按规则选取的演示子集元数据与必要的小体积聚合结果；完整原始文件请自行按数据集官方说明获取，勿将大体量原始 CSV 强行推入本仓库。
