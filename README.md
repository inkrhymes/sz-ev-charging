# 基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现

本仓库为上述课题的**演示实现**，面向简历展示与开题薄皮验收。系统完成 UrbanEV 深圳充电站子集的数据接入、质量清洗、MySQL 分层建模、指标汇总，以及基于 Flask 与 ECharts 的运行管理可视化大屏。

## 一、系统目标

1. 对充电站 5 分钟粒度运行数据进行清洗与质量评估，识别卡死站与高缺失站。
2. 按 ODS → DWD → ADS 分层落库，形成可复现的指标表。
3. 通过薄层 API 与单页大屏展示站点概况、质量分布、片区时序及朴素基线对比。
4. 保留开题所述的极简工单、只读登录与上一时刻基线三类薄皮能力（stub）。

## 二、技术路线

| 层次 | 技术 |
|------|------|
| 数据源 | UrbanEV 开源数据集（CC0），演示子集 25 站、14 日窗 |
| 处理 | Python ETL（造数/清洗/质检） |
| 存储 | MySQL 8.0（ods / dwd / ads） |
| 服务 | Flask REST API |
| 前端 | ECharts 单页大屏 |
| 编排 | Docker Compose + `scripts/start.sh` |

说明：Flask、MySQL、ECharts 仅作为技术实现手段，不写入课题题名。

## 三、演示范围与边界

**已实现主路径**

- UrbanEV 25 站演示子集 → 清洗与质检 → MySQL 分层 → 核心指标 SQL → Flask API → ECharts 大屏

**薄皮 stub（可演示、非生产）**

- 极简运维工单、只读演示登录、上一时刻 naive 基线曲线

**明确不做**

- 对抗式真实爬虫、Hive/Spark、React/Vue 重前端、LSTM 等真实预测模型

## 四、仓库结构

| 路径 | 说明 |
|------|------|
| `sql/ods` `sql/dwd` `sql/ads` | 分层建表与核心指标 SQL |
| `etl/` | 子集生成、清洗、MySQL 装载脚本 |
| `api/` | Flask 薄 API |
| `web/` | ECharts 可视化大屏 |
| `stubs/` | 工单 / 登录 / 基线 stub |
| `data/demo/` | 演示子集（原始全量不入库；大 CSV 见 `.gitignore`） |
| `scripts/start.sh` `scripts/stop.sh` | 一键启动与停止 |
| `docs/` | 字段字典、质检报告、API 约定、录屏脚本、简历项目描述等 |
| `docker-compose.yml` | MySQL 与 API 服务编排 |

## 五、环境要求

- Docker / Docker Compose
- Python 3.10+（本地装载与 ETL；建议使用仓库内 `.venv`）
- 可选：复制 `.env.example` 为 `.env` 修改端口与数据库口令

## 六、快速启动

```bash
git clone https://github.com/inkrhymes/sz-ev-charging.git
cd sz-ev-charging
cp .env.example .env          # 可按需修改
bash scripts/start.sh         # MySQL → 建表 → 装载 → API
```

启动成功后：

- API 健康检查：`http://localhost:5000/api/health`
- 概览指标：`http://localhost:5000/api/overview`
- 可视化大屏：浏览器打开 `web/index.html`（或经 API 静态挂载路径访问）

停止服务：

```bash
bash scripts/stop.sh          # 保留数据卷
bash scripts/stop.sh --purge  # 同时删除数据卷
```

> 说明：`docker compose` 仅挂载 `sql/` 根目录初始化文件；分层建表与装载由 `scripts/start.sh` 顺序执行。API 文件模式需挂载仓库根目录（见 `docker-compose.yml` 中 `REPO_ROOT`）。

## 七、演示数据摘要（实测）

| 项目 | 数值 |
|------|------|
| 站点 / 片区 | 25 站 / 23 个 TAZ |
| 时间窗 | 2022-09-01 ～ 2022-09-14（5 分钟粒度） |
| 行数 | 100,800 |
| 整行缺失率 | 26.42% |
| 疑似卡死站 / 高缺失站 | 4 / 6 |

详细站级结果见 `docs/quality_report.md`。

## 八、验收口径

1. **能跑**：`scripts/start.sh` 可完成 MySQL 健康检查、ADS 回填与 API 响应。
2. **能录**：按 `docs/demo_script.md` 完成 1～2 分钟大屏演示录屏。
3. **能量化**：质检数字、分层表与大屏指标一致；三类 stub 可点击演示。

## 九、相关文档

- `docs/data_dictionary.md` — 数据字典  
- `docs/api_contract.md` — API 约定  
- `docs/quality_report.md` — 质检报告  
- `docs/resume_project.md` — 简历项目描述草稿  
- `docs/schedule_15d.md` — 15 日推进计划  

## 十、许可说明

UrbanEV 原始数据遵循其开源许可（CC0）。本仓库仅收录演示所需的小规模子集与衍生表；完整原始 CSV 不纳入版本库。
