# 基于大数据技术的深圳市电动汽车充电站运行管理可视化系统

本仓库为实现「基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现」的可运行演示工程，面向毕业设计验收与技术简历展示。系统覆盖数据采集、数据清洗、分层入库、指标计算与可视化大屏，技术栈为 Python、MySQL、Flask 与 ECharts。

仓库地址：https://github.com/inkrhymes/sz-ev-charging

## 一、系统简介

本系统基于公开数据集 UrbanEV（深圳市电动汽车充电站运行数据，5 分钟粒度，CC0 许可），选取 25 个站点构成演示子集，完成以下闭环：

1. 从原始数据生成演示子集并写入 ODS 层；
2. 清洗与质检后落入 DWD 层，并汇总 ADS 层运行指标；
3. 通过 Flask 提供 REST 接口；
4. 使用 ECharts 单页大屏展示城市概览、片区时序、数据质量与运营辅助信息。

演示子集实测口径（详见 `docs/quality_report.md`）：

| 项目 | 数值 |
|------|------|
| 站点数量 | 25 |
| 片区（TAZ）数量 | 23 |
| 数据时间窗 | 2022-09-01 ～ 2022-09-14 |
| 清洗后记录数 | 100,800 |
| 整行缺失率 | 26.42% |
| 疑似卡死站点 | 4 |
| 缺失率超过 30% 的站点 | 6 |

## 二、功能模块

| 模块 | 说明 |
|------|------|
| 数据采集 | 基于 UrbanEV 公开数据生成演示子集（非线上实时爬虫） |
| 数据清洗与质检 | 缺失、卡死、高缺失站点识别，输出质检报告与工单建议 |
| 数据分层存储 | MySQL 中按 ODS / DWD / ADS 三层建表与装载 |
| 运行分析 | ADS 层城市概览、片区小时占用、质量汇总等指标 |
| 可视化大屏 | KPI、时序曲线、质检面板、朴素基线对比 |
| 运营调度辅助（演示） | 工单列表与状态流转 Stub |
| 系统管理（演示） | 只读演示登录 Stub |

说明：预测类能力当前仅保留上一时刻（lag-1）朴素基线曲线，不包含 LSTM 等深度学习模型。

## 三、技术架构

```
UrbanEV 原始数据
    → etl/generate_demo_subset.py（演示子集）
    → etl/load_ods.py / clean_to_dwd.py（ODS → DWD）
    → etl/load_mysql_from_demo.py（装载 MySQL，回填 ADS）
    → Flask API（api/app.py）
    → ECharts 大屏（web/index.html）
```

- 数据层：MySQL 8.0（ODS / DWD / ADS）
- 服务层：Flask（默认文件模式读取 `data/demo/dwd`，亦可对接 MySQL）
- 展示层：原生 HTML + ECharts
- 编排：Docker Compose + `scripts/start.sh`

## 四、目录结构

| 路径 | 说明 |
|------|------|
| `etl/` | 演示子集生成、清洗、质检与 MySQL 装载脚本 |
| `sql/ods`、`sql/dwd`、`sql/ads` | 分层建表与核心指标 SQL |
| `api/` | Flask 接口服务 |
| `web/` | 可视化大屏页面 |
| `stubs/` | 工单、登录、朴素基线等演示 Stub |
| `data/demo/` | 演示子集与 DWD 小文件（大体积原始 CSV 不纳入版本库） |
| `scripts/` | 一键启动与停止脚本 |
| `docs/` | 数据字典、接口约定、质检报告、录屏脚本、简历项目描述等 |
| `docker-compose.yml` | MySQL 与 API 容器编排 |

## 五、环境要求

- Docker 与 Docker Compose
- Python 3.10+（首次装载需本地虚拟环境执行装载脚本）
- Git

## 六、快速启动

```bash
git clone https://github.com/inkrhymes/sz-ev-charging.git
cd sz-ev-charging
cp .env.example .env
python3 -m venv .venv
.venv/bin/pip install -r etl/requirements.txt -r api/requirements.txt
bash scripts/start.sh
```

启动完成后：

- 接口服务：http://localhost:5000
- 健康检查：http://localhost:5000/api/health
- 可视化大屏：http://localhost:5000/（由 API 托管 `web/index.html`）

停止服务：

```bash
bash scripts/stop.sh          # 停止容器，保留数据卷
bash scripts/stop.sh --purge  # 停止并删除数据卷
```

说明：`scripts/start.sh` 会依次启动 MySQL、执行分层建表、装载演示 CSV 并回填 ADS，最后构建并启动 API。本机已实测可通过该脚本完成联调。

## 七、主要接口

统一响应格式：`{ "ok": true, "data": ... }`

| 方法 | 路径 | 用途 |
|------|------|------|
| GET | `/api/health` | 服务探活 |
| GET | `/api/overview` | 城市概览 KPI |
| GET | `/api/zone/series` | 片区时序 |
| GET | `/api/quality` | 数据质量汇总 |
| GET | `/api/baseline` | 朴素基线曲线 |
| GET | `/api/tickets` | 工单列表 |
| PATCH | `/api/tickets/<id>` | 工单状态更新（演示） |
| POST | `/api/auth/login` | 演示登录 |

完整约定见 `docs/api_contract.md`。

## 八、文档索引

| 文档 | 内容 |
|------|------|
| `docs/data_dictionary.md` | 字段与分层命名 |
| `docs/quality_report.md` | 演示子集质检实测结果 |
| `docs/api_contract.md` | 接口约定 |
| `docs/demo_script.md` | 演示录屏口播脚本 |
| `docs/resume_project.md` | 简历项目描述草稿 |
| `docs/schedule_15d.md` | 十五日推进节奏 |

## 九、数据与仓库约定

- 版本库仅包含代码、文档及演示所需的小型 DWD 文件。
- UrbanEV 全量原始数据及体积较大的清洗 CSV（如 `dwd_station_5min_clean.csv`）由 `.gitignore` 排除，不提交至远程仓库。
- 缺少本地 DWD 大文件时，可在配置好原始数据路径后，通过 `etl/` 脚本重新生成。

## 十、开发与提交

本仓库默认分支为 `main`。日常开发请在 `/workspace/sz-ev-charging`（或本地克隆目录）中修改后执行：

```bash
git add -A
git commit -m "简要说明本次变更"
git push origin HEAD:main
```

请勿对已共享的 `main` 历史使用强制推送。

## 十一、范围说明

本演示工程刻意控制在可交付周期内，明确不做以下内容：线上实时爬虫对抗、Hive / Spark 集群、React / Vue 重型前端、LSTM 等真实预测模型。相关能力如需扩展，可在现有 Stub 与 ADS 指标之上继续迭代。
