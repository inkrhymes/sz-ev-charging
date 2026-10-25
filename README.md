# 基于大数据技术的深圳市电动汽车充电站运行管理可视化系统

本仓库为本科毕业设计对应演示工程，面向公开数据集 [UrbanEV](https://doi.org/10.1038/s41597-025-04874-4)（深圳电动汽车充电站 5 分钟粒度运行数据，CC0）构建「数据采集 → 清洗 → 分层入库 → 指标分析 → Web 可视化」完整链路，可用于系统演示与技术面试讲解。

正式题名：**基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现**  
技术栈（Flask / MySQL / ECharts）写入实现与技术路线，不纳入题眼。

仓库地址：https://github.com/inkrhymes/sz-ev-charging

---

## 1. 系统功能

| 模块 | 说明 |
|------|------|
| 演示子集构建 | 从 UrbanEV 原始 5 分钟 CSV 中，按锁定规则选取 **25** 个站点、**14** 天窗口切片 |
| 数据清洗与质检 | 识别整行缺失、数值卡死、高缺失站点，输出站级质检表与汇总报告 |
| 分层数据模型 | MySQL 中建设 **ods → dwd → ads** 三层表结构，并由脚本装载演示数据、回填指标 |
| 服务接口 | Flask 提供健康检查、城市总览、片区时序、质检汇总、朴素基线、工单与只读登录等接口 |
| 运行大屏 | 单页 ECharts 大屏展示 KPI、时序、片区汇总、质检面板与基线对比 |
| 一键启停 | `scripts/start.sh` / `scripts/stop.sh` 串联 MySQL、建表、装载、API 与静态大屏 |

开题范围内保留三件轻量能力（可演示、可讲解，非重型业务实现）：运维工单流转、只读演示登录、上一时刻（lag-1）朴素基线曲线。

**明确不做：** 真实平台爬虫对抗、Hive/Spark 集群、React/Vue 重前端、LSTM 等真实预测训练。

---

## 2. 技术栈

- 语言与脚本：Python 3
- 数据存储：MySQL 8.0（Docker）
- 服务端：Flask
- 前端可视化：ECharts（单页 `web/index.html`）
- 编排：Docker Compose、Bash 一键脚本

---

## 3. 数据与质检（演示子集实测）

- **数据窗：** 2022-09-01 00:00 ～ 2022-09-14 23:55（5 分钟粒度）
- **站点：** 25 个（见 `data/demo/demo_station_ids.txt`，禁止另行编造站号）
- **片区（TAZ）：** 23
- **行数：** 100,800

| 指标 | 实测值 |
|------|--------|
| 整行缺失率 | 26.42%（26,631 / 100,800） |
| 疑似卡死站 | 4 |
| 站级缺失率 &gt; 30% | 6 |
| 工单建议 | repair 4 / inspect 6 |

完整说明见 `docs/quality_report.md`、`docs/demo_subset_rules.txt`、`docs/data_dictionary.md`。

仓库为控制体积，**不**纳入 UrbanEV 原始全量与约 8MB 的 `dwd_station_5min_clean.csv`。已纳入仓库、供文件模式 API 直接读取的小文件为：

- `data/demo/dwd/dwd_quality_station.csv`
- `data/demo/dwd/dwd_zone_hour.csv`

本地若具备 UrbanEV 原始路径，可按 `etl/README.md` 重新生成完整 ODS/DWD。

---

## 4. 快速开始

### 4.1 环境要求

- Docker / Docker Compose
- Python 3（用于装载脚本；建议在仓库根目录使用虚拟环境并安装 `etl/requirements.txt`）

### 4.2 配置

```bash
cp .env.example .env
# 按需修改 MySQL 与 API 端口；默认 API 为 http://localhost:5000
```

### 4.3 一键启动（推荐）

```bash
# 建议先准备虚拟环境（start.sh 中装载步骤会调用 .venv/bin/python）
python3 -m venv .venv
.venv/bin/pip install -r etl/requirements.txt -r api/requirements.txt

chmod +x scripts/start.sh scripts/stop.sh
./scripts/start.sh
```

脚本依次完成：启动 MySQL → 按 ods/dwd/ads 执行建表 SQL →（如缺文件则触发 ETL）装载演示 CSV 并回填 ads → 构建并启动 API。

验收探针示例：

```bash
curl -s http://localhost:5000/api/health
curl -s 'http://localhost:5000/api/overview'
```

大屏页面：浏览器打开仓库内 `web/index.html`，或通过 API 容器已挂载的静态资源访问（以当前 Compose 配置为准）。

### 4.4 停止

```bash
./scripts/stop.sh          # 停止容器，保留数据卷
./scripts/stop.sh --purge  # 停止并删除数据卷
```

实测记录（2026-10-25）：`scripts/start.sh` 已在具备 Docker 的环境跑通；ADS 回填与质检口径一致（卡死 4 / 高缺失 6）；API 默认 `mode=file` 时可正常返回 `/api/health`、`/api/overview`。

---

## 5. 目录结构

```
sz-ev-charging/
├── api/                 # Flask 接口
├── web/                 # ECharts 运行大屏
├── etl/                 # 子集生成、ODS/DWD 清洗、MySQL 装载
├── sql/ods|dwd|ads/     # 分层建表与指标 SQL
├── stubs/               # 工单 / 登录 / 朴素基线（轻量能力）
├── data/demo/           # 演示子集清单与小体积 DWD 产物
├── docs/                # 字典、质检、接口约定、录屏稿、简历表述等
├── scripts/start.sh     # 一键启动
├── scripts/stop.sh      # 一键停止
├── docker-compose.yml
└── README.md
```

---

## 6. 接口一览

Base URL：`http://localhost:5000`  
统一成功响应：`{ "ok": true, "data": ... }`

| 方法 | 路径 | 用途 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/overview` | 城市总览 KPI |
| GET | `/api/zone/series` | 片区占用时序 |
| GET | `/api/quality` | 质检汇总 |
| GET | `/api/baseline` | lag-1 朴素基线 |
| GET | `/api/tickets` | 工单列表 |
| PATCH | `/api/tickets/<id>` | 工单状态流转 |
| POST | `/api/auth/login` | 演示登录 |

详见 `docs/api_contract.md`。

---

## 7. 仅跑文件链路（无 Docker）

若只需复现清洗与质检数字：

```bash
python3 etl/generate_demo_subset.py   # 需本机具备 UrbanEV 原始 CSV
python3 etl/load_ods.py
python3 etl/clean_to_dwd.py           # 更新 docs/quality_report.md
```

说明见 `etl/README.md`。

---

## 8. 相关文档

| 文档 | 内容 |
|------|------|
| `docs/quality_report.md` | 质检实测报告 |
| `docs/data_dictionary.md` | 字段与分层字典 |
| `docs/api_contract.md` | 接口约定 |
| `docs/demo_script.md` | 演示录屏口播稿 |
| `docs/resume_project.md` | 简历项目描述草稿 |
| `docs/schedule_15d.md` | 约 15 日推进节奏 |

---

## 9. 数据来源与许可

演示数据来源于 UrbanEV 公开数据集（深圳充电站运行记录，5 分钟粒度，许可为 CC0）。本仓库仅收录按规则选取的演示子集元数据与必要的小体积聚合结果；完整原始文件请自行按数据集官方说明获取，勿将大体量原始 CSV 强行推入本仓库。
