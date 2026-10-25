# 基于大数据技术的深圳市电动汽车充电站运行管理可视化系统

面向城市公共充电站运维与管理场景的演示系统。系统以公开数据集 [UrbanEV](https://www.nature.com/articles/s41597-025-04874-4)（深圳市公共充电站运行记录，CC0）为数据基础，完成多源数据导入、清洗与质量评估、分层存储、指标计算与可视化查询，并为运维工单与只读权限预留可演示接口。

**仓库地址：** https://github.com/inkrhymes/sz-ev-charging

## 1. 项目背景

随着公共充电设施规模扩大，运营方更关注原始数据是否可靠、脏数据如何处理、片区忙闲如何呈现，以及异常设备如何进入检修流程。本项目在本科毕业设计开题范围内，将「数据采集 → 清洗与质量评估 → 运行分析 → 可视化查询 → 运营调度辅助 → 系统管理」六类能力收敛为一套可本地一键启动的演示链路，便于答辩展示与技术面试说明。

## 2. 功能概览

| 模块 | 能力说明 |
|------|----------|
| 数据导入与清洗 | 按固定 25 站演示子集抽取 UrbanEV 5 分钟原始记录，完成缺失标记、卡死识别、聚合与质量报告输出 |
| 分层存储 | MySQL 按 ODS / DWD / ADS 分层建表，支持从演示 CSV 装载并回填概览、质检、工单等指标 |
| 运行分析 | 片区小时占用率序列；以上一时刻值作为朴素基线，用于对照展示（不做深度学习训练） |
| 可视化查询 | ECharts 单页大屏：KPI、占用时序、质检面板、基线对比、工单列表 |
| 运营调度辅助 | 由卡死 / 高缺失规则生成待检修、巡检类工单，支持状态流转演示 |
| 系统管理 | 只读演示登录入口（轻量 stub，非完整权限体系） |

## 3. 技术栈

- 语言与脚本：Python 3
- 存储：MySQL 8.0（ODS / DWD / ADS）
- 后端：Flask（REST API）
- 前端：原生 HTML + ECharts
- 部署：Docker Compose，一键脚本 `scripts/start.sh`

## 4. 演示数据与实测指标

- 站点规模：25 个公共充电站（含卡死与高缺失样本，便于展示质检与工单）
- 时间窗口：2022-09-01～2022-09-14（14 天，约 100,800 行）
- 交通小区（TAZ）：23 个
- 整行缺失率：**26.42%**
- 疑似卡死站：**4**
- 缺失率超过 30% 的站点：**6**
- 工单建议：repair 4 / inspect 6

详细口径见 `docs/quality_report.md`、`docs/data_dictionary.md`。

> 说明：UrbanEV 全量原始数据约数 GB，不纳入本仓库。仓库仅保留站点清单与 API / 大屏所需的小体积 DWD 产物；完整 5 分钟清洗表可按 `etl/` 脚本在本地再生。

## 5. 快速开始

### 5.1 环境要求

- Docker 与 Docker Compose
- Python 3.10+（装载脚本使用；首次运行会创建 `.venv`）

### 5.2 启动

```bash
git clone https://github.com/inkrhymes/sz-ev-charging.git
cd sz-ev-charging
cp .env.example .env
bash scripts/start.sh
```

脚本将依次：启动 MySQL → 执行分层建表 SQL → 装载演示 CSV 并回填 ADS → 构建并启动 API。

- API 默认地址：http://localhost:5000  
- 健康检查：http://localhost:5000/api/health  
- 大屏页面：用浏览器打开 `web/index.html`（优先请求本地 API；不可用时回落本地假数据）

停止服务：

```bash
bash scripts/stop.sh          # 停止容器
bash scripts/stop.sh --purge  # 停止并清除数据卷
```

### 5.3 仅文件模式（不启 MySQL）

若仅需联调大屏与质检数字，可在仓库根目录运行：

```bash
cd api && pip install -r requirements.txt && python app.py
```

默认读取 `data/demo/dwd` 下实测 CSV（`MOCK=1` 可强制假数据）。

## 6. 目录结构

```
sz-ev-charging/
├── api/                 # Flask 接口
├── web/                 # ECharts 可视化页面
├── etl/                 # 子集抽取、清洗、MySQL 装载
├── sql/
│   ├── ods/             # 原始层建表
│   ├── dwd/             # 明细层建表
│   └── ads/             # 应用层建表与指标 SQL
├── data/demo/           # 演示站清单与 DWD 小文件
├── docs/                # 字段字典、质检报告、接口约定、录屏脚本等
├── scripts/             # start.sh / stop.sh
├── docker-compose.yml
└── README.md
```

## 7. 文档索引

| 文档 | 内容 |
|------|------|
| `docs/data_dictionary.md` | 字段与分层表说明 |
| `docs/quality_report.md` | 演示子集质检实测报告 |
| `docs/api_contract.md` | REST 接口约定 |
| `docs/demo_script.md` | 演示录屏口播脚本 |
| `docs/resume_project.md` | 简历项目表述草稿 |
| `docs/schedule_15d.md` | 演示版实施排期 |

## 8. 范围说明

本仓库对应「可演示交付」范围：以 UrbanEV 演示子集跑通清洗、分层、接口与大屏，并对开题中的工单、只读登录、朴素基线保留可点击 stub。

当前版本**不包含**：全量 5.4GB 现场重跑、分片区天气消融全套实验、ARIMA / XGBoost / LSTM 训练、Hive / Spark 集群、React / Vue 重前端，以及生产级权限与电网 EMS 接入。上述内容可按毕业设计进度表在后续阶段扩展。

## 9. 数据来源与许可

UrbanEV 数据集许可为 CC0 公共领域。使用本仓库代码时请保留对原数据论文的引用：

Li H, Qu H, Tan X, et al. UrbanEV: An open benchmark dataset for urban electric vehicle charging demand prediction. *Scientific Data*, 2025, 12: 523.
