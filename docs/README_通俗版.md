# 这个项目你怎么看（白话说明）

仓库 GitHub 主页用根目录正式版 `README.md`。下面这份是给你自己读的对照说明。

---

## 这是干什么的？

正式题名：**基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现**

一句话：用深圳公开充电站数据（UrbanEV），挑 **25 个站、14 天**，走完「取数 → 清洗 → 分层入库 → 算指标 → 做大屏」，方便毕设答辩和投简历时讲清楚你做了什么。

技术用 Python + Flask + MySQL + ECharts（写在技术路线里，**不写进题名**）。

仓库：https://github.com/inkrhymes/sz-ev-charging

---

## 做了什么 / 没做什么

**已经做了**
- 25 站 × 14 天，共 **100,800** 行
- 质检有实测数字：缺失率 **26.42%**、卡死 **4** 站、高缺失 **6** 站（见 `docs/quality_report.md`）
- MySQL 三层：ods（贴源）→ dwd（清洗明细）→ ads（指标）
- Flask 接口 + ECharts 大屏
- 一键启动：`scripts/start.sh`（已在有 Docker 的机器实跑通过）

**开题里留的三件「薄皮」（能演示，不是完整业务系统）**
- 工单：列表里改一下状态
- 只读登录：演示用
- 朴素基线：用「上一时刻」对比，不是 LSTM

**明确不做**
真爬虫、Hive/Spark、React 重前端、真预测模型。

---

## 你怎么自己跑起来看

电脑先装好 Docker，然后：

```bash
git clone https://github.com/inkrhymes/sz-ev-charging.git
cd sz-ev-charging
cp .env.example .env
python3 -m venv .venv
.venv/bin/pip install -r etl/requirements.txt -r api/requirements.txt
./scripts/start.sh
```

跑完后打开：
- 探活：http://localhost:5000/api/health
- 总览：http://localhost:5000/api/overview
- 大屏：仓库里的 `web/index.html`

关掉：`./scripts/stop.sh`  
连数据库文件一起清：`./scripts/stop.sh --purge`

---

## 面试 / 简历可报的硬数字

| 项 | 数 |
|----|----|
| 站点 / 片区 | 25 / 23 |
| 时间窗 | 2022-09-01～09-14 |
| 行数 | 100,800 |
| 整行缺失率 | 26.42% |
| 卡死站 / 高缺失站 | 4 / 6 |

---

## 现在还没做完的（对外别说「全验收完了」）

1. 正式录屏视频（口播稿有了，片子还没有）
2. 整包拷到你自己的电脑（电脑上线后再传）
3. 简历里「部署」那句，等联调口径定了再补

有问题直接在「项目履历」群里 @ 对应模块的人，或私聊我。
