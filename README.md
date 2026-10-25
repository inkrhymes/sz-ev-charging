# 深圳市电动汽车充电站运行管理可视化系统（演示版）

毕设对齐题名：**基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现**

范围锁定（简历主路径 + 开题薄皮，约 15 天）：
- 主线：UrbanEV 子集 / 造数 → 清洗 → MySQL 分层（ods/dwd/ads）→ 指标 SQL → 薄 Flask → ECharts 大屏 → 录屏
- 薄皮 stub：极简运维工单、只读演示登录、上一时刻 naive 基线曲线
- 不做：真爬虫对抗、Hive/Spark、React/Vue 重前端、LSTM/真预测模型

## 目录

| 路径 | 说明 | 负责人 |
|------|------|--------|
| `sql/ods` `sql/dwd` `sql/ads` | 分层建表与核心指标 SQL | 小黑 |
| `docs/data_dictionary.md` | 字段字典 | 小黑 |
| `etl/` | 造数 / 清洗 ETL + 质检 | 小紫 |
| `api/` | Flask 薄 API（6～10） | 小粉 |
| `web/` | ECharts 单页大屏 | 小黄 |
| `stubs/tickets` `stubs/auth` `stubs/baseline` | 开题薄皮三件 | 联调 |
| `data/demo/` | UrbanEV 25 站演示子集（外挂，不进大仓） | — |
| `docs/demo_script.md` | 1～2 分钟录屏脚本 | 小黄 |
| `docker-compose.yml` | MySQL + API 一键起 | 小白 |

## 一键启动（骨架占位）

```bash
cp .env.example .env
docker compose up -d
# API 默认 http://localhost:5000  大屏见 web/index.html
```

## 验收口径

能跑、能录、能量化：质检数字 + 分层表 + 大屏指标 + 三件 stub 可点。
