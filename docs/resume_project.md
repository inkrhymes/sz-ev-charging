# 简历项目描述

**深圳市电动汽车充电站运行管理可视化系统**｜个人项目｜Python / PySpark / Hive / Flask / MySQL / ECharts / Airflow / Kafka / Redis / LightGBM  
仓库：https://github.com/inkrhymes/sz-ev-charging

- 基于公开数据集 UrbanEV（深圳 1,682 站、181 天、约 8,768 万行 5 分钟数据），搭建「Parquet 数据湖 → PySpark/Hive 指标 → MySQL → Flask API → ECharts 大屏」全链路，覆盖采集、清洗、质检阻断、调度与可视化。
- 全量入湖约 2 分 03 秒（5 GB CSV → 约 82 MB ODS Parquet）；Spark 产出小时占用等 ads 指标，与 MySQL 对齐 1,338,568 行；Airflow 日调度在 ods/dwd/ads 设质检门，失败日保留上次发布并写告警（如 2022-10-27）。
- 下一小时 LightGBM 片区占用率 MAE 0.0255，优于「沿用上一小时」0.0285；日前 24 小时预测 MAE 0.0626，优于沿用末值与照抄昨日同时刻；Kafka 单日回放约 48.4 万条、流计算占用率 0.273 对齐离线 0.271。
- Flask 提供总览、片区、预测、地图、告警、工单、调度运行、选址补桩、数据关系等页面；可选 Redis 读缓存后压测约 329→1387 QPS、P50 约 150→33 ms；CI 145 通过 / 6 跳过，Docker Compose 一键可复现。
