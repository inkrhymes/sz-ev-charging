# Spark 离线数仓性能基线

环境：单机 `local[2]`，driver 内存 2g，PySpark 3.5.3，Temurin JRE 17，Hive 内嵌 Derby Metastore，无 Hadoop 集群。
数据：1682 站、331 个片区（TAZ），5 分钟粒度，日期 2022-09-01 起。
口径：下表行数与耗时全部取自作业日志 `spark/_runs/perf.jsonl`。耗时为单步墙钟时间（秒），“总耗时”含 JVM 启动。

## 1. DAG 入口（ads_build，直接读上游 dwd/charge_5min_clean），ds=2022-09-01

| 步骤 | 行数 | 耗时(s) |
|---|---:|---:|
| ods.station_info 装载 | 1,682 | 9.73 |
| 外表注册 dwd.ext_charge_5min_clean（共 181 个分区），当日行数 | 484,416 | 3.01 |
| dwd.zone_hour | 7,760 | 3.37 |
| dwd.quality_station_day | 1,682 | 2.76 |
| ads.zone_occupancy_hour | 7,760 | 0.59 |
| ads.baseline_naive | 7,760 | 1.07 |
| ads.city_overview | 1 | 1.27 |
| ads.zone_daily | 331 | 1.04 |
| ads.quality_summary（卡死站数按全部已注册分区统计） | 4 | 13.56 |
| 湖输出 ads/zone_hour/dt=D | 7,760 | 0.74 |
| 湖输出 ads/zone_daily/dt=D | 331 | 0.46 |
| **总耗时** | | **41.7** |

export_ads_mysql.py --ds 2022-09-01 --dry-run：6 张表共 17,538 行，总耗时 12.2s。

## 2. 自建链路（ods_load → dwd_clean → ads_metrics），全量湖

| 层 / 表 | 1 天（09-01） | | 7 天（09-01..09-07） | |
|---|---:|---:|---:|---:|
| | 行数 | 耗时(s) | 行数 | 耗时(s) |
| ods.charge_5min | 484,416 | 2.00 | 3,390,912 | 3.91 |
| dwd.station_5min_clean | 484,416 | 5.70 | 3,390,912 | 8.53 |
| dwd.zone_hour | 7,760 | 1.12 | 54,168 | 2.07 |
| dwd.quality_station_day | 1,682 | 1.27 | 11,774 | 2.74 |
| ads.zone_occupancy_hour | 7,760 | 2.23 | 54,168 | 2.59 |
| ads.baseline_naive | 7,760 | 0.83 | 54,168 | 1.05 |
| ads.city_overview | 1 | 1.08 | 7 | — |
| ads.zone_daily | 331 | 0.83 | 2,317 | 0.90 |
| **总耗时** | | **36.4** | | **42.3** |

7 天质量汇总：卡死站 60，行缺失率 0.126578，高缺失站 211。
`--from-lake-dwd` 跳过清洗（ds=2022-09-02）：外部 dwd 484,416 行用时 3.29s，zone_hour 7,761 行，总耗时 22.2s。
7 天后 warehouse 体积：ods 3.7M、dwd 5.5M、ads 1.5M。

全区间 181 天（2022-09-01..2023-02-28）：ods.charge_5min 87,679,296 行，用时 69.93s。后续各层见补充记录。

## 3. Demo 数据（25 站）

| 范围 | ods | dwd | zone_hour | 总耗时 |
|---|---:|---:|---:|---:|
| 1 天 | 7,200 | 7,200 | 429 | 约 32s |
| 14 天 | 100,800 | 100,800 | 5,554（与 pandas 实现一致） | 37.9s；重跑 32.8s，行数一致 |

## 4. 说明

- 单日作业耗时主要是 JVM 与 Spark 启动开销，计算本身只占几秒。增加天数后耗时增长很慢。
- quality_summary 的卡死统计扫描全部已注册分区，是 ads_build 中最慢的一步。
- 尚未做“查询优化前后”的对照实验，本文只给出基线。
