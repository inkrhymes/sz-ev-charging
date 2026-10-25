# Spark 离线数仓性能基线

环境：单机 `local[2]`，driver 内存 2g，PySpark 3.5.3，Temurin JRE 17，Hive 内嵌 Derby Metastore，无 Hadoop 集群。
数据：1682 站、331 个片区（TAZ），5 分钟粒度，日期 2022-09-01 起。
口径：下表行数与耗时全部取自作业日志 `spark/_runs/perf.jsonl`。耗时为单步墙钟时间（秒），“总耗时”含 JVM 启动。

## 1. DAG 入口（ads_build，直接读上游 dwd/charge_5min_clean），ds=2022-09-01

输出写入 `/workspace/sz-ev-charging/data/lake/ads/`。

| 步骤 | 行数 | 耗时(s) |
|---|---:|---:|
| 外表注册 dwd.ext_charge_5min_clean，当日行数 | 484,416 | 2.52 |
| dwd.zone_hour | 7,760 | 1.75 |
| dwd.quality_station_day | 1,682 | 1.49 |
| ads.zone_occupancy_hour | 7,760 | 0.32 |
| ads.baseline_naive | 7,760 | 0.55 |
| ads.city_overview | 1 | 0.67 |
| ads.zone_daily | 331 | 0.56 |
| ads.quality_summary（卡死站数按全部已注册分区统计） | 4 | 10.45 |
| 湖输出 ads/zone_hour/dt=D | 7,760 | 0.46 |
| 湖输出 ads/zone_daily/dt=D | 331 | 0.25 |
| **总耗时** | | **26.6** |

时区说明：上游 ts 是不带时区的 Parquet 时间戳。如果用 Hive SerDe 的 TIMESTAMP 外表读取，会偏移 8 小时，所以外表改为 Spark 数据源外表（推断为 TIMESTAMP_NTZ）。湖输出的 ts_hour 也写成 NTZ。已核对：pandas 读出的范围是 2022-09-01 00:00 到 23:00。

export_ads_mysql.py --ds 2022-09-01，真实写回 sz_ev，总耗时 8.5s：

| 表 | 写入行数 | 耗时(s) | 写回后库内核对 |
|---|---:|---:|---|
| ads_city_overview | 1 | 3.98 | 当日 1 行 |
| ads_zone_occupancy_hour | 7,760 | 0.39 | 当日 7,760；全表 12,885（原 5,554 − 当日 demo 429 + 7,760） |
| ads_baseline_naive | 7,760 | 0.32 | 当日 7,760 |
| ads_zone_daily | 331 | 0.22 | 当日 331 |
| dim_station | 1,682 | 0.19 | 1,682，经纬度非空 1,682 |
| ads_quality_summary | 4 | 0.20 | 4 个指标被覆盖，旧的 demo_station_cnt 行保留 |

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

### 2.1 全区间 181 天（2022-09-01..2023-02-28，--dry-run）

| 层 / 表 | 行数 | 耗时(s) |
|---|---:|---:|
| ods.station_info | 1,682 | 1.74 |
| ods.charge_5min | 87,679,296 | 69.93 |
| dwd.station_5min_clean | 87,679,296 | 176.75 |
| dwd.zone_hour | 1,338,568 | 26.59 |
| dwd.quality_station_day | 304,442 | 46.14 |
| ads.zone_occupancy_hour | 1,338,568 | 7.14 |
| ads.baseline_naive | 1,338,568 | 6.81 |
| ads.city_overview | 181 | 5.34 |
| ads.zone_daily | 59,911 | 4.32 |
| ads.quality_summary | 4 | 9.39 |
| 导出 CSV：zone_occupancy_hour / baseline_naive | 1,338,568 / 1,338,568 | 7.81 / 9.52 |
| 导出 CSV：zone_daily / city_overview / dim_station / quality_summary | 59,911 / 181 / 1,682 / 4 | 0.91 / 0.56 / 0.06 / 0.14 |
| **总耗时** | | **390.4（6m30s）** |

全区间质量汇总：卡死站 6，行缺失率 0.211628，高缺失站 259，站点 1,682。warehouse 体积：ods 66M、dwd 161M、ads 46M。全程 driver 2g，任务结束后没有残留 Spark 进程。

## 3. Demo 数据（25 站）

| 范围 | ods | dwd | zone_hour | 总耗时 |
|---|---:|---:|---:|---:|
| 1 天 | 7,200 | 7,200 | 429 | 约 32s |
| 14 天 | 100,800 | 100,800 | 5,554（与 pandas 实现一致） | 37.9s；重跑 32.8s，行数一致 |

## 4. 说明

- 单日作业耗时主要是 JVM 与 Spark 启动开销，计算本身只占几秒。增加天数后耗时增长很慢。
- quality_summary 的卡死统计原先每次扫描全部已注册分区，是 ads_build 中最慢的一步；现已改为增量（见 §6）。

## 5. MySQL 关键查询：加索引前后

测试条件：sz_ev 库，ads_zone_occupancy_hour 共 12,885 行，ads_baseline_naive 共 7,760 行。每条查询先预热 5 次，再计时 50 次，取中位数和 P95（毫秒）。客户端是本机 pymysql。脚本逻辑与 API 查询口径一致；当前 API 容器运行在 file 模式（/api/health 返回 mode=file），并不直接查询这些表。

| 查询 | 加索引前 中位/P95 | 执行计划 | 仅 (ts_hour) 单列索引 | 覆盖索引后 中位/P95 | 执行计划 |
|---|---|---|---|---|---|
| Q1 城市小时曲线（当日，跨片区） | 4.515 / 4.975 | 全表扫描 | 9.518 / 11.094（回表，变慢） | 1.989 / 2.346 | range idx_zoh_ts_occ |
| Q2 基线当日平均误差 | 1.797 / 3.057 | 全表扫描 | 1.399 / 1.731（未用索引） | 1.396 / 1.779 | index idx_bl_ts_err |
| Q3 单片区时间序列 | 0.217 / 0.771 | 主键 range | 0.221 / 0.484 | 0.200 / 0.255 | 主键 range |
| Q4 地图片区日汇总 | 1.195 / 1.621 | 主键 ref | 1.148 / 1.563 | 1.802 / 2.373 | 主键 ref（未改动） |
| Q5 片区内站点 | 0.139 / 0.166 | idx_dim_station_taz | 0.146 / 0.209 | 0.195 / 0.422 | 同左（未改动） |

最终保留 `sql/ads/05_ads_time_indexes.sql` 中的两个覆盖索引。Q1 加速约 2.3 倍，Q2 变化不大。Q3–Q5 的索引没有改动，前后差异属于测量波动。数据量只有万行级别，绝对耗时都在毫秒级；全区间约 134 万行时差异会更明显，但这次没有实测。

## 6. quality_summary 增量化前后（ads_build --ds 2022-09-01，真实湖 181 个 dwd 分区，local[2] / driver 2g）

做法：新增分区表 ads.station_busy_day（站×日：有效读数条数、min/max busy）。每次只重算本次 --ds 以及该表里还缺的源分区；quality_summary 只读这张汇总表，不再扫 dwd 明细。卡死判定从 COUNT(DISTINCT busy)=1 改为 SUM(valid_cnt)≥12 且 MIN(min_busy)=MAX(max_busy)，口径不变。

| 场景 | station_busy_day 重算天数 / 耗时(s) | quality_summary 耗时(s) | ads_build 总墙钟(s) |
|---|---|---|---|
| 改前（全量扫明细） | — | 7.87–10.45 | 约 22–27 |
| 改后首跑（一次性回补 181 天） | 181 / 6.35 | 1.84 | 25 |
| 改后日常增量（仅 ds 当天） | 1 / 0.48 | 2.05 | 16 |

结果校验（与改前一致）：stuck_station_cnt 6，row_missing_rate 0.114230，high_missing_station_cnt 180，station_cnt 1682。
