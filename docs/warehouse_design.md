# 离线数仓设计（Spark + Hive）

本文说明 `spark/` 目录下离线数仓的分层、分区、幂等、增量与补跑机制，以及它与 MySQL 的分工和单机内存配置。实测数字见 `docs/perf/spark_baseline.md`。

## 1. 总体链路

```
UrbanEV 原始 CSV ──etl/full_pipeline.py──▶ 数据湖（Parquet，按天分区）
                                           $SZ_EV_LAKE/ods/charge_5min/dt=YYYY-MM-DD/
                                           $SZ_EV_LAKE/dwd/charge_5min_clean/dt=YYYY-MM-DD/
                                           $SZ_EV_LAKE/ods/station_info/
                     │
          spark/jobs/ods_load.py   ──▶ Hive ods.charge_5min / ods.station_info
          spark/jobs/dwd_clean.py  ──▶ Hive dwd.station_5min_clean / dwd.zone_hour / dwd.quality_station_day
          spark/jobs/ads_metrics.py──▶ Hive ads.*  ──回写──▶ MySQL ads_* / dim_station ──▶ Flask 接口 ──▶ 前端
```

- 计算引擎：PySpark 3.5（`local[2]`），Spark SQL 完成全部转换。
- 元数据：Hive Metastore。本机使用嵌入式 Derby（`spark/_metastore/`，单进程），生产环境可换为 MySQL Metastore（见 §7）。
- 存储：Hive 表统一为 Parquet，仓库目录默认 `data/warehouse/`（可由 `SZ_WAREHOUSE` 覆盖），不入 Git。

## 2. 分层

| 层 | Hive 表 | 粒度 | 说明 |
|----|---------|------|------|
| ODS | `ods.charge_5min` | 站 × 5 分钟 | 原样装载，统一类型，追加 `src_path`、`load_at` |
| ODS | `ods.station_info` | 站 | 站点维表：经纬度、慢/快桩数、总桩数、片区（TAZ） |
| DWD | `dwd.station_5min_clean` | 站 × 5 分钟 | 打标：整行缺数、读数不动、IQR 异常；计算占用率 |
| DWD | `dwd.zone_hour` | 片区 × 小时 | 片区小时占用率、在网站数、桩数、额定功率合计 |
| DWD | `dwd.quality_station_day` | 站 × 天 | 行数、缺数行数与比例、读数不动、异常行数、最长连续缺数、工单建议 |
| ADS | `ads.zone_occupancy_hour` | 片区 × 小时 | 片区占用率曲线、负荷估计 |
| ADS | `ads.baseline_naive` | 片区 × 小时 | 「用上一小时」简单预测及误差（跨天取前一日最后一小时） |
| ADS | `ads.city_overview` | 天 | 城市日概览 KPI |
| ADS | `ads.zone_daily` | 片区 × 天 | 片区日均占用率与中心点（地图用） |
| ADS | `ads.quality_summary` | 指标 | 全部已入仓分区的质量汇总 |

清洗口径沿用 `etl/clean_to_dwd.py`：

- 整行缺数：`busy` 与 `idle` 同时为空；
- 读数不动：有效点 ≥ 12 且 `busy` 只有一个取值。DWD 行级标记按「站 × 天」计算，以保证单日分区可独立重算；ADS 汇总指标的卡死口径见下文「quality_summary 各指标含义」；
- 异常：按「站 × 天」对 `busy` 做 1.5 × IQR（有效点 ≥ 8 且 IQR > 0），只打标不删行；
- 占用率：`occupancy = busy / total_piles`（非缺数且桩数 > 0）；
- 工单建议：读数不动 → `repair`，缺数比例 > 30% → `inspect`。

若上游已产出清洗分区（`$SZ_EV_LAKE/dwd/charge_5min_clean`），`dwd_clean.py --from-lake-dwd` 直接装载该分区，跳过清洗，只做 DWD 汇总；此时行级 `is_stuck` / `is_outlier` 采用上游按运行窗口计算的结果。

## 3. 分区

- 所有事实表按 `dt STRING`（`YYYY-MM-DD`）分区，与数据湖目录 `dt=YYYY-MM-DD` 及调度参数 `ds` 一一对应。
- 维表 `ods.station_info` 与 `ads.quality_summary` 不分区，每次整表覆盖。
- MySQL 侧按日期列建主键/索引（`ads_zone_daily (dt, taz_id)`、`ads_zone_occupancy_hour (taz_id, ts_hour)` 等），接口只扫描所需日期。

## 4. 幂等

- Hive 写入统一使用 `INSERT OVERWRITE TABLE … PARTITION (dt)`，并开启 `spark.sql.sources.partitionOverwriteMode=dynamic`：只替换本次涉及的分区，其他分区不受影响。同一天重复运行，结果行数不变（实测见 `docs/perf/spark_baseline.md` §3）。
- MySQL 回写：按日期范围 `DELETE` 后批量 `INSERT`，在同一事务内提交；`dim_station` 使用 `REPLACE INTO`；`ads_quality_summary` 是整表快照，先整表 `DELETE` 再 `INSERT`，避免旧指标残留。
- 读取输入时，缺失的日期分区会跳过并打印提示，不会写出空分区覆盖已有数据。

## 5. 增量与补跑

三个作业参数一致，便于接入调度 DAG：

| 参数 | 含义 |
|------|------|
| `--ds YYYY-MM-DD`（同 `--dt`） | 单日增量 |
| `--start D1 --end D2` | 补跑闭区间 |
| `ods_load.py --input PATH` | ODS 分区根目录（默认 `$SZ_EV_LAKE/ods/charge_5min`） |
| `ods_load.py --station-info PATH` | 站点维表（默认 `$SZ_EV_LAKE/ods/station_info` → UrbanEV `station_information.csv` → 演示维表） |
| `dwd_clean.py --from-lake-dwd [--dwd-input PATH]` | 直接装载上游清洗分区 |
| `ads_metrics.py --dry-run` | 不连 MySQL，结果导出 CSV（`data/lake/export/ads/`，或 `SZ_ADS_EXPORT`） |
| `ads_metrics.py --skip-export` | 只计算 Hive ADS 表 |

串行入口：`spark/run_range.sh --ds 2022-09-01 [--dry-run] [--from-lake-dwd]` 或 `--start … --end …`。每一步的行数与耗时追加写入 `spark/_runs/perf.jsonl`。

`ads.baseline_naive` 计算时额外读取起始日前一天的片区小时数据，保证单日增量与整段补跑的结果一致；仅当相邻记录恰好间隔 1 小时才给出预测值。

### 5.1 调度入口

调度器按天调用以下两个脚本：
- `python spark/jobs/ads_build.py --ds D`：直接读上游清洗结果 `$SZ_EV_LAKE/dwd/charge_5min_clean`，注册为外表 `dwd.ext_charge_5min_clean`，输出 Hive ads 表和湖目录 `$SZ_EV_LAKE/ads/zone_hour/dt=D/`、`ads/zone_daily/dt=D/`。
- `python spark/jobs/export_ads_mysql.py --ds D`：把当日 ads 结果回写 MySQL。

如果调度器的解释器里没有 pyspark，脚本会自动切换到 `SZ_PY` 指定的解释器（默认 `/workspace/sz-venv-spark/bin/python`）。JAVA_HOME 默认是 `/workspace/jdk`。

## 6. 与 MySQL 的分工

| 职责 | 位置 |
|------|------|
| 明细存储、清洗、分层计算、历史补跑 | Spark + Hive（Parquet） |
| 页面查询的结果表（小数据量、低延迟、带索引） | MySQL：`ads_city_overview`、`ads_zone_occupancy_hour`、`ads_baseline_naive`、`ads_quality_summary`、`ads_zone_daily`、`dim_station` |
| 工单、告警等可变业务状态 | MySQL（告警表 `dq_alert` 由质检模块负责） |

接口只读 MySQL 结果表，不直接访问数据湖，Spark 只在批处理时运行。

### 6.1 新增 MySQL 表字段字典（`sql/ads/04_ads_zone_daily_dim_station.sql`）

**ads_zone_daily**（主键 `dt, taz_id`）

| 字段 | 类型 | 说明 |
|------|------|------|
| dt | DATE | 日期 |
| taz_id | VARCHAR(32) | 片区编号（UrbanEV TAZID） |
| avg_occupancy | DECIMAL(8,6) | 当日片区小时占用率均值；当日无有效读数时为空 |
| station_cnt | INT | 当日有记录的站点数 |
| pile_cnt | INT | 上述站点总桩数之和 |
| center_lon | DECIMAL(10,6) | 片区中心经度 = 片区内全部站点经度均值 |
| center_lat | DECIMAL(10,6) | 片区中心纬度 = 片区内全部站点纬度均值 |

**dim_station**（主键 `station_id`）

| 字段 | 类型 | 说明 |
|------|------|------|
| station_id | VARCHAR(32) | 站点编号 |
| taz_id | VARCHAR(32) | 片区编号 |
| longitude / latitude | DECIMAL(10,6) | 经纬度，取自 UrbanEV `station_information.csv`（原文件未注明坐标系） |
| slow_count / fast_count | INT | 慢充 / 快充桩数 |
| total_piles | INT | 总桩数（`charge_count`） |
| rated_power_kw | DECIMAL(10,2) | 额定功率；UrbanEV 站点表无此字段，全量下为空 |

说明：`ads_zone_occupancy_hour.load_kw_est = 片区占用率 × 片区额定功率之和`，因全量站点表缺少额定功率，全量运行时该列为空。

## 7. Metastore 选型

- 本机：嵌入式 Derby，零依赖，适合单进程批处理；同一时刻只能有一个 Spark 进程访问，作业须串行执行（`run_range.sh` 即为串行）。
- 生产：MySQL Metastore（独立库 `hive_metastore`），支持多个作业并发访问。配置方法见 `spark/conf/hive-site.xml` 末尾注释，替换 `javax.jdo.option.ConnectionURL` 等四项，并在 classpath 中加入 MySQL JDBC 驱动。

## 8. 内存与运行配置

| 项 | 取值 | 原因 |
|----|------|------|
| master | `local[2]` | 与 MySQL、接口等服务共用 15 GB 内存的单机 |
| spark.driver.memory | 2g（`SZ_DRIVER_MEM` 可改） | 本机剩余可用内存约 6 GB |
| spark.sql.shuffle.partitions | 8 | 数据量为千万级，默认 200 会产生大量小文件 |
| spark.sql.adaptive.enabled | true | 自动合并小分区 |
| Spark UI | 关闭 | 节省内存与端口 |
| Java | Temurin JRE 17（`JAVA_HOME`，默认 `/workspace/jdk`） | 无需 root 安装 |

运行约定：Spark 作业跑完即退出，不常驻；不与调度器、MySQL 大批量导入同时长时间运行。ADS 导出使用流式拉取（`toLocalIterator`），避免 driver 一次性持有全部结果。

## 9. 环境准备

```bash
python3 -m venv /workspace/sz-venv-spark
/workspace/sz-venv-spark/bin/pip install pyspark==3.5.3 pyarrow pymysql pandas
# Java：解压 Temurin JRE 17 到 /workspace/jdk，或设置 JAVA_HOME
python spark/tools/make_demo_lake.py      # 用演示数据生成小样本分区（data/lake/ods/charge_5min）
spark/run_range.sh --start 2022-09-01 --end 2022-09-14 --dry-run
```

数据湖、仓库目录与 Metastore（`data/lake/`、`data/warehouse/`、`spark/_metastore/`、`spark/_runs/`）均已加入 `.gitignore`。


## quality_summary 各指标含义

`ads.quality_summary` / MySQL `ads_quality_summary` 每行一个指标（metric, value, note）。卡死相关的数字和告警、工单用同一份来源：`etl/full_pipeline.py` 产出的站级质量表 `$SZ_EV_LAKE/dwd/quality_station/run=<起>_<止>/`（自动取覆盖天数最多的那次运行，可用 `SZ_QUALITY_STATION` 指定）。Spark 只读这张表，不自己重算。

| metric | 大白话 |
|---|---|
| `station_cnt` | 数仓里有数据的站一共多少座。 |
| `row_missing_rate` | 所有 5 分钟读数里，整行没数据的占多少（按行数加权）。 |
| `high_missing_station_cnt` | 缺数超过 30% 的站有几座。 |
| `stuck_station_cnt` | **累计卡死 ≥30 天的站数**。某站某天有效读数 ≥12 条、但读数从头到尾一个值，就算这天“卡死”；一站累计卡死满 30 天就计入。和 dq_alert 的累计卡死告警、维修工单阈值是同一口径。 |
| `stuck_any_day_cnt` | 半年里至少卡死过一天的站数。 |
| `stuck_whole_window_cnt` | 全程卡死：整个已入仓时间段里读数从没变过的站（以前的 `stuck_station_cnt` 就是这个口径）。由 Spark 用 `ads.station_busy_day` 增量计算。 |
| `repair_ticket_cnt` | 质量表建议派**维修**工单的站数（`ticket_suggest = repair`）。 |
| `inspect_ticket_cnt` | 建议派**巡检**工单的站数（`ticket_suggest = inspect`，缺数多但没到维修）。 |

找不到站级质量表时，后四项里来自质量表的四个指标（`stuck_station_cnt`、`stuck_any_day_cnt`、`repair_ticket_cnt`、`inspect_ticket_cnt`）不输出，不会用别的口径顶替。
`city_overview.stuck_cnt` 是另一回事：当天被标记为卡死的站数（按天统计），不是累计。

`ads_zone_occupancy_hour.load_kw_est`（= 占用率 × 片区额定功率之和）目前全为 NULL：UrbanEV 站点表只有站点位置和慢充/快充桩数，没有额定功率字段，算不出来，所以如实留空，没有用假设功率去估。
