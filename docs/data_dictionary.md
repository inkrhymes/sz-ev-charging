# 字段字典（v0.1 草稿）

对齐开题：**基于大数据技术的深圳市电动汽车充电站运行管理可视化系统设计与实现**  
主数据：UrbanEV 站点级 5 分钟原始（演示子集 20～30 站，含卡死/高缺失）；辅：片区天气（Open-Meteo）、节假日表。  
负责人：小黑。命名约定：`ods_` 贴源、`dwd_` 清洗明细、`ads_` 指标宽表；时间统一 `Asia/Shanghai`。

## 1. ODS（贴源，尽量不改语义）

| 表名 | 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|------|
| ods_station_raw_5min | station_id | VARCHAR(32) | 站点 ID | UrbanEV station CSV 文件名/主键 |
| | ts | DATETIME | 5 分钟时间戳 | 列 `time` |
| | busy | INT | 占用桩数 | `busy` |
| | idle | INT | 空闲桩数 | `idle` |
| | s_price | DECIMAL(10,4) | 服务费 | `s_price` |
| | e_price | DECIMAL(10,4) | 电价 | `e_price` |
| | fast_busy / fast_idle | INT | 快充占用/空闲 | 同名列 |
| | slow_busy / slow_idle | INT | 慢充占用/空闲 | 同名列 |
| | duration | DECIMAL(12,4) | 充电时长相关 | `duration` |
| | volume | DECIMAL(12,4) | 电量相关 | `volume` |
| | src_file | VARCHAR(128) | 源文件名 | ETL 写入 |
| | load_at | DATETIME | 入库时间 | ETL |
| ods_station_info | station_id | VARCHAR(32) | 站点 | UrbanEV inf |
| | taz_id | VARCHAR(32) | 交通小区 | inf / 映射 |
| | lon_gcj / lat_gcj | DOUBLE | GCJ-02 | inf |
| | lon_wgs / lat_wgs | DOUBLE | WGS-84（可空，dwd 补） | ETL |
| | total_piles | INT | 总桩数 | busy+idle 稳态或 inf |
| | rated_power_kw | DECIMAL(10,2) | 额定功率（可空） | 逐桩表汇总/造数 |
| ods_weather_zone_h | taz_id | VARCHAR(32) | 片区 | Open-Meteo |
| | ts_hour | DATETIME | 整点 | 请求 timezone=Asia/Shanghai |
| | temp_c / precip_mm / humidity / wind_ms | DOUBLE | 气象 | API |
| ods_calendar_day | dt | DATE | 日期 | 国务院放假安排自整 |
| | is_holiday | TINYINT | 节假日 | |
| | is_workday | TINYINT | 工作日（含调休） | |

## 2. DWD（清洗后明细 + 质量标记）

| 表名 | 字段 | 类型 | 说明 |
|------|------|------|------|
| dwd_station_5min_clean | station_id, ts | | 主键 |
| | busy, idle, total_piles | INT | 修正后 |
| | occupancy | DECIMAL(8,6) | busy/total_piles |
| | s_price, e_price, duration, volume | | 异常处理后 |
| | is_row_missing | TINYINT | 原整行 NaN |
| | is_stuck | TINYINT | 占用长期不变站标记 |
| | is_outlier | TINYINT | IQR/3σ 等 |
| | quality_flag | VARCHAR(32) | ok / imputed / dropped_src |
| dwd_zone_hour | taz_id, ts_hour | | 片区小时聚合 |
| | occupancy_avg | DECIMAL(8,6) | 站→区均值或桩加权 |
| | busy_sum, piles_sum | INT | |
| | station_cnt | INT | 参与聚合站数 |
| dwd_quality_station | station_id | | 站级质量汇总 |
| | missing_rate | DECIMAL(8,6) | |
| | stuck_flag | TINYINT | |
| | gap_days_max | INT | 最长连续缺口（天） |
| | ticket_suggest | VARCHAR(16) | repair / inspect / none |

## 3. ADS（大屏 / API 指标）

| 表名 | 字段 | 说明 | 对应大屏/API |
|------|------|------|--------------|
| ads_city_overview | dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt | 城市日概览 | `/api/overview` |
| ads_zone_occupancy_hour | taz_id, ts_hour, occupancy, load_kw_est | 热力/时序；load≈busy×额定功率 | `/api/zone/series` |
| ads_zone_peak | taz_id, peak_hour, peak_occupancy, cluster_id | 高峰与聚类标签（KMeans 可后补） | `/api/zone/peak` |
| ads_quality_summary | metric, value, note | 质检数字（缺失率、卡死站数等） | `/api/quality` |
| ads_baseline_naive | taz_id, ts_hour, y_true, y_hat_lag1, abs_err | 上一时刻基线 stub | `/api/baseline` |
| ads_ticket | ticket_id, station_id, type, status, created_at, updated_at | 待检修/巡检 stub | `/api/tickets` |

## 4. 质检数字（开题可引用口径，ETL 跑完回填）

| 指标 | 全量参考（已核查） | 演示子集目标 |
|------|-------------------|--------------|
| 原始行量级 | ~87.68M / ~5.4GB | 20～30 站 × 同期 5min |
| 整行缺失率 | ~21.2% | 同规则统计 |
| 全程无数据站 | 21 | 子集可不含 |
| 缺失>30% 站 | 259 | 子集至少 3 |
| 疑似卡死站 | 27 | 子集至少 2 |
| 片区 | 331→清洗后 275 | 随站映射 |

## 5. 不做进表的内容

- LSTM/ARIMA/XGBoost 训练结果表（半个月不做真模型）
- 电网 EMS / 真实人员排班
- Hive/Spark 外部表
