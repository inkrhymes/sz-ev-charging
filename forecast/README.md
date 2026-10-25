# forecast/ — 片区下一小时占用率预测（LightGBM vs 上一小时）

```bash
pip install -r forecast/requirements.txt
python -m forecast.train                                    # 随仓演示数据 data/demo/dwd/dwd_zone_hour.csv
python -m forecast.train --source data/lake/dwd/charge_5min_clean   # 全量清洗后 Parquet
```

输出：`forecast/metrics.json`（整体 + 分片区 MAE/RMSE）、`forecast/predictions.csv`（taz_id, ts, y, y_hat_lgbm, y_hat_lag1）。

## 数据加载（`features.load_zone_hour(source)`，唯一入口）

- `*.csv`：dwd_zone_hour 口径，列 `taz_id, ts_hour, occupancy_avg`。
- Parquet 目录/文件（默认约定 `data/lake/dwd/charge_5min_clean/dt=YYYY-MM-DD/`，Hive 风格分区）：
  需要列 `station_id, ts (timestamp, 5min), busy (float), total_piles (int), taz_id`；
  先剔除 busy/total_piles 为空或桩数为 0 的行，`occ = busy/total_piles`，按 `taz_id + 小时` 取均值。
- 至少需要 7 天历史（lag-24 / rolling-24；lag-168 前一周会被补成空值由 LightGBM 处理），不足会直接报错退出。

## 特征与评估

hour, day_of_week, is_weekend, lag 1/2/3/24/168, 过去 3/24/168 小时滚动均值（先 shift(1) 再滚动），diff_1（lag1−lag2），
过去 4 周同一时刻均值 same_hour_mean_4w，taz_id（类别）。全部只用预测时刻之前的数据。
每个片区先补齐整点网格再算 lag，保证「lag_k = k 小时前」，不跨片区。时间严格切分：`--test-days 28` 即最后 28 天做测试集。
基线 = lag-1（与 `/api/baseline` 的 `y_hat_lag1` 同口径）。
全量读取：逐个 `dt=` 分区用 pyarrow 读入后立即聚合到片区·小时（跳过 is_row_missing=1），不会把 8768 万行一次装进内存。

全量复现：

```bash
python -m forecast.train --source data/lake/dwd/charge_5min_clean --test-days 28   # 约 4 分钟
```

## 当前结果（全量：331 片区、2022-09-01 ~ 2023-02-28；测试集 2023-02-01 起 28 天，182,227 个片区小时）

| 方法 | MAE | RMSE |
|---|---|---|
| 上一小时 lag-1 | 0.0285 | 0.0578 |
| LightGBM | **0.0255** | **0.0461** |

LightGBM 整体 MAE 降低约 10.6%，RMSE 降低约 20.1%。分片区看：331 个片区中 174 个 MAE 更低、289 个 RMSE 更低——
优势主要在大波动时刻（早晚高峰切换），平稳片区里 lag-1 本身已经很准，两者接近。训练集 1,151,449 行。
`predictions.csv` 只保留测试集最后 2 天（`--pred-keep-days`），完整逐片区指标见 `metrics.json`。

## 历史记录：早期演示结果（已作废）

最早只在随仓演示数据上跑过（18 个片区、14 天）。那时 LightGBM 的 MAE 反而不如上一小时（0.0339 对 0.0277），因为数据太少学不到一周的规律。这个结果只留作记录，不要再引用，以上面的全量结果为准。

## 接口字段（已接入 `/api/baseline`）

每个点多 `y_hat_lgbm`、`abs_err_lgbm`（不在预测的 2 月 27–28 日内为 null）；顶层多 `metrics`（`mae_lgbm`、`mae_lag1`、`rmse_lgbm`、`rmse_lag1`、`split_ts`）和 `forecast_window`。文件模式和 `API_SOURCE=mysql` 模式字段一样。详细说明见 `docs/architecture.md` 8.3 节。

## 每日生产预测（`forecast/daily.py` + `forecast/evaluate.py`）

每天两步，都是独立命令行，读 `$SZ_EV_LAKE`（默认 `data/lake`）：

```bash
python -m forecast.daily    --ds D      # 用截至 D 当天 23:00 的数据训练，预测 D+1 全天 24 小时
python -m forecast.evaluate --ds D-1+1  # 即评估“昨天预测的今天”：--ds 填被评估的日期
```

| 命令 | 读 | 写 | 退出码 |
|---|---|---|---|
| `daily --ds D` | `ads/zone_hour/dt=D-84..D`（至少 14 天，D 当天必须有） | `ads/forecast_zone_hour/dt=D+1/part-0.parquet`；MySQL `ads_forecast_zone_hour`（D+1 整天先删后插） | 0 成功 / 2 输入不足 / 3 写库失败 |
| `evaluate --ds D` | `ads/forecast_zone_hour/dt=D`、`ads/zone_hour/dt=D` 和 `dt=D-1` | `ads/forecast_metric_daily/dt=D`；MySQL `ads_forecast_metric_daily`（按天先删后插）；触发时 `dq_alert` 一条 `forecast_drift` | 0 正常（含告警）/ 2 输入不足 / 3 写库失败 |

可选参数：`--no-db`（只写 Parquet）、`--train-days 56`、`--no-holiday`、`daily --out-dir`。建表语句在 `sql/ads/06_ads_forecast.sql`，脚本第一次写库时也会自动 `CREATE TABLE IF NOT EXISTS`。

- **怎么预测一整天**：训练窗口是最近 56 天（再往前多读 29 天给 4 周同时刻均值用）。预测时逐小时递推，下一小时的 lag 用上一步的预测值。单次约 4 秒，峰值内存约 400 MB（2 线程）。
- **基线 `y_hat_lag1`**：日前场景拿不到“上一小时的真实值”，所以基线是预测时最后一个已知值，24 小时都用它。评估时另算 `mae_lag24`（昨天同一时刻）作参考。
- **没数据的片区**：331 个片区都会出行，但最近 7 天一个有效值都没有的片区，预测值留空（NULL）。
- **节假日特征**：`forecast/calendar_cn.py` 提供 `is_holiday / is_adjusted_workday / is_offday`，覆盖 2022-23 年法定假日和调休，包括国庆 10-01~07、春节 2023-01-21~27。
- **天气插口**：在 `daily.EXTRA_FEATURES` 里注册 `fn(ts, taz_id) -> DataFrame`，训练和预测时会自动带上新的列。
- **漂移告警**（`evaluate.py`）：出现下面任一情况，就往 `dq_alert` 写一条 `layer=ads, severity=warn, rule_name=forecast_drift`：① 模型 MAE 连续 3 天比基线差；② 当天 MAE 超过前 28 天中位数的 1.5 倍（至少要有 7 天历史）。同一天重跑时，会先删掉当天还是 open 的旧告警。
- **API**：MySQL 模式下，`/api/baseline` 的 `y_hat_lgbm` 和 `forecast_window` 优先读 `ads_forecast_zone_hour`，表不存在或为空时退回 `predictions.csv`，接口字段不变。`/api/forecast/metrics` 返回每天的误差序列 `{source, points:[{ds, n, mae_lgbm, rmse_lgbm, mae_lag1, rmse_lag1, mae_lag24, model_version, drift}]}`。
