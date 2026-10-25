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
