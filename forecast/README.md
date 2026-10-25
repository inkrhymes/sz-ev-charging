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
- 至少需要 7 天历史（lag-24 / rolling-24），不足会直接报错退出。

## 特征与评估

hour, day_of_week, is_weekend, lag 1/2/3/24, 过去 3/24 小时滚动均值（先 shift(1) 再滚动），taz_id（类别）。
每个片区先补齐整点网格再算 lag，保证「lag_k = k 小时前」，不跨片区。时间严格切分：最后约 20% 的时刻做测试集。
基线 = lag-1（与 `/api/baseline` 的 `y_hat_lag1` 同口径）。

## 当前结果（演示子集：18 片区、14 天，测试集 959 个片区小时）

| 方法 | MAE | RMSE |
|---|---|---|
| 上一小时 lag-1 | 0.0277 | 0.0670 |
| LightGBM | 0.0339 | 0.0644 |

如实说明：在这份小数据上 LightGBM 只在 RMSE 上略好（大误差更少），MAE 不如 lag-1，18 个片区里只有 1 个 MAE 更低。
原因是训练样本只有约 4.5k 行、每片区 11 天，小时级占用自相关极强。全量数据（约 275 片区、更长时间）接入后重跑再下结论。

## 建议的接口字段（暂不接入，供 @小黄 / API 参考）

`GET /api/baseline?taz_id=&from=&to=` 每个点在现有 `y_true / y_hat_lag1 / abs_err` 之外增加：

| 字段 | 类型 | 说明 |
|---|---|---|
| `y_hat_lgbm` | float \| null | LightGBM 预测；不在测试/预测区间时为 null |
| `abs_err_lgbm` | float \| null | `|y_true - y_hat_lgbm|` |

响应 `data` 顶层增加 `metrics: {"lag1": {"mae","rmse"}, "lgbm": {"mae","rmse"}, "split_ts": "..."}`（取自 metrics.json 对应片区）。
API 端实现方式：读 `forecast/predictions.csv`（后续可换成 MySQL `ads_forecast_zone_hour` 表），按 taz_id+ts 关联。
