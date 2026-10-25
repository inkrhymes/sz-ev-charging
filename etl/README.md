# ETL / 质检（小紫）

文件优先（不依赖 MySQL）。站号**只**用已锁定的 `data/demo/demo_station_ids.txt`（25 站），从 UrbanEV 真实 CSV 复制/切片，禁止另造站号。

## 一键顺序

在仓库根目录：

```bash
cd /workspace/sz-ev-charging

# 1) 从 UrbanEV 复制锁定 25 站 → data/demo/raw/（默认切 2022-09-01～09-14）
python3 etl/generate_demo_subset.py

# 全量半年（体积大）可用：
# python3 etl/generate_demo_subset.py --full

# 2) 拼 ODS CSV → data/demo/ods/
python3 etl/load_ods.py

# 3) 清洗 → data/demo/dwd/ + 写 docs/quality_report.md（含实测数字）
python3 etl/clean_to_dwd.py
```

## 产出

| 路径 | 说明 |
|------|------|
| `docs/demo_station_list.md` | 25 站锁定说明与选取理由 |
| `data/demo/raw/{id}.csv` | UrbanEV 原始切片 |
| `data/demo/station_info.csv` | 站信息（含 flag） |
| `data/demo/ods/ods_station_raw_5min.csv` | ODS 贴源 |
| `data/demo/ods/ods_station_info.csv` | ODS 站维 |
| `data/demo/dwd/dwd_station_5min_clean.csv` | 清洗明细 |
| `data/demo/dwd/dwd_quality_station.csv` | 站级质检 + ticket_suggest |
| `data/demo/dwd/dwd_zone_hour.csv` | 片区小时聚合 |
| `docs/quality_report.md` | **实测**缺失率 / 卡死站数 / 高缺失站数 |

## 质检口径（摘要）

- 整行缺失：`busy` 与 `idle` 同时为空
- 卡死：窗口内非缺失 `busy` 唯一取值
- 高缺失站：站级 `missing_rate > 30%` → `ticket_suggest=inspect`
- 卡死站 → `ticket_suggest=repair`
- 目标：卡死 ≥2、高缺失 ≥3（见 `docs/data_dictionary.md` §4）

## TODO

- MySQL `INSERT` / `LOAD DATA` 进 `ods_*`、`dwd_*`、`ads_quality_summary`（docker-compose 起库后）
- 不做：LSTM / 真预测训练
