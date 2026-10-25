# 数据湖字典（data/lake）

数据湖放在仓库的 `data/lake/` 下（已加入 .gitignore，不进仓库）。想换位置就设环境变量 `SZ_EV_LAKE`。
全部是 Parquet 文件，Spark / Hive / pandas 都能直接读。数据来源是 UrbanEV 深圳数据集，时间范围 2022-09-01 到 2023-02-28，共 181 天。

## 目录一览

| 目录 | 内容 | 怎么分 | 行数 | 大小 |
|---|---|---|---|---|
| `ods/charge_5min/dt=YYYY-MM-DD/part-0.parquet` | 原始 5 分钟充电记录，原样搬进来 | 每天一个目录 | 87,679,296（181 天） | 约 84 MB |
| `dwd/charge_5min_clean/dt=YYYY-MM-DD/part-0.parquet` | 清洗后的 5 分钟记录，带质量标记 | 每天一个目录 | 87,679,296（181 天） | 约 69 MB |
| `ods/station_info/part-0.parquet` | 站点信息（位置、桩数、所属片区、额定功率） | 不分 | 1,682 | 约 64 KB |
| `ods/pile_power/part-0.parquet` | 每根充电桩的额定功率 | 不分 | 22,650 | 约 160 KB |
| `dwd/quality_station/run=<开始>_<结束>/part-0.parquet` | 每个站在一次运行里的质量汇总和工单建议 | 每次运行一个目录 | 每次 1,682 | 很小 |
| `_runs/<开始>_<结束>_<时间>_<进程号>.json` | 每次运行的记录：参数、耗时、行数、质检数字 | 每次运行一个文件 | — | 很小 |

说明：
- `dt` 只出现在目录名里，文件里没有这一列。用 Spark/Hive 或 `pyarrow.dataset(..., partitioning="hive")` 读时会自动加回来。
- 每天约 484,416 行，即 1,682 个站 × 288 个 5 分钟。
- 全量数据以 `quality_station/run=2022-09-01_2023-02-28` 为准。其他 `run=` 目录是单天或几天的试跑，看全量时不要混进来。
- `_staging/` 是运行中的临时目录，跑完会自动删，留下来的可以忽略。
- `ads/` 归小黑的 Spark 作业管，不在本文档里。

## ods/charge_5min：原始记录

| 列 | 类型 | 含义 |
|---|---|---|
| station_id | int32 | 站点编号 |
| ts | timestamp | 这 5 分钟的开始时间（北京时间，不带时区） |
| busy | double | 正在用的桩数 |
| idle | double | 空闲的桩数 |
| s_price | double | 服务费（元/度） |
| e_price | double | 电费（元/度） |
| fast_busy / fast_idle | double | 快充桩里正在用 / 空闲的数量 |
| slow_busy / slow_idle | double | 慢充桩里正在用 / 空闲的数量 |
| duration | double | 这 5 分钟里的充电时长 |
| volume | double | 这 5 分钟里的充电量 |

原始数据里的空值保持为空，没有补。

## dwd/charge_5min_clean：清洗后的记录

| 列 | 类型 | 含义 |
|---|---|---|
| station_id | int32 | 站点编号 |
| taz_id | int32 | 站点所属的交通片区编号 |
| ts | timestamp | 这 5 分钟的开始时间（北京时间） |
| busy / idle | double | 正在用 / 空闲的桩数，和原始数据相同 |
| total_piles | int32 | 站里的桩总数（来自 station_info 的 charge_count） |
| s_price / e_price | double | 服务费 / 电费 |
| duration / volume | double | 充电时长 / 充电量 |
| is_row_missing | int8 | 1 = 这一行 busy 和 idle 都是空的 |
| is_stuck | int8 | 1 = 这一天这个站的数据“卡住了”（规则见下） |
| is_outlier | int8 | 1 = 这一行的 busy 明显偏离平时（规则见下） |
| occupancy | double | 占用率 = busy ÷ total_piles；缺数据或桩数为 0 时为空 |
| quality_flag | string | `ok` 正常；`dropped_src` 原始就缺；`outlier` 异常值 |

### 标记规则

- **缺数据**：busy 和 idle 都是空的，记 `is_row_missing = 1`，`quality_flag = dropped_src`。
- **卡死**：按站按天判断。把当天和前 6 天（共 7 天）里所有不缺的 busy 值放一起看，如果至少有 12 个，而且全都是同一个数，就认为这个站这一天卡死了，当天所有行都记 `is_stuck = 1`。
- **异常值**：同样用这 7 天里不缺的 busy 值（至少 8 个），算出上四分位和下四分位，往外各放宽 1.5 倍四分位距，超出这个范围的 busy 记为异常值。如果这 7 天的数全都一样，就不判异常值。
- 这两个判断只看固定的 7 天，和这次运行从哪天跑到哪天无关。所以单独跑某一天，和全量跑里的同一天，结果完全一样。

## ods/station_info：站点信息

| 列 | 类型 | 含义 |
|---|---|---|
| station_id | int32 | 站点编号 |
| longitude / latitude | double | 经度 / 纬度 |
| slow_count / fast_count | int64 | 站点表登记的慢充 / 快充桩数 |
| charge_count | int64 | 站点表登记的桩总数 |
| taz_id | int32 | 所属交通片区 |
| rated_power_kw_total | double | 全站所有桩的额定功率加起来（kW）；功率表里没有这个站时为空 |
| dc_pile_cnt | int32 | 功率表里这个站的直流桩数 |
| ac_pile_cnt | int32 | 功率表里这个站的交流桩数 |
| pile_cnt_from_power | int32 | 功率表里这个站的桩总数（没有记录时为 0） |

后 4 列是从 `pile_power` 加上去的，前 7 列和原始站点表相同。
两张表对不上的地方：有 130 个站在功率表里一根桩都没有；另有 110 个站功率表的桩数和 `charge_count` 不一样（差距从少 98 根到多 82 根）。能完全对上的有 1,420 个站。用功率时请留意这一点。

## ods/pile_power：每根桩的额定功率

| 列 | 类型 | 含义 |
|---|---|---|
| pile_id | int64 | 桩编号（原始文件里是小数，这里转成整数） |
| station_id | int32 | 所属站点 |
| power_kw | double | 额定功率（kW） |
| pile_type | string | `AC` 交流 或 `DC` 直流 |

共 22,650 根桩，其中交流 19,556 根、直流 3,094 根，分布在 1,552 个站。

## dwd/quality_station：每站质量汇总

| 列 | 类型 | 含义 |
|---|---|---|
| station_id / taz_id | int64 | 站点 / 片区编号 |
| rows | int64 | 这次运行里这个站的行数 |
| missing_rows | int64 | 缺数据的行数 |
| missing_rate | double | 缺数据的比例 |
| stuck_flag | int64 | 1 = 至少卡死过一天 |
| stuck_days | int64 | 一共卡死了几天 |
| outlier_rows | int64 | 异常值的行数 |
| gap_days_max | int64 | 最长一段连续缺数据有几天（四舍五入） |
| ticket_suggest | string | 工单建议：`repair` 维修、`inspect` 巡检、`none` 不用管 |

工单建议怎么来的：
- 一共卡死 30 天及以上 → `repair`。全量里有 161 个站。
- 没到维修，但缺数据超过 30% → `inspect`。全量里有 239 个站。
- 其他 → `none`。

某一天卡死的站多了只发预警，不开工单：当天卡死的站超过 221 个，或者当天新卡死的站超过 63 个（今天卡死、昨天没卡死；运行的第一天没有“昨天”，不算）。这两个数是 181 天里每天数字排在第 95% 位置的值。

## _runs：运行记录

每次跑 `etl/full_pipeline.py` 都会留一个 json，里面有：运行参数（`args`）、结束时间（`finished`）、每一步的耗时和行数（`metrics`）、质检结果（`quality`：站数、行数、缺数据行数、异常值行数、卡死站数、维修和巡检建议数）。

## 怎么重新生成

```bash
export SZ_EV_LAKE=/path/to/data/lake        # 不设就用仓库里的 data/lake
export URBANEV_SRC=/path/to/20220901-20230228_station-raw   # 原始数据目录，有默认值

# 全量 181 天（约 2 分钟，内存峰值约 2.2 GB），同时重写两份报告
python etl/full_pipeline.py --start 2022-09-01 --end 2023-02-28 --write-report

# 只重跑某一天（不会改 docs 下的报告）
python etl/full_pipeline.py --ds 2022-09-01

# 只重建桩功率表，并给 station_info 加上功率几列（几秒钟）
python etl/pile_power.py
```

- 每天的目录是整个替换的：先写到临时目录，写完再换上去，所以同一天跑两遍结果一样。
- 写完会核对行数，暂存的行数和写出的行数对不上会直接报错退出。
- `full_pipeline.py` 每次都会重写 `station_info`，并自动跑一遍 `pile_power`，功率几列不会丢。
- 测试：`python -m pytest -q tests/etl`，用的是几个假站点的小数据，几秒就跑完。
