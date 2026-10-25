# 准实时链路设计（第二批）

离线链路（Airflow + Spark 按天跑 ads）之外，再加一条准实时链路：小紫把 5 分钟数据按真实节奏回放进 Kafka，
`stream/zone_occupancy_stream.py` 用 Spark Structured Streaming 算每个片区最近 15 分钟的占用率，写进 MySQL 的
`rt_zone_occupancy`，API `/api/realtime/zones` 读出来给页面自动刷新。

用 Spark 而不是 Flink：机器内存紧，Spark 已经装好、和离线用同一套依赖，流作业 `local[2]` + 1G 驱动内存就够。

## 1. 消息格式（和小紫约定）

- topic：`charge_5min`，6 个分区，key = `station_id`（同一个站的消息总在同一个分区，保证站内有序）。
- 地址：环境变量 `KAFKA_BOOTSTRAP`，默认 `localhost:9092`（单节点 KRaft，归小紫管）。
- value 是 JSON，一条消息 = 一个站一个 5 分钟点：

| 字段 | 类型 | 说明 |
|---|---|---|
| station_id | string | 站点编号 |
| taz_id | string | 片区编号 |
| ts | string | 事件时间，北京时间 `YYYY-MM-DD HH:MM:SS` |
| busy | int | 在用桩数 |
| idle | int | 空闲桩数 |
| total_piles | int | 总桩数（分母用它） |
| occupancy | double | 站级占用率（流计算不用它，片区占用率按 busy/total_piles 重新加总） |
| quality_flag | string | 质量标记 |

`quality_flag` 实际取值：`ok`、`outlier`（数值异常）、`dropped_src`（源头断传补的点）。
过滤规则：`quality_flag` 属于环境变量 `SZ_RT_BAD_FLAGS` 里的值（逗号分隔、不分大小写，默认 `outlier,dropped_src`）就算坏消息，
不参与占用率、busy_sum、piles_sum、station_cnt、msg_cnt，但会按标记分别计数到 `outlier_cnt`、`dropped_src_cnt`，
页面能看出某个片区"这 15 分钟有几条数据被排除"。设成空字符串则全部参与计算。
另外 `total_piles<=0`、`busy` 为空、`ts` 解析不出来的消息直接丢掉，不计数。窗口里全是坏消息时 occupancy 为空。

## 2. 窗口和 watermark

- 按 `taz_id` 分组，窗口长 15 分钟、每 5 分钟滑一次。所以一条 10:05 的消息会同时算进
  [09:55,10:10)、[10:00,10:15)、[10:05,10:20) 三个窗口。
- 输出：`occupancy = sum(busy)/sum(total_piles)`、`busy_sum`、`piles_sum`、`station_cnt`（窗口内去重站数）、`msg_cnt`、`window_start`、`window_end`。
- watermark 10 分钟：Spark 记着见过的最大事件时间，比它早 10 分钟以上的迟到消息直接丢弃，对应窗口的状态也会被清掉，
  内存不会一直涨。回放是按时间顺序发的，10 分钟足够吸收 6 个分区之间的先后差。
- 输出模式用 `update`：窗口每来一批新数据就重新输出一次当前值，页面能尽早看到；窗口关闭前的值可能还会被改。

## 3. 写库：幂等 upsert，而不是端到端 exactly-once

- 每个 micro-batch 用 `foreachBatch` 把结果按主键 `(taz_id, window_end)` 做 `INSERT … ON DUPLICATE KEY UPDATE`。
- Spark 自己通过 checkpoint 记住 Kafka offset 和窗口状态。作业挂了重启，会从上次提交的 offset 重放最后一批，
  这一批可能写两次 —— 这就是"至少一次"。
- 因为同一个窗口不管写几次都覆盖成同一个值，结果和只写一次一样，效果上等于 exactly-once，不需要 Kafka 事务或两阶段提交。
  代价是：MySQL 表里只保留每个窗口的最新值，不保留中间版本。
- checkpoint 目录默认 `/workspace/sz-rt-checkpoint/zone_occupancy`（不放 /tmp，重启会丢）。改了窗口参数要换一个新目录。

## 4. 表结构和延迟

DDL：`sql/ads/20_rt_zone_occupancy.sql`。除了上面的指标，每行还记：

- `ingest_ts_max`：窗口里最晚一条消息进入 Spark 的时间；
- `write_ts`：这一行写进 MySQL 的时间；
- `latency_ms = write_ts - ingest_ts_max`，即从进入流计算到落库的耗时；
- `batch_id`：哪个 micro-batch 写的；
- `outlier_cnt` / `dropped_src_cnt`：被排除的坏消息条数（`/api/realtime/zones` 也返回这两个字段）。

本机 file 模式实测一批延迟约 2 秒（Kafka 模式还要加上 trigger 间隔，默认 30 秒）。

## 5. 和离线 ads 的关系（Lambda 取舍）

| | 离线 ads（批） | 准实时 rt（流） |
|---|---|---|
| 粒度 | 小时 / 天 | 15 分钟窗口，5 分钟一更新 |
| 延迟 | T+1，一天跑一次约 2 分钟 | 秒到半分钟 |
| 数据质量 | 有完整的 dwd 质检，整天断传会被拦 | 只做逐条过滤，没有整天级质检 |
| 迟到数据 | 全量重算，都能算进去 | 超过 10 分钟的迟到直接丢 |
| 用途 | 报表、趋势、预测训练、告警 | 页面上"现在"各片区有多忙 |

两条线并存，离线结果是"准的"，实时结果是"快的"。同一时段两边对不上时以离线为准；实时表只保留近期数据即可。
不做 Kappa（全部用流重算），是因为离线那套质检、回填和 Hive 数仓已经跑通，流这边只补"实时看板"这一个缺口，改动最小。

## 6. 怎么跑

```bash
export JAVA_HOME=/workspace/jdk PATH=/workspace/jdk/bin:$PATH
# 无 Kafka：读 JSON lines 目录，处理完就退出
python stream/zone_occupancy_stream.py --source file --input DIR --sink mysql --checkpoint DIR_CK --once
# 有 Kafka（会自动下载 spark-sql-kafka 包）
KAFKA_BOOTSTRAP=localhost:9092 python stream/zone_occupancy_stream.py --source kafka --sink mysql
```

`--sink csv --out DIR` 或 `--sink console` 用于调试。开流作业时不要同时跑 Airflow 或离线 Spark。
测试：`tests/stream/test_zone_occupancy_stream.py` 用 file 模式和造的小数据跑一遍，没装 pyspark 时自动跳过。

### 6.1 起 Kafka（单节点 KRaft，第三段）

```bash
docker compose --profile stream up -d kafka kafka-init   # kafka 内存上限 512M，堆 256M；kafka-init 建 charge_5min（6 分区，1 副本）后退出
export KAFKA_BOOTSTRAP=localhost:9092
SZ_SPARK_MASTER=local[1] SZ_DRIVER_MEM=1g python stream/zone_occupancy_stream.py ...   # 默认即 local[1] + 1g
```

- broker 关闭了自动建 topic，必须先跑 kafka-init。
- 内存紧张：回放时先停 API/Redis，不与 ads_build/Airflow/Locust 同时跑；开跑前后在群里报。

## 7. Kafka 端到端回放实测（2026-10-25）

用 2022-09-01 一整天的数据，从 Kafka 一路跑到 MySQL，再和离线小时表对账。

**环境**

- Kafka：单节点 KRaft（apache/kafka 3.7.0，内存上限 512M），topic `charge_5min`，6 个分区。
- 流作业：Spark Structured Streaming，`local[1]`，driver 内存 1g。
- 跑的时候机器可用内存约 10G。

**结果**

| 项目 | 数值 |
|---|---|
| 发送（`etl/kafka_replay.py` 回放 2022-09-01） | 484,416 条，用时 16 秒（约 3 万条/秒），无报错 |
| 流作业消费 | 3 个微批，各分区 offset 加起来 484,416，和发送条数一致 |
| 写入 `rt_zone_occupancy` | 93,727 行，325 个片区 |
| 每个片区的窗口数 | 平均 288.4 个，最多 290 个（一天 288 个 5 分钟窗口，加上首尾跨天的边界窗口） |
| 平均占用率 | 0.273（离线小时表为 0.267） |
| 异常值 | 15,357 条 |
| 源数据被丢弃（dropped_src） | 0 |

**和离线 `ads_zone_occupancy_hour` 对账**

- 少了 6 个片区：573、583、782、784、788、952。它们 09-01 本来就没有数据，是后来才开始有数的：583、782 从 10-01 开始，784 从 10-07，573 从 11-26，788、952 从 12-01。
- 片区 806（1 个站、12 根桩）只有 210 个窗口。原因是源数据在北京时间 10:00–11:45 和 14:00–18:45 两段本身就缺数，离线表在同样的时段也是空的。
- 当天最后几个窗口要等 watermark 推过去才会写出，回放刚结束时看不到是正常的。

另外，小黄在实时页（`#/realtime`）录了一段 09-02 按 288 倍速回放的画面。
