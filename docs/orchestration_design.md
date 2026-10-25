# 调度与数据质量设计（小白）

目标：每天按分区 `ds` 跑一次 ods → dwd → ads，每层后面接一道质检闸门；严重问题直接阻断下游，历史数据可以一条命令补跑。

## 1. 选型
| 方案 | 结论 |
|---|---|
| **Apache Airflow 2.x**（选用） | 单容器 `standalone`，LocalExecutor；元数据库放在现有 `sz-ev-mysql` 的独立库 `airflow`，不再多起一个数据库；容器内存上限 1.5G。DAG 就是 Python 文件，能和质检引擎共用代码，方便写测试。 |
| DolphinScheduler（备选） | 可视化拖拽、国内用得多；但 master / worker / api / 注册中心组件多，单机 15G 内存还要跑 MySQL 和 Spark，划不来。 |
| crontab | 没有依赖关系、重试和补跑记录，不考虑。 |

内存约定：Airflow 只在需要调度时启动，演示完就 `down`；平时用 `run_day.py` 跑同一条链路。

## 2. DAG：`sz_ev_daily`
```
ods_ingest(ds) → dq_ods(ds) → dwd_build(ds) → dq_dwd(ds) → ads_build(ds) → dq_ads(ds) → publish(ds)
```
- 文件：`orchestration/dags/sz_ev_daily.py`；任务逻辑在 `orchestration/tasks.py`，DAG 和本地执行器共用这一份。
- `schedule=@daily`，`catchup=False`（历史数据显式补跑），`max_active_runs=1`（一次只跑一天，省内存）。
- 构建任务重试 2 次、间隔 5 分钟；质检任务不重试（数据不会自己变好）。

### 各负责人的步骤怎么接
任务函数只是薄包装：按路径调用脚本，统一传 `ds`。脚本存在就调真实步骤，不存在就退回演示实现（从 `data/demo` 按天切分区）。

| 任务 | 调用 | 负责人 | 现状 |
|---|---|---|---|
| ods_ingest | `python etl/full_pipeline.py --start ds --end ds --layer ods` | 小紫 | 已接入 |
| dwd_build | `python etl/full_pipeline.py --start ds --end ds --layer dwd` | 小紫 | 已接入（之后切到小黑的 Spark dwd 作业） |
| ads_build | `python spark/jobs/ads_build.py --ds ds` | 小黑 | 占位，现用演示汇总 |
| publish | `python spark/jobs/export_ads_mysql.py --ds ds` | 小黑 | 占位，现只打印行数 |

分区路径：`data/lake/ods/charge_5min/dt=ds/`、`data/lake/dwd/charge_5min_clean/dt=ds/`、`data/lake/ads/zone_hour/dt=ds/`。演示模式写到 `orchestration/_work/lake/`（不进 git），不会覆盖正式湖里的数据。`ORCH_FORCE_DEMO=1` 可以强制走演示。

## 3. 增量、幂等与补跑
- **增量**：每次只处理一个 `ds` 分区。
- **幂等**：每个任务先清空当天分区再写（覆盖写），重跑结果一样；`dq_run_log` 按 `run_id` 区分每次运行。
- **补跑**：`orchestration/scripts/run_backfill.sh 2022-09-01 2022-09-07`
  - Airflow 在跑：`airflow dags backfill -s 2022-09-01 -e 2022-09-07 --reset-dagruns -y sz_ev_daily`
  - Airflow 没起：`python orchestration/run_day.py --start 2022-09-01 --end 2022-09-07`，按天顺序跑，某天失败就停。

## 4. 失败与告警
- 失败回调写错误日志；所有规则结果写入 MySQL `dq_run_log`（DDL：`quality/sql/dq_run_log.sql`），数据质量页和告警页可以直接查。
- 邮件钩子已留好：设置 `ALERT_EMAIL` 并配置 Airflow SMTP 后自动发信，默认关闭。

## 5. 质检规则
引擎：`quality/engine.py`（pandas）。规则：`quality/rules/{ods,dwd,ads}.yml`。数据来源可以是 CSV / Parquet 文件或分区目录，也可以是一条 MySQL 查询（`--sql "... WHERE dt='{ds}'"`）。

```yaml
rules:
  - name: ods_pk_unique        # 唯一名字
    type: duplicate_key        # 规则类型
    keys: [station_id, ts]     # 类型自己的参数
    severity: block            # block 阻断下游 / warn 只记录
    enabled: true              # 可选
```

| type | 参数 | 含义 |
|---|---|---|
| not_null_rate | columns, min, mode(any/all) | 非空率下限 |
| group_null_rate | columns, group_by, threshold, max_groups | 缺数比例超过阈值的站点数 |
| range | column, min, max, max_violations | 取值范围 |
| row_count_min | min | 行数下限 |
| row_count_delta | max_ratio | 与前一天分区相比的行数波动 |
| stuck_value | column, group_by, order_by, n_slots, max_groups | 连续 N 个时段读数不变的站点数 |
| duplicate_key | keys | 主键重复 |
| freshness | column, min_end_offset | 当天数据最新时间是否够晚 |

**级别划分**：主键重复、空分区、行数骤降、占用率越界等说明数据坏了，设为 `block`。缺数比例（14 天演示窗口 26.42%）、读数长时间不变（4 站）、缺数偏多（6 站）都是原始数据本身的特点，设为 `warn`，只记录、不阻断。规则写错（比如列名不存在）按失败处理。

命令行：`python quality/engine.py --layer ods --ds 2022-09-01 --path <分区目录>`，有 block 失败时退出码为 2。

## 6. 实测（2026-10-25）
见本仓库提交说明与下表，数字为本机实测。

| 场景 | 结果 |
|---|---|
| run_day.py 演示 2 天（2022-09-01、09-02，25 站） | 每天 7 个任务全部成功，整条链路约 0.4 秒；每天 ods 7200 行、ads 552 行；warn：缺数比例约 20.5%（单日），缺数偏多 5 站，全天读数不变 4 站 |
| 故意弄坏的分区（重复 50 行 + 20 个负数读数） | dq_ods 两条 block 规则失败，下游 5 个任务全部跳过 |
| 全量单日分区质检（小紫的 2022-09-02，1682 站） | ods/dwd 各 484,416 行，两层质检合计约 1.6 秒，block 全过；warn：缺数偏多 179 站，全天读数不变 63 站 |
| Airflow 2.9.3 单容器，触发 ds=2022-09-01（演示数据） | 7 个任务全部 success，从 ods_ingest 开始到 publish 结束约 7 秒 |
| Airflow 中跑故意弄坏的 ds=2022-09-05（`airflow dags test`，坏数据放在临时目录） | dq_ods 为 failed，下游 5 个任务为 upstream_failed，阻断生效 |
| Airflow 内存 | 启动时最高约 1.38G（上限 1.5G），空闲约 0.9G；演示完已 `down` 释放 |

未通过的规则同时写入 `dq_alert`（DDL：`quality/sql/dq_alert.sql`，字段与告警页一致）。同一 ds / 层 / 规则只保留一条，重跑只刷新数值，不会把已处理的告警重新打开。

## 7. 启停
```bash
docker compose -f orchestration/docker-compose.airflow.yml up -d     # 首次约 2 分钟（初始化 airflow 库）
docker compose -f orchestration/docker-compose.airflow.yml down
```
本地 Web 界面：http://localhost:8080（admin / admin，仅本机演示用）。
