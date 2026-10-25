# 运行监控（Observability）

数据来源都在 MySQL `sz_ev`，接口只读，不依赖 Spark/Airflow 进程在线：

| 表 | 谁写 | 内容 |
|---|---|---|
| `ops_task_run` | `orchestration/tasks.recorded()`（Airflow）、`run_chain()`（run_day.py）、`backfill_run_log.py` | 每个任务每次尝试的状态、起止时间、耗时、拦截原因 |
| `dq_run_log` | `quality/engine.py` | 每次质检每条规则的结果 |
| `dq_alert` | 质检告警 | open / ack / closed |

不接 Grafana：下面这些接口已经够前端「调度运行」页和日常排查用。

## GET /api/runs、/api/ops/runs?from=YYYY-MM-DD&to=YYYY-MM-DD

`/api/runs` 是 `/api/ops/runs` 的别名，返回完全一样。

```json
{"ok": true, "data": {
  "from": "2022-10-26", "to": null,
  "runs": [{
    "ds": "2022-10-27", "status": "blocked",
    "started_at": "2026-10-25 13:02:07", "ended_at": "2026-10-25 13:02:50", "duration_s": 42.763,
    "error": "dq_dwd 拦截：……",
    "steps": [{"name": "ods_ingest", "label": "ODS 入湖", "status": "success",
               "started_at": "...", "ended_at": "...", "duration_s": 20.018, "error": null,
               "source": "backfill_airflow", "source_label": "..."}, "..."],
    "blocked_at": "dq_dwd", "blocked_at_label": "DWD 质检", "complete": true,
    "tasks_total": 7, "tasks_success": 4, "attempts": 9}],
  "counts": {"success": 172, "blocked": 9},
  "chain": ["ods_ingest", "dq_ods", "dwd_build", "dq_dwd", "ads_build", "dq_ads", "publish"],
  "labels": {"ods_ingest": "ODS 入湖", "...": "..."}}}
```

- `status`（天）：有 blocked → `blocked`，否则有 failed → `failed`，否则有 running → `running`，否则 `success`。
- `steps[].status`：`success | failed | blocked | skipped | running | not_recorded`（主链任务没有记录时给 `not_recorded`）。每个步骤取最近一次尝试。
- `error`：天级取拦截/失败步骤的原因；步骤级是该步骤的原因。没有就是 `null`。
- `duration_s` 是各步骤最近一次耗时之和（秒）；时间均为服务器本地时间（Asia/Shanghai），格式 `YYYY-MM-DD HH:MM:SS`。

## GET /api/runs/<ds>、/api/ops/runs/<ds>

同上面单天对象，再加 `history`：这一天所有尝试（含重试），元素结构与 `steps[]` 相同。无记录 404，日期格式错 400，读不到 MySQL 503。

## GET /api/ops/health_metrics?days=30

| 字段 | 含义 |
|---|---|
| `freshness.{ods,dwd,ads}` | `latest_ds` 最近有质检记录的分区；`latest_passed_ds` 最近一个 block 规则全过的分区；`last_checked_at`；`last_built_at` / `built_age_hours` 最近一次单日构建成功的时间 |
| `freshness.publish` / `freshness.forecast` | `latest_ds` 最近成功发布/预测的 ds；`data_until` MySQL 结果表（`ads_zone_occupancy_hour` / `ads_forecast_zone_hour`）里最大的 `ts_hour`；`last_built_at` / `built_age_hours` |
| `last_runs` | 最近 7 次单日运行（按开始时间倒序）：`ds, status, started_at, ended_at, duration_s, steps{任务: 秒}`（skipped 不计） |
| `last_runs_duration_s` | 上面 7 次的 avg / max / min |
| `dq_pass_rate.{ods,dwd,ads}` | 最近 `days` 个分区（每个分区只看最近一次质检）：`day_pass_rate` = 没有 block 失败的天数占比，`rule_pass_rate` = 规则通过占比，`blocked_days` 列表 |
| `open_alerts` | `open`、`ack` 数量，`by_layer`、`by_severity`（只统计 open） |

说明：全量区间重跑的补录记录（`backfill_range`）和只有质检的补录（`backfill_dq`）是一段时间摊到每天的，不代表单日运行，所以健康指标里的「最近运行」「最近构建」不用它们；`/api/runs` 里仍然展示。

## 排查顺序

1. `/api/ops/health_metrics` 看哪层不新鲜、哪层通过率掉了、告警是否堆积。
2. `/api/runs?from=...` 找 blocked/failed 的天。
3. `/api/runs/<ds>` 看具体步骤和 `error`，对照 `dq_run_log` 的规则明细。
