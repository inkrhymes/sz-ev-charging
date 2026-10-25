-- 调度运行记录：每个任务每次执行一行（Airflow 重试会多一行），供「调度运行」页和监控指标读取
CREATE TABLE IF NOT EXISTS ops_task_run (
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  dag_id       VARCHAR(64)  NOT NULL,
  ds           DATE         NOT NULL,
  task_id      VARCHAR(64)  NOT NULL,
  state        VARCHAR(16)  NOT NULL,   -- success / failed / blocked / skipped / running
  start_ts     DATETIME(3)  NOT NULL,   -- 北京时间
  end_ts       DATETIME(3)  NULL,
  duration_s   DECIMAL(10,3) NULL,
  block_reason VARCHAR(1000) NULL,      -- 质检拦下或失败的原因（大白话）
  source       VARCHAR(16)  NOT NULL DEFAULT 'run_day',  -- airflow / run_day / backfill
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_run (dag_id, ds, task_id, start_ts),
  KEY idx_ds (ds),
  KEY idx_start (start_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
