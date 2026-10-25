-- 准实时片区占用率（stream/zone_occupancy_stream.py 写入，按 (taz_id, window_end) 幂等 upsert）
CREATE TABLE IF NOT EXISTS rt_zone_occupancy (
  taz_id        VARCHAR(16)  NOT NULL,
  window_start  DATETIME     NOT NULL COMMENT '窗口开始（北京时间）',
  window_end    DATETIME     NOT NULL COMMENT '窗口结束（北京时间，不含）',
  occupancy     DECIMAL(8,6) NULL     COMMENT 'sum(busy)/sum(total_piles)',
  busy_sum      INT          NOT NULL,
  piles_sum     INT          NOT NULL,
  station_cnt   INT          NOT NULL COMMENT '窗口内去重站点数',
  msg_cnt       INT          NOT NULL COMMENT '窗口内参与计算的消息条数',
  ingest_ts_max DATETIME(3)  NULL     COMMENT '窗口内最晚一条消息进入 Spark 的时间',
  write_ts      DATETIME(3)  NOT NULL COMMENT '本行写入 MySQL 的时间',
  latency_ms    INT          NULL     COMMENT 'write_ts - ingest_ts_max，毫秒',
  batch_id      BIGINT       NOT NULL COMMENT 'Spark micro-batch 编号',
  PRIMARY KEY (taz_id, window_end),
  KEY idx_rt_window_end (window_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
