-- 数据质量运行记录：每条规则每次执行一行
CREATE TABLE IF NOT EXISTS dq_run_log (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id      VARCHAR(64)  NOT NULL,
  ds          DATE         NOT NULL,
  layer       VARCHAR(16)  NOT NULL,
  rule_name   VARCHAR(128) NOT NULL,
  rule_type   VARCHAR(32)  NOT NULL,
  severity    VARCHAR(8)   NOT NULL,   -- block / warn
  passed      TINYINT      NOT NULL,
  actual      VARCHAR(255) NULL,
  expected    VARCHAR(255) NULL,
  message     VARCHAR(512) NULL,
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ds_layer (ds, layer),
  KEY idx_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
