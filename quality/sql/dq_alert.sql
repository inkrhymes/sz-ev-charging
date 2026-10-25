-- 质检告警：每条未通过的规则（或站点级问题）一行，前端告警页读写 status/note
CREATE TABLE IF NOT EXISTS dq_alert (
  alert_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
  ds           DATE         NOT NULL,
  layer        VARCHAR(16)  NOT NULL,
  rule_name    VARCHAR(128) NOT NULL,
  severity     ENUM('block','warn') NOT NULL,
  station_id   VARCHAR(32)  NULL,
  taz_id       VARCHAR(32)  NULL,
  metric_value DOUBLE       NULL,
  threshold    DOUBLE       NULL,
  detail       VARCHAR(500) NULL,
  status       ENUM('open','ack','closed') NOT NULL DEFAULT 'open',
  note         VARCHAR(500) NULL,
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ds_status (ds, status),
  KEY idx_rule (rule_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
