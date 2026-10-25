USE sz_ev;

CREATE TABLE IF NOT EXISTS ads_city_overview (
  dt DATE PRIMARY KEY,
  station_cnt INT NULL,
  zone_cnt INT NULL,
  avg_occupancy DECIMAL(8,6) NULL,
  peak_hour INT NULL,
  stuck_cnt INT NULL,
  high_missing_cnt INT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ads_zone_occupancy_hour (
  taz_id VARCHAR(32) NOT NULL,
  ts_hour DATETIME NOT NULL,
  occupancy DECIMAL(8,6) NULL,
  load_kw_est DECIMAL(12,2) NULL,      -- 占用率 × 片区内有功率数据站的额定功率之和；片区全无功率数据为 NULL
  power_coverage DECIMAL(7,6) NULL,   -- 片区内有功率数据站的桩数 / 片区全部桩数
  PRIMARY KEY (taz_id, ts_hour)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ads_quality_summary (
  metric VARCHAR(64) PRIMARY KEY,
  value DECIMAL(18,6) NULL,
  note VARCHAR(255) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ads_baseline_naive (
  taz_id VARCHAR(32) NOT NULL,
  ts_hour DATETIME NOT NULL,
  y_true DECIMAL(8,6) NULL,
  y_hat_lag1 DECIMAL(8,6) NULL,
  abs_err DECIMAL(8,6) NULL,
  PRIMARY KEY (taz_id, ts_hour)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ads_ticket (
  ticket_id BIGINT PRIMARY KEY AUTO_INCREMENT,
  station_id VARCHAR(32) NOT NULL,
  alert_id BIGINT NULL,          -- 来源质检告警 dq_alert.alert_id（可空）
  type VARCHAR(16) NOT NULL,
  status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_alert_id (alert_id)
) ENGINE=InnoDB;
