USE sz_ev;

CREATE TABLE IF NOT EXISTS dwd_station_5min_clean (
  station_id VARCHAR(32) NOT NULL,
  ts DATETIME NOT NULL,
  busy INT NULL,
  idle INT NULL,
  total_piles INT NULL,
  occupancy DECIMAL(8,6) NULL,
  s_price DECIMAL(10,4) NULL,
  e_price DECIMAL(10,4) NULL,
  duration DECIMAL(12,4) NULL,
  volume DECIMAL(12,4) NULL,
  is_row_missing TINYINT NOT NULL DEFAULT 0,
  is_stuck TINYINT NOT NULL DEFAULT 0,
  is_outlier TINYINT NOT NULL DEFAULT 0,
  quality_flag VARCHAR(32) NOT NULL DEFAULT 'ok',
  PRIMARY KEY (station_id, ts)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dwd_zone_hour (
  taz_id VARCHAR(32) NOT NULL,
  ts_hour DATETIME NOT NULL,
  occupancy_avg DECIMAL(8,6) NULL,
  busy_sum INT NULL,
  piles_sum INT NULL,
  station_cnt INT NULL,
  PRIMARY KEY (taz_id, ts_hour)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS dwd_quality_station (
  station_id VARCHAR(32) PRIMARY KEY,
  missing_rate DECIMAL(8,6) NULL,
  stuck_flag TINYINT NOT NULL DEFAULT 0,
  gap_days_max INT NULL,
  ticket_suggest VARCHAR(16) NOT NULL DEFAULT 'none'
) ENGINE=InnoDB;
