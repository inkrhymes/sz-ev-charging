-- ODS v0.1 — 对齐 docs/data_dictionary.md
CREATE DATABASE IF NOT EXISTS sz_ev DEFAULT CHARACTER SET utf8mb4;
USE sz_ev;

CREATE TABLE IF NOT EXISTS ods_station_raw_5min (
  station_id VARCHAR(32) NOT NULL,
  ts DATETIME NOT NULL,
  busy INT NULL,
  idle INT NULL,
  s_price DECIMAL(10,4) NULL,
  e_price DECIMAL(10,4) NULL,
  fast_busy INT NULL,
  fast_idle INT NULL,
  slow_busy INT NULL,
  slow_idle INT NULL,
  duration DECIMAL(12,4) NULL,
  volume DECIMAL(12,4) NULL,
  src_file VARCHAR(128) NULL,
  load_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (station_id, ts)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_station_info (
  station_id VARCHAR(32) PRIMARY KEY,
  taz_id VARCHAR(32) NULL,
  lon_gcj DOUBLE NULL,
  lat_gcj DOUBLE NULL,
  lon_wgs DOUBLE NULL,
  lat_wgs DOUBLE NULL,
  total_piles INT NULL,
  rated_power_kw DECIMAL(10,2) NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_weather_zone_h (
  taz_id VARCHAR(32) NOT NULL,
  ts_hour DATETIME NOT NULL,
  temp_c DOUBLE NULL,
  precip_mm DOUBLE NULL,
  humidity DOUBLE NULL,
  wind_ms DOUBLE NULL,
  PRIMARY KEY (taz_id, ts_hour)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ods_calendar_day (
  dt DATE PRIMARY KEY,
  is_holiday TINYINT NOT NULL DEFAULT 0,
  is_workday TINYINT NOT NULL DEFAULT 1
) ENGINE=InnoDB;
