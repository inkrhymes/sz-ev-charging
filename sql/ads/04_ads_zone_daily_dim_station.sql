USE sz_ev;
-- 片区日汇总（前端地图用）：中心点 = 片区内站点经纬度均值；由 spark/jobs/ads_metrics.py 回写
CREATE TABLE IF NOT EXISTS ads_zone_daily (
  dt DATE NOT NULL,
  taz_id VARCHAR(32) NOT NULL,
  avg_occupancy DECIMAL(8,6) NULL,
  station_cnt INT NULL,
  pile_cnt INT NULL,
  center_lon DECIMAL(10,6) NULL,
  center_lat DECIMAL(10,6) NULL,
  PRIMARY KEY (dt, taz_id),
  KEY idx_zone_daily_taz (taz_id, dt)
) ENGINE=InnoDB;

-- 站点维表（经纬度来自 UrbanEV station_information.csv）
CREATE TABLE IF NOT EXISTS dim_station (
  station_id VARCHAR(32) PRIMARY KEY,
  taz_id VARCHAR(32) NULL,
  longitude DECIMAL(10,6) NULL,
  latitude DECIMAL(10,6) NULL,
  slow_count INT NULL,
  fast_count INT NULL,
  total_piles INT NULL,
  rated_power_kw DECIMAL(10,2) NULL,
  KEY idx_dim_station_taz (taz_id)
) ENGINE=InnoDB;
