-- 片区选址 / 加桩建议（小紫的 Parquet $SZ_EV_LAKE/ads/zone_site_suggest → export_ads_parquet.py --only site_suggest 全量替换）
CREATE TABLE IF NOT EXISTS ads_zone_site_suggest (
  run_dt            DATE          NOT NULL COMMENT '计算批次日期',
  taz_id            VARCHAR(32)   NOT NULL,
  center_lon        DECIMAL(10,6) NULL,
  center_lat        DECIMAL(10,6) NULL,
  station_cnt       INT           NULL,
  piles             INT           NULL COMMENT '片区总桩数',
  rated_kw          DECIMAL(12,2) NULL COMMENT '片区额定功率之和（有功率数据的站）',
  avg_occupancy     DECIMAL(8,6)  NULL,
  peak_occupancy    DECIMAL(8,6)  NULL,
  busy_hours_ratio  DECIMAL(8,6)  NULL COMMENT '高占用小时占比',
  demand_score      DECIMAL(10,4) NULL,
  supply_score      DECIMAL(10,4) NULL,
  gap_score         DECIMAL(10,4) NULL COMMENT '缺口分，越大越该加桩',
  suggest_add_piles INT           NULL,
  `rank`            INT           NULL COMMENT '1 = 最优先',
  function_tag      VARCHAR(64)   NULL COMMENT '片区功能标签（居住/办公/商业/交通…）',
  reason            VARCHAR(512)  NULL COMMENT '大白话理由',
  PRIMARY KEY (run_dt, taz_id),
  KEY idx_site_rank (`rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 兜底底座：在建议表没数据时，也能从现有维表/ads 表拿到片区的站数、桩数、功率、占用率
CREATE OR REPLACE VIEW v_zone_site_base AS
SELECT s.taz_id,
       s.station_cnt, s.piles, s.rated_kw,
       d.center_lon, d.center_lat, d.avg_occupancy, d.peak_daily_occupancy
FROM (SELECT taz_id, COUNT(*) AS station_cnt, SUM(total_piles) AS piles, SUM(rated_power_kw_total) AS rated_kw
      FROM dim_station WHERE taz_id IS NOT NULL GROUP BY taz_id) s
LEFT JOIN (SELECT taz_id, AVG(center_lon) AS center_lon, AVG(center_lat) AS center_lat,
                  AVG(avg_occupancy) AS avg_occupancy, MAX(avg_occupancy) AS peak_daily_occupancy
           FROM ads_zone_daily GROUP BY taz_id) d ON d.taz_id = s.taz_id;

-- 建议 + 底座：建议表字段为空时用底座补
CREATE OR REPLACE VIEW v_zone_site_suggest AS
SELECT g.run_dt, b.taz_id,
       COALESCE(g.center_lon, b.center_lon) AS center_lon, COALESCE(g.center_lat, b.center_lat) AS center_lat,
       COALESCE(g.station_cnt, b.station_cnt) AS station_cnt, COALESCE(g.piles, b.piles) AS piles,
       COALESCE(g.rated_kw, b.rated_kw) AS rated_kw, COALESCE(g.avg_occupancy, b.avg_occupancy) AS avg_occupancy,
       g.peak_occupancy, g.busy_hours_ratio, g.demand_score, g.supply_score, g.gap_score, g.suggest_add_piles,
       g.`rank`, g.function_tag, g.reason
FROM v_zone_site_base b
LEFT JOIN ads_zone_site_suggest g
  ON g.taz_id = b.taz_id AND g.run_dt = (SELECT MAX(run_dt) FROM ads_zone_site_suggest);
