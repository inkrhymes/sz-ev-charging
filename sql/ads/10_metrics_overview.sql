-- 城市日概览（按演示子集 dwd 回填；兼容 only_full_group_by）
-- 幂等：重跑前清空
TRUNCATE TABLE ads_city_overview;
INSERT INTO ads_city_overview (dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt)
SELECT
  d.dt,
  (SELECT COUNT(DISTINCT station_id) FROM dwd_station_5min_clean) AS station_cnt,
  d.zone_cnt,
  d.avg_occupancy,
  p.peak_hour,
  (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1) AS stuck_cnt,
  (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30) AS high_missing_cnt
FROM (
  SELECT DATE(ts_hour) AS dt,
         COUNT(DISTINCT taz_id) AS zone_cnt,
         AVG(occupancy_avg) AS avg_occupancy
  FROM dwd_zone_hour
  GROUP BY DATE(ts_hour)
) d
LEFT JOIN (
  SELECT dt, peak_hour
  FROM (
    SELECT DATE(ts_hour) AS dt,
           HOUR(ts_hour) AS peak_hour,
           AVG(occupancy_avg) AS occ,
           ROW_NUMBER() OVER (PARTITION BY DATE(ts_hour) ORDER BY AVG(occupancy_avg) DESC) AS rn
    FROM dwd_zone_hour
    GROUP BY DATE(ts_hour), HOUR(ts_hour)
  ) x
  WHERE rn = 1
) p ON p.dt = d.dt;
