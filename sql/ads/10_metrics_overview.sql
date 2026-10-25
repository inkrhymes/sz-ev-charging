-- 城市日概览（示例：按演示子集 dwd 回填）
INSERT INTO ads_city_overview (dt, station_cnt, zone_cnt, avg_occupancy, peak_hour, stuck_cnt, high_missing_cnt)
SELECT
  DATE(z.ts_hour) AS dt,
  (SELECT COUNT(DISTINCT station_id) FROM dwd_station_5min_clean) AS station_cnt,
  COUNT(DISTINCT z.taz_id) AS zone_cnt,
  AVG(z.occupancy_avg) AS avg_occupancy,
  (SELECT HOUR(ts_hour) FROM dwd_zone_hour z2
    WHERE DATE(z2.ts_hour)=DATE(z.ts_hour)
    GROUP BY HOUR(ts_hour) ORDER BY AVG(occupancy_avg) DESC LIMIT 1) AS peak_hour,
  (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1) AS stuck_cnt,
  (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30) AS high_missing_cnt
FROM dwd_zone_hour z
GROUP BY DATE(z.ts_hour);
