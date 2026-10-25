-- 片区小时占用 → ads_zone_occupancy_hour
USE sz_ev;

-- 幂等：重跑前清空
TRUNCATE TABLE ads_zone_occupancy_hour;
INSERT INTO ads_zone_occupancy_hour (taz_id, ts_hour, occupancy, load_kw_est)
SELECT
  z.taz_id,
  z.ts_hour,
  z.occupancy_avg AS occupancy,
  ROUND(z.busy_sum * IFNULL(
    (SELECT AVG(rated_power_kw) FROM ods_station_info i
     WHERE i.taz_id=z.taz_id AND i.rated_power_kw IS NOT NULL), 7), 2) AS load_kw_est
FROM dwd_zone_hour z
ON DUPLICATE KEY UPDATE
  occupancy=VALUES(occupancy),
  load_kw_est=VALUES(load_kw_est);
