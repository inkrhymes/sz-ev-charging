-- 上一时刻 naive 基线 → ads_baseline_naive
USE sz_ev;

-- 幂等：重跑前清空
TRUNCATE TABLE ads_baseline_naive;
INSERT INTO ads_baseline_naive (taz_id, ts_hour, y_true, y_hat_lag1, abs_err)
SELECT
  taz_id,
  ts_hour,
  occupancy AS y_true,
  LAG(occupancy) OVER (PARTITION BY taz_id ORDER BY ts_hour) AS y_hat_lag1,
  ABS(occupancy - LAG(occupancy) OVER (PARTITION BY taz_id ORDER BY ts_hour)) AS abs_err
FROM ads_zone_occupancy_hour
ON DUPLICATE KEY UPDATE
  y_true=VALUES(y_true),
  y_hat_lag1=VALUES(y_hat_lag1),
  abs_err=VALUES(abs_err);
-- 首行 lag 为空，查询时 WHERE y_hat_lag1 IS NOT NULL
