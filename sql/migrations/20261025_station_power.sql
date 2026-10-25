-- 2026-10-25 站点功率：dim_station 加 4 列，ads_zone_occupancy_hour 加 power_coverage。
-- 已有库执行一次即可（export_ads_parquet.py 连库时也会自动检查并补列）。新库由 sql/ads/03、04 建表时直接带上。
ALTER TABLE dim_station
  ADD COLUMN rated_power_kw_total DECIMAL(12,2) NULL,
  ADD COLUMN dc_pile_cnt INT NULL,
  ADD COLUMN ac_pile_cnt INT NULL,
  ADD COLUMN pile_cnt_from_power INT NULL;
ALTER TABLE ads_zone_occupancy_hour
  ADD COLUMN power_coverage DECIMAL(7,6) NULL;
