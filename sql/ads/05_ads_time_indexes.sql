-- 按时间范围跨片区查询（城市小时曲线、基线当日误差）用的覆盖索引。
-- 主键 (taz_id, ts_hour) 只适合“单片区 + 时间范围”的查询。
-- 实测：只建 (ts_hour) 单列索引会走索引后回表，Q1 反而变慢，所以带上查询列做成覆盖索引。见 docs/perf/spark_baseline.md。
ALTER TABLE ads_zone_occupancy_hour ADD INDEX idx_zoh_ts_occ (ts_hour, occupancy);
ALTER TABLE ads_baseline_naive ADD INDEX idx_bl_ts_err (ts_hour, abs_err);
