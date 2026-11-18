-- 2026-11-18：告警列表按 created_at DESC, alert_id DESC 查询，增加排序覆盖索引。
-- 可重复执行；旧库已存在索引时跳过。
USE sz_ev;

SET @has_idx := (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 'dq_alert' AND index_name = 'idx_created_at_id'
);
SET @sql := IF(@has_idx = 0,
  'ALTER TABLE dq_alert ADD KEY idx_created_at_id (created_at, alert_id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
