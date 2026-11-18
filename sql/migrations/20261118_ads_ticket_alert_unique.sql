-- 2026-11-18：同一质检告警最多只能关联一张工单。
-- 可重复执行：已存在 uk_alert_id 时不重复创建；旧的非唯一 idx_alert_id 会被移除。
-- 如果已有重复的非 NULL alert_id，本迁移会失败并提示数据需要先人工清理，避免静默丢工单。
USE sz_ev;

SET @has_unique := (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 'ads_ticket' AND index_name = 'uk_alert_id'
);
SET @sql := IF(@has_unique = 0,
  'ALTER TABLE ads_ticket ADD UNIQUE KEY uk_alert_id (alert_id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @has_old := (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE() AND table_name = 'ads_ticket' AND index_name = 'idx_alert_id'
);
SET @sql := IF(@has_old = 1,
  'ALTER TABLE ads_ticket DROP INDEX idx_alert_id',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
