-- 2026-10-25：工单关联质检告警（dq_alert.alert_id），前端「派工单」写入
ALTER TABLE ads_ticket ADD COLUMN alert_id BIGINT NULL AFTER station_id, ADD KEY idx_alert_id (alert_id);
