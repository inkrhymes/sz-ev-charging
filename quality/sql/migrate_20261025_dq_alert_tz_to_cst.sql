-- 一次性迁移（2026-10-25）：dq_alert 旧数据 created_at/updated_at 是 UTC，统一 +8h 改成北京时间。
-- 此后 quality/engine.py 连接时 SET time_zone='+08:00'，新写入/更新的行直接是北京时间。只执行一次！
UPDATE dq_alert
   SET created_at = created_at + INTERVAL 8 HOUR,
       updated_at = updated_at + INTERVAL 8 HOUR
 WHERE created_at < '2026-10-25 04:00:00';
