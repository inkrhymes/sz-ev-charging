-- 2026-10-25：工单开始处理时间。PATCH /api/tickets/<id> 首次改成 processing 时写 NOW()，前端时间线用；已有工单保持 NULL
ALTER TABLE ads_ticket ADD COLUMN started_at DATETIME NULL AFTER status;
