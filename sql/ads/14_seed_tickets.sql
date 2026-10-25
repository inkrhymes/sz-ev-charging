-- 工单 stub：卡死→repair，高缺失→inspect
USE sz_ev;

INSERT INTO ads_ticket (station_id, type, status)
SELECT station_id,
       CASE WHEN stuck_flag=1 THEN 'repair' ELSE 'inspect' END,
       'open'
FROM dwd_quality_station
WHERE stuck_flag=1 OR missing_rate>0.30;
