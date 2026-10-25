-- 质检数字 → ads_quality_summary（演示子集跑完后执行）
USE sz_ev;

REPLACE INTO ads_quality_summary (metric, value, note) VALUES
('demo_station_cnt',
  (SELECT COUNT(*) FROM dwd_quality_station),
  '演示子集站数'),
('row_missing_rate',
  (SELECT AVG(missing_rate) FROM dwd_quality_station),
  '站均缺失率；也可改为全量子集加权'),
('stuck_station_cnt',
  (SELECT COUNT(*) FROM dwd_quality_station WHERE stuck_flag=1),
  '卡死站'),
('high_missing_station_cnt',
  (SELECT COUNT(*) FROM dwd_quality_station WHERE missing_rate>0.30),
  'missing_rate>0.30');
