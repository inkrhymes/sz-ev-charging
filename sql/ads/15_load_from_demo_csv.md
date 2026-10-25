# 从演示 CSV 灌库（小黑）

前置：`docker compose up -d`，库名 `sz_ev`，已执行 `sql/ods|dwd|ads` 建表。

```bash
# 在仓库根目录
python etl/load_mysql_from_demo.py
# 然后执行指标 SQL（或脚本内已含 ads 回填）
```

质检口径以 `docs/quality_report.md` 为准（14 天窗实测）：缺失率 26.42%，卡死 4，高缺失 6。
MOCK / 大屏假数据请改成这组数，避免和录屏对不上。
