# 备份与恢复

机器重启后 MySQL 数据需要重新导入过一次，所以补了一套一键备份和恢复脚本。

## 备份什么

| 内容 | 文件 | 说明 |
|---|---|---|
| MySQL `sz_ev` 全库 | `mysql_sz_ev.sql.gz` | 在容器 `sz-ev-mysql` 里跑 `mysqldump --single-transaction`，不锁表，含存储过程和触发器 |
| 各表精确行数 | `mysql_rowcounts.tsv` | 备份当时的 `COUNT(*)`，恢复后拿来核对 |
| 数据湖 | `lake.tar.gz` | `data/lake` 下 ods/dwd/ads/_runs 全部保留，只跳过 `_tmp`、`_staging` 临时目录 |
| 校验和与说明 | `SHA256SUMS`、`MANIFEST` | 恢复前先校验文件没坏；MANIFEST 记录时间、湖路径、git 版本 |

输出目录是 `/workspace/backups/<时间戳>/`（可用 `BACKUP_ROOT` 改）。轮转只保留最近 3 份（`KEEP=3`），只针对以时间戳开头的目录；改过名的目录（例如 `pre-occupancy-fix_*`）不会被轮转删掉，适合留作长期回滚点。仓库里的 `backups/` 已加入 `.gitignore`。湖路径取 `SZ_EV_LAKE`，没设置时用 `<仓库>/data/lake`。

## 用法

```bash
bash scripts/backup.sh                    # 库 + 湖，约 13 秒
bash scripts/backup.sh --db-only          # 只备份库
bash scripts/restore.sh latest            # 用最近一份覆盖恢复 sz_ev
bash scripts/restore.sh /workspace/backups/<目录> --target-db sz_ev_copy   # 恢复到别的库名
bash scripts/restore.sh <目录> --lake      # 同时还原数据湖（原目录先改名为 lake.bak.<时间戳>）
bash scripts/backup_verify.sh [<目录>]     # 恢复演练：恢复到临时库、逐表比对行数、删掉临时库
```

## 实测结果（2026-10-25 14:43，北京时间）

- 第一次演练（14:43）：备份约 13 秒，库 dump 压缩后 13M，数据湖 112M（1124 个文件），恢复后行数全部一致。
- 2026-10-25 15:10 再演练一次（告警重建、预测入库之后）：`/workspace/backups/20261025T151051`，备份约 14 秒，库 dump 22M，数据湖 146M（1181 个文件）。
  恢复到 `sz_ev_restore_test` 约 29 秒，21 张表行数全部一致，例如 `ads_zone_occupancy_hour` 1,338,568、`ads_forecast_zone_hour` 222,432、`dq_alert` 651、`dq_run_log` 9,966、`ops_task_run` 1,263、`ads_ticket` 8。
- 这次演练发现一个问题：库里的视图（`v_zone_site_base` 等）定义者是 `ev`，恢复到别的库名时 `ev` 没权限，建视图失败，而错误被吞掉了。已修：`restore.sh` 导入时去掉视图的 DEFINER，并且导入出错会直接报错退出。
- 比对完成后临时库已删除，正式库 `sz_ev` 没有受影响。
- 这份备份保留为 `/workspace/backups/pre-occupancy-fix_20261025T144314`，是 dwd 占用率修复之前的回滚点。

## 注意

- 恢复会覆盖目标库里的同名表；恢复正式库前建议先再备份一次。
- 别的任务正在写湖或库时不要备份，最好在每天调度跑完后执行。
