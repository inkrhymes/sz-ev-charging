# 验收清单（Acceptance Checklist）

- 验收时间：2026-10-25 09:30（Asia/Shanghai）
- 验收环境：Linux 箱，Docker 26.1.5，Docker Compose 2.26.1
- 验收方式：`bash scripts/stop.sh --purge` 清空后执行 `bash scripts/start.sh`，再逐项执行下列命令
- 结论：**31 项全部通过 ✅**。另有 2 个未解决问题，见文末「遗留问题」。

约定：`B=http://localhost:5000`；MySQL 查询前缀记为 `Q="docker exec sz-ev-mysql mysql -uev -pevpass sz_ev -N -e"`。

## 一、环境

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| Docker | `docker --version` | ≥ 20 | ✅ 26.1.5 |
| Compose | `docker compose version` | v2 | ✅ 2.26.1 |
| 配置文件 | `cp .env.example .env` | 文件存在 | ✅ |

## 二、启动

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 一键启动 | `bash scripts/start.sh` | 退出码 0，输出「完成」 | ✅ 退出码 0，约 18 s |
| MySQL 健康 | `docker ps --filter name=sz-ev-mysql` | healthy | ✅ healthy |
| API 容器 | `docker ps --filter name=sz-ev-api` | Up | ✅ Up |
| 探活 | `curl $B/api/health` | `ok=true, mode=file` | ✅ `mode=file` |

## 三、数据分层（MySQL 行数）

| 表 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| ods_station_info | `$Q "SELECT COUNT(*) FROM ods_station_info"` | 25 | ✅ 25 |
| ods_station_raw_5min | 同上 | 100,800 | ✅ 100800 |
| dwd_station_5min_clean | 同上 | 100,800 | ✅ 100800 |
| dwd_quality_station | 同上 | 25 | ✅ 25 |
| dwd_zone_hour | 同上 | > 0 | ✅ 5554 |
| ads_city_overview | 同上 | 14（天） | ✅ 14 |
| ads_zone_occupancy_hour | 同上 | = dwd_zone_hour | ✅ 5554 |
| ads_quality_summary | 同上 | 4 | ✅ 4 |
| ads_ticket | 同上 | 10 | ✅ 10 |

说明：`ods_calendar_day`、`ods_weather_zone_h`、`ads_baseline_naive` 为预留表，当前 0 行，基线由 API 实时计算。

## 四、质检数字（对照 `docs/quality_report.md`）

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 站点数 | `curl "$B/api/overview?dt=2022-09-01"` | 25 | ✅ 25 |
| 片区数 | 同上 `zone_cnt` | 23 | ✅ 23 |
| 卡死站 | 同上 `stuck_cnt` | 4 | ✅ 4 |
| 高缺失站 | 同上 `high_missing_cnt` | 6 | ✅ 6 |
| 整行缺失率 | `curl $B/api/quality` | 0.2642（26631/100800） | ✅ 0.2642 |
| 入库一致性 | `$Q "SELECT metric,value FROM ads_quality_summary"` | 同上 | ✅ 25 / 4 / 6 / 0.2642 |

## 五、接口

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 片区时序 | `curl "$B/api/zone/series?taz_id=1011&from=2022-09-01&to=2022-09-02"` | 48 个小时点 | ✅ 48（09-01 00:00～09-02 23:00） |
| 片区时序（不传时间） | `curl "$B/api/zone/series?taz_id=1011"` | 默认 168 点 | ✅ 168 |
| 质检汇总 | `curl $B/api/quality` | 4 项指标 | ✅ 4 项 |
| 跨域预检 | `curl -X OPTIONS $B/api/tickets -i` | 204/200 且带 CORS 头 | ✅ |

## 六、大屏

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 同源托管 | `curl -i $B/` | 200 text/html | ✅ 200，标题「深圳充电站运行管理可视化（演示）」 |
| 浏览器打开 | 浏览器访问 `http://localhost:5000` | KPI 显示 25/23/4/6 | ✅ 接口数据一致（大屏默认请求 `http://127.0.0.1:5000`） |

## 七、扩展接口（工单 / 登录 / 基线）

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 工单列表 | `curl $B/api/tickets` | 10 条，repair 4 / inspect 6 | ✅ 10 条，4 / 6 |
| 工单流转 | `curl -X PATCH -H 'Content-Type: application/json' -d '{"status":"processing"}' $B/api/tickets/1` | 200，status=processing | ✅ ticket 1：open→processing |
| 非法状态 | 同上，`-d '{"status":"x"}'` | ok=false | ✅ 报错 `status must be open\|processing\|done` |
| 只读登录 | `curl -X POST -H 'Content-Type: application/json' -d '{"user":"demo","role":"readonly"}' $B/api/auth/login` | 返回 token | ✅ `demo-readonly` |
| 朴素基线 | `curl "$B/api/baseline?taz_id=1011&from=2022-09-01&to=2022-09-02"` | 48 点，含 y_true / y_hat_lag1 / abs_err | ✅ 48 点 |

## 八、停止与清理

| 项目 | 验收方法 | 期望 | 实测结果 |
|---|---|---|---|
| 停止 | `bash scripts/stop.sh` | 容器移除 | ✅ |
| 彻底清理 | `bash scripts/stop.sh --purge` | 容器与数据卷均为 0 | ✅ 0 个容器 / 0 个卷 |
| 清理后重启 | `bash scripts/start.sh` | 重新可用 | ✅ health 正常 |

## 遗留问题

1. **`start.sh` 不支持重复执行**：数据卷保留时再次执行，`sql/ads/10_metrics_overview.sql` 会报 `ERROR 1062 Duplicate entry '2022-09-01' for key 'ads_city_overview.PRIMARY'`。目前的规避方法是先执行 `scripts/stop.sh --purge`。建议把回填 SQL 改为 `REPLACE INTO`，或者先 `TRUNCATE`（该文件归 @小黑 负责）。
2. **API 仍为 file 模式**：MySQL 已装载，但接口读取的是 `data/demo/dwd/*.csv`，`mode=mysql` 尚未实现。两边数字一致。
3. **工单状态只存在内存里**：PATCH 后的状态在 API 重启后会恢复成初始值，作为 stub 可以接受。

本次修复（只改了 `api/app.py`）：新增 `/` 路由，直接托管 `web/index.html`，此前返回 404；`zone/series` 和 `baseline` 在 file 模式下开始遵守 `from`/`to` 参数，此前会忽略这两个参数。

## 本机复现步骤（Windows）

1. 安装 Docker Desktop，启用 WSL 2 后端并启动它；安装 Git for Windows（自带 Git Bash）。
2. 打开 Git Bash 或 WSL 终端：
   ```bash
   git clone https://github.com/inkrhymes/sz-ev-charging.git
   cd sz-ev-charging
   cp .env.example .env
   bash scripts/stop.sh --purge   # 首次可跳过，重复执行前必须做
   bash scripts/start.sh
   ```
3. 在浏览器打开 `http://localhost:5000`（大屏）和 `http://localhost:5000/api/health`（探活）。
4. 结束：`bash scripts/stop.sh`。如果 5000 端口被占用，在 `.env` 中修改 `API_PORT`，同时在浏览器控制台执行 `localStorage.API_BASE='http://127.0.0.1:新端口'`。
5. 如果 Git Bash 报 `\r` 错误，执行 `git config --global core.autocrlf input` 后重新克隆。
