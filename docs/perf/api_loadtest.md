# API 压测与 Redis 读缓存

## 怎么测的

- 被测对象：本仓库 `api/app.py`，用 gunicorn 起 4 个 worker（`gunicorn -w 4 -b 127.0.0.1:5056 app:app`），MySQL 模式（`API_SOURCE=mysql`，连 sz-ev-mysql）。没有动 5000 端口的共享容器。
- 压测工具：locust，脚本在 `perf/locustfile.py`。按前端真实使用比例分配权重：zone/series 10、realtime/zones 8、overview 5、map/zones 4、zones 3、alerts 3、quality 2、site/suggest 2、forecast/metrics 1。日期和片区每次随机取，模拟不同用户看不同数据。
- 两组场景：
  1. **常规负载**：50 个用户，每人请求间隔 0.5–2 秒，跑 60 秒。
  2. **极限压力**：50 个用户，不等待（`LOCUST_NO_WAIT=1`），跑 30 秒，看吞吐上限。
- 每组分别在“无缓存”和“开 Redis 缓存”下各跑一次。汇总表用 `python perf/summarize.py perf/out/nocache perf/out/cache` 生成（`perf/out/` 不入库）。
- 复现命令：

```bash
cd api && REDIS_URL=redis://127.0.0.1:6380/0 API_SOURCE=mysql gunicorn -w 4 -b 127.0.0.1:5056 app:app &
locust -f perf/locustfile.py --headless -u 50 -r 10 -t 60s --host http://127.0.0.1:5056 --csv perf/out/cache
```

延迟单位都是毫秒，所有场景错误率都是 0。

## 结果一：常规负载（50 用户，60 秒）

| 接口 | nocache QPS | P50 | P95 | P99 | 错误率 | cache QPS | P50 | P95 | P99 | 错误率 |
|---|---|---|---|---|---|---|---|---|---|---|
| /api/alerts | 2.7 | 19 | 34 | 42 | 0.00% | 3.0 | 3 | 5 | 30 | 0.00% |
| /api/forecast/metrics | 1.0 | 11 | 19 | 49 | 0.00% | 0.9 | 2 | 3 | 18 | 0.00% |
| /api/map/zones | 4.1 | 21 | 37 | 47 | 0.00% | 4.5 | 2 | 3 | 6 | 0.00% |
| /api/overview | 5.5 | 11 | 20 | 36 | 0.00% | 5.3 | 2 | 11 | 16 | 0.00% |
| /api/quality | 2.0 | 11 | 20 | 32 | 0.00% | 1.9 | 2 | 3 | 6 | 0.00% |
| /api/realtime/zones | 7.5 | 11 | 22 | 35 | 0.00% | 8.1 | 2 | 4 | 14 | 0.00% |
| /api/site/suggest | 1.8 | 12 | 28 | 39 | 0.00% | 1.9 | 2 | 5 | 12 | 0.00% |
| /api/zone/series | 10.3 | 11 | 25 | 34 | 0.00% | 10.6 | 13 | 16 | 18 | 0.00% |
| /api/zones | 4.1 | 14 | 38 | 47 | 0.00% | 4.2 | 2 | 10 | 71 | 0.00% |
| Aggregated | 39.0 | 12 | 28 | 41 | 0.00% | 40.4 | 3 | 14 | 22 | 0.00% |

QPS 基本由用户的思考时间决定，所以开不开缓存差别不大。差别在延迟上：P50 从 12ms 降到 3ms，P99 从 41ms 降到 22ms。zone/series 的日期和片区参数组合多，命中率低，所以缓存后 P50 还在 13ms 左右。

## 结果二：极限压力（50 用户不等待，30 秒）

| 接口 | nocache_stress QPS | P50 | P95 | P99 | 错误率 | cache_stress QPS | P50 | P95 | P99 | 错误率 |
|---|---|---|---|---|---|---|---|---|---|---|
| /api/alerts | 25.5 | 150 | 190 | 230 | 0.00% | 107.7 | 34 | 44 | 58 | 0.00% |
| /api/forecast/metrics | 9.2 | 150 | 180 | 210 | 0.00% | 34.4 | 32 | 43 | 52 | 0.00% |
| /api/map/zones | 36.0 | 150 | 190 | 220 | 0.00% | 147.7 | 33 | 44 | 57 | 0.00% |
| /api/overview | 41.7 | 140 | 180 | 210 | 0.00% | 179.6 | 33 | 43 | 59 | 0.00% |
| /api/quality | 16.6 | 140 | 180 | 210 | 0.00% | 72.2 | 32 | 43 | 59 | 0.00% |
| /api/realtime/zones | 68.0 | 140 | 180 | 220 | 0.00% | 292.5 | 33 | 43 | 56 | 0.00% |
| /api/site/suggest | 17.6 | 150 | 180 | 220 | 0.00% | 73.8 | 33 | 43 | 52 | 0.00% |
| /api/zone/series | 84.7 | 140 | 180 | 220 | 0.00% | 367.6 | 36 | 50 | 62 | 0.00% |
| /api/zones | 29.3 | 140 | 180 | 220 | 0.00% | 111.3 | 33 | 43 | 57 | 0.00% |
| Aggregated | 328.7 | 150 | 180 | 220 | 0.00% | 1386.7 | 33 | 46 | 59 | 0.00% |

吞吐从约 329 QPS 提到约 1387 QPS（约 4.2 倍），P50 从 150ms 降到 33ms。跑完后 Redis 统计是 38,634 次命中、5,408 次未命中，命中率约 88%，内存只用了 16MB（上限 128MB）。

## 缓存怎么设计的

- 默认关闭。只有设置了 `REDIS_URL` 才启用；不设时行为和以前完全一样，也不会出现 `X-Cache` 头。`redis` 包是延迟导入的，没装也能跑。
- 只缓存 GET，且只缓存 200 的 JSON、并且没有 `mysql_error` 的响应。出错或降级的结果不会被缓存。
- key 格式是 `szev:{mysql|file}:{路径}?{排序后的查询参数}`，不同参数各自一份。
- 过期时间（TTL）：
  - 实时类 `/api/realtime/zones` 5 秒。
  - 告警和工单 10 秒。
  - 历史类（overview、zones、zone/series、quality、map、site/suggest、forecast/metrics 等）60 秒，可以用 `CACHE_TTL_DEFAULT` 调整。
  - `/api/health` 不缓存。
- 响应头 `X-Cache: HIT/MISS` 方便排查问题。
- 写操作（POST/PATCH/PUT/DELETE）成功后会主动清缓存：
  - 改告警时清掉告警、工单和 overview 的缓存。
  - 改工单时清掉工单和告警的缓存。
- Redis 连不上时（超时 0.2 秒）直接查库，并在 30 秒内不再尝试连接，所以 Redis 挂掉不会拖垮 API。

## 失效方式的取舍

- **按 TTL 过期，简单但会有短暂旧数据**。实时数据最多旧 5 秒，和 5 分钟一次的数据粒度相比可以接受。历史数据是每天跑批写入的，最多旧 60 秒，影响很小。
- **跑批写入 MySQL 后不会主动清缓存**。Spark 或 Airflow 直接写库，API 并不知道，所以新数据要等 TTL 过期才会出现，最多 60 秒。如果以后要求跑批完成后立刻可见，可以在 DAG 最后加一步 `redis-cli --scan --pattern 'szev:*' | xargs redis-cli del`，或者直接 FLUSHDB。
- **写操作按前缀删除**用的是 `SCAN`，key 数量少时很快；key 多到几十万时会变慢，到那时可以改成带版本号的 key。
- **多个 worker 共用一个 Redis**，所以不会出现各 worker 本地缓存不一致的问题。代价是多一个要运维的组件，好在它是可选的。
- 内存上限是 128MB，用 allkeys-lru 淘汰。目前只用了 16MB，余量很大。

## 机器规格说明（看数字前请先读）

- 测试机器是 8 核、16GB 内存，**和其他 agent 共用**，测试时还有别人在用 pandas 训练模型。
- locust 也跑在同一台机器上，会和 API、MySQL 抢 CPU。
- 所以绝对值只能作参考，主要看开缓存前后的相对差别。换到独立服务器、并把 locust 放到另一台机器上，数字会不一样。
- 数据量是当前库里的测试数据。数据变多以后，不开缓存的延迟会上升，缓存的收益只会更大。

## 在 compose 里开启 Redis

```bash
REDIS_URL=redis://redis:6379/0 docker compose --profile cache up -d --build redis api
```

不加 `--profile cache` 时 redis 服务不会启动。`REDIS_URL` 默认为空，api 的行为和以前一样。
