"""片区准实时占用率：Spark Structured Streaming。

输入（二选一）：
  --source kafka  topic charge_5min（6 分区，key=station_id），KAFKA_BOOTSTRAP 默认 localhost:9092
  --source file   --input DIR，目录里的 JSON lines（每行一条消息，测试/无 Kafka 时用）
消息 value（JSON）：station_id, taz_id, ts(北京时间 'YYYY-MM-DD HH:MM:SS'), busy, idle, total_piles, occupancy, quality_flag
窗口：按 taz_id，15 分钟滑动窗口、5 分钟步长，watermark 10 分钟。
输出：foreachBatch 按 (taz_id, window_end) upsert 到 MySQL rt_zone_occupancy（--sink mysql），
      或写 CSV 到 --out 目录（--sink csv，测试用），或打印（--sink console）。

坏数据过滤：quality_flag 属于 SZ_RT_BAD_FLAGS（逗号分隔，默认 "outlier,dropped_src"，大小写不敏感）的消息丢弃；
设为空字符串则不过滤。坏消息不参与占用率，但按标记计数到 outlier_cnt / dropped_src_cnt。
total_piles<=0 或 busy 为空的消息直接丢弃（不计数）。
"""
from __future__ import annotations

import argparse
import os
import sys

TOPIC_DEFAULT = "charge_5min"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def bad_flags() -> list[str]:
    raw = os.getenv("SZ_RT_BAD_FLAGS", "outlier,dropped_src")
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


def message_schema():
    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructField, StructType
    return StructType([
        StructField("station_id", StringType()),
        StructField("taz_id", StringType()),
        StructField("ts", StringType()),
        StructField("busy", IntegerType()),
        StructField("idle", IntegerType()),
        StructField("total_piles", IntegerType()),
        StructField("occupancy", DoubleType()),
        StructField("quality_flag", StringType()),
    ])


def build_spark(source: str, app="sz-rt-zone-occupancy"):
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName(app)
         .master(os.getenv("SZ_SPARK_MASTER", "local[1]"))
         .config("spark.driver.memory", os.getenv("SZ_DRIVER_MEM", "1g"))
         .config("spark.sql.shuffle.partitions", os.getenv("SZ_RT_SHUFFLE", "4"))
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .config("spark.ui.enabled", "false"))
    if source == "kafka":
        b = b.config("spark.jars.packages", os.getenv("SZ_KAFKA_PKG", KAFKA_PKG))
    return b.getOrCreate()


def read_messages(spark, source: str, input_dir: str | None = None, topic: str = TOPIC_DEFAULT):
    """返回带 value 字段解析后的 DataFrame + ingest_ts（进入 Spark 的时间）。"""
    from pyspark.sql import functions as F
    schema = message_schema()
    if source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
               .option("subscribe", topic)
               .option("startingOffsets", os.getenv("SZ_RT_STARTING_OFFSETS", "latest"))
               .option("failOnDataLoss", "false")
               .option("maxOffsetsPerTrigger", os.getenv("SZ_RT_MAX_OFFSETS", "200000"))
               .load())
        df = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("m")).select("m.*")
    elif source == "file":
        if not input_dir:
            raise SystemExit("--source file 需要 --input DIR")
        df = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 50).json(input_dir))
    else:
        raise SystemExit(f"未知 source: {source}")
    return df.withColumn("ingest_ts", F.current_timestamp())


def transform(df, watermark="10 minutes", window="15 minutes", slide="5 minutes"):
    from pyspark.sql import functions as F
    df = df.withColumn("event_ts", F.to_timestamp("ts", "yyyy-MM-dd HH:mm:ss"))
    cond = (F.col("event_ts").isNotNull() & F.col("taz_id").isNotNull() & F.col("busy").isNotNull()
            & (F.col("total_piles") > 0))
    flags = bad_flags()
    flag = F.lower(F.trim(F.col("quality_flag")))
    good = F.lit(True) if not flags else (F.col("quality_flag").isNull() | ~flag.isin(flags))
    df = df.where(cond).withColumn("_good", good).withColumn("_flag", flag)
    w = F.window("event_ts", window, slide)
    g = F.col("_good")
    agg = (df.withWatermark("event_ts", watermark)
           .groupBy(F.col("taz_id"), w)
           .agg(F.sum(F.when(g, F.col("busy")).otherwise(0)).alias("busy_sum"),
                F.sum(F.when(g, F.col("total_piles")).otherwise(0)).alias("piles_sum"),
                F.approx_count_distinct(F.when(g, F.col("station_id"))).alias("station_cnt"),
                F.sum(F.when(g, 1).otherwise(0)).alias("msg_cnt"),
                F.sum(F.when(~g & (F.col("_flag") == "outlier"), 1).otherwise(0)).alias("outlier_cnt"),
                F.sum(F.when(~g & (F.col("_flag") == "dropped_src"), 1).otherwise(0)).alias("dropped_src_cnt"),
                F.max("ingest_ts").alias("ingest_ts_max")))
    occ = F.when(F.col("piles_sum") > 0, F.col("busy_sum") / F.col("piles_sum"))
    return agg.select("taz_id", F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      occ.alias("occupancy"), "busy_sum", "piles_sum", "station_cnt", "msg_cnt",
                      "outlier_cnt", "dropped_src_cnt", "ingest_ts_max")


UPSERT_SQL = (
    "INSERT INTO rt_zone_occupancy (taz_id, window_start, window_end, occupancy, busy_sum, piles_sum, station_cnt,"
    " msg_cnt, outlier_cnt, dropped_src_cnt, ingest_ts_max, write_ts, latency_ms, batch_id)"
    " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) AS new"
    " ON DUPLICATE KEY UPDATE window_start=new.window_start, occupancy=new.occupancy, busy_sum=new.busy_sum,"
    " piles_sum=new.piles_sum, station_cnt=new.station_cnt, msg_cnt=new.msg_cnt, outlier_cnt=new.outlier_cnt,"
    " dropped_src_cnt=new.dropped_src_cnt, ingest_ts_max=new.ingest_ts_max,"
    " write_ts=new.write_ts, latency_ms=new.latency_ms, batch_id=new.batch_id")


def to_rows(pdf_records, batch_id: int, now):
    rows = []
    for r in pdf_records:
        ing = r["ingest_ts_max"]
        lat = int((now - ing).total_seconds() * 1000) if ing is not None else None
        occ = None if r["occupancy"] is None else round(float(r["occupancy"]), 6)
        rows.append((str(r["taz_id"]), r["window_start"], r["window_end"], occ, int(r["busy_sum"]), int(r["piles_sum"]),
                     int(r["station_cnt"]), int(r["msg_cnt"]), int(r.get("outlier_cnt") or 0),
                     int(r.get("dropped_src_cnt") or 0), ing, now, lat, int(batch_id)))
    return rows


def mysql_writer(batch_df, batch_id):
    import datetime as dt
    import pymysql
    recs = [r.asDict() for r in batch_df.collect()]  # 每批只有 窗口数×片区数 行（≤ 331×3+），collect 到 driver 可接受
    if not recs:
        return
    now = dt.datetime.now()  # session 时区=Asia/Shanghai，Spark 返回的 naive datetime 也是北京时间
    rows = to_rows(recs, batch_id, now)
    conn = pymysql.connect(host=os.getenv("MYSQL_HOST", "127.0.0.1"), port=int(os.getenv("MYSQL_PORT", "3306")),
                           user=os.getenv("MYSQL_USER", "ev"), password=os.getenv("MYSQL_PASSWORD", "evpass"),
                           database=os.getenv("MYSQL_DATABASE", "sz_ev"), charset="utf8mb4",
                           init_command="SET time_zone='+08:00'", autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
        conn.commit()
    finally:
        conn.close()


def csv_writer(out_dir: str):
    import csv
    import datetime as dt
    os.makedirs(out_dir, exist_ok=True)

    def _w(batch_df, batch_id):
        recs = [r.asDict() for r in batch_df.collect()]
        if not recs:
            return
        rows = to_rows(recs, batch_id, dt.datetime.now())
        with open(os.path.join(out_dir, f"batch_{batch_id:06d}.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["taz_id", "window_start", "window_end", "occupancy", "busy_sum", "piles_sum", "station_cnt",
                        "msg_cnt", "outlier_cnt", "dropped_src_cnt", "ingest_ts_max", "write_ts", "latency_ms", "batch_id"])
            w.writerows(rows)
    return _w


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["kafka", "file"], default="kafka")
    ap.add_argument("--input", help="file 模式：JSON lines 目录")
    ap.add_argument("--topic", default=os.getenv("SZ_RT_TOPIC", TOPIC_DEFAULT))
    ap.add_argument("--sink", choices=["mysql", "csv", "console"], default="mysql")
    ap.add_argument("--out", help="csv sink 输出目录")
    ap.add_argument("--checkpoint", default=os.getenv("SZ_RT_CHECKPOINT", "/workspace/sz-rt-checkpoint/zone_occupancy"))
    ap.add_argument("--trigger", default="30 seconds", help="micro-batch 间隔")
    ap.add_argument("--once", action="store_true", help="availableNow：处理完现有数据就退出（测试/补数）")
    ap.add_argument("--output-mode", choices=["update", "append"], default="update")
    a = ap.parse_args(argv)

    spark = build_spark(a.source)
    spark.sparkContext.setLogLevel("WARN")
    out = transform(read_messages(spark, a.source, a.input, a.topic))
    if a.sink == "mysql":
        fn = mysql_writer
    elif a.sink == "csv":
        if not a.out:
            raise SystemExit("--sink csv 需要 --out DIR")
        fn = csv_writer(a.out)
    else:
        def fn(df, bid):
            print(f"--- batch {bid}"); df.orderBy("taz_id", "window_end").show(50, truncate=False)
    w = (out.writeStream.outputMode(a.output_mode).foreachBatch(fn).option("checkpointLocation", a.checkpoint))
    w = w.trigger(availableNow=True) if a.once else w.trigger(processingTime=a.trigger)
    q = w.start()
    q.awaitTermination()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
