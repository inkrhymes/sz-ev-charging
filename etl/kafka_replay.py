#!/usr/bin/env python3
"""Replay dwd/charge_5min_clean into Kafka topic `charge_5min` (key = station_id).

Reads one dt partition at a time (pyarrow), sorts by (ts, station_id) and sends one JSON message
per row:  {station_id, taz_id, ts (Beijing time "YYYY-MM-DD HH:MM:SS"); station_id/taz_id as strings, busy, idle, total_piles, occupancy, quality_flag}

  python etl/kafka_replay.py --start 2022-09-01 --end 2022-09-01 --dry-run out.jsonl
  python etl/kafka_replay.py --start 2022-09-01 --end 2022-09-07 --speed 288 --stations 1001,1002
  KAFKA_BOOTSTRAP=kafka:9092 python etl/kafka_replay.py --start 2022-09-01 --speed 0

--speed: simulated-time multiplier (288 = one day of data per 5 real minutes); 0 = as fast as possible.
--dry-run [FILE]: write JSONL to FILE (or stdout when no FILE / "-") instead of Kafka; needs no broker.
Kafka client (kafka-python-ng) is imported only when actually sending. Memory: ~one day (~0.5M rows).
"""
from __future__ import annotations
import argparse, json, math, os, sys, time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

ROOT = Path(__file__).resolve().parents[1]
LAKE = Path(os.environ.get("SZ_EV_LAKE", ROOT / "data" / "lake"))
TOPIC = "charge_5min"
PARTITIONS = 6
FIELDS = ["station_id", "taz_id", "ts", "busy", "idle", "total_piles", "occupancy", "quality_flag"]
BATCH = 50_000


def _num(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else v


def days(lake: Path, start: str | None, end: str | None) -> list[str]:
    d = lake / "dwd" / "charge_5min_clean"
    out = sorted(p.name.split("=", 1)[1] for p in d.glob("dt=*"))
    return [x for x in out if (not start or x >= start) and (not end or x <= end)]


def read_day(lake: Path, dt: str, stations: set[int] | None = None) -> pa.Table:
    dset = ds.dataset(lake / "dwd" / "charge_5min_clean" / f"dt={dt}", format="parquet")
    flt = ds.field("station_id").isin(sorted(stations)) if stations else None
    t = dset.to_table(columns=FIELDS, filter=flt)
    return t.take(pc.sort_indices(t, sort_keys=[("ts", "ascending"), ("station_id", "ascending")]))


def _int(x):
    x = _num(x)
    return None if x is None else int(x)


def iter_messages(t: pa.Table):
    """Yield (key_bytes, dict, ts) in table order; ts is naive Beijing time."""
    for b in t.to_batches(BATCH):
        cols = {c: b.column(c).to_pylist() for c in FIELDS}
        for i in range(b.num_rows):
            ts = cols["ts"][i]
            taz = _num(cols["taz_id"][i])
            v = {"station_id": str(int(cols["station_id"][i])),
                 "taz_id": None if taz is None else str(int(taz)),
                 "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                 "busy": _int(cols["busy"][i]), "idle": _int(cols["idle"][i]),
                 "total_piles": _int(cols["total_piles"][i]), "occupancy": _num(cols["occupancy"][i]),
                 "quality_flag": cols["quality_flag"][i]}
            yield str(v["station_id"]).encode(), v, ts


class JsonlSink:
    def __init__(self, fh):
        self.fh = fh

    def send(self, topic, key, value):
        self.fh.write(json.dumps(value, ensure_ascii=False) + "\n")

    def flush(self):
        self.fh.flush()


def make_producer(bootstrap: str):
    from kafka import KafkaProducer  # lazy: dry-run needs no client/broker
    from kafka.admin import KafkaAdminClient, NewTopic
    try:
        adm = KafkaAdminClient(bootstrap_servers=bootstrap)
        if TOPIC not in adm.list_topics():
            adm.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)])
        adm.close()
    except Exception as e:  # topic may already exist / be managed by compose
        print(f"topic check skipped: {e}", file=sys.stderr)
    return KafkaProducer(bootstrap_servers=bootstrap, linger_ms=20, batch_size=256 * 1024,
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
                         acks=1, compression_type="gzip")


def replay(lake: Path, producer, start=None, end=None, speed: float = 0.0, stations=None,
           sleep=time.sleep, clock=time.monotonic) -> int:
    n = 0; t0_sim = None; t0_real = None
    for dt in days(lake, start, end):
        t = read_day(lake, dt, stations)
        for key, val, ts in iter_messages(t):
            if speed > 0:
                if t0_sim is None:
                    t0_sim, t0_real = ts, clock()
                wait = (ts - t0_sim).total_seconds() / speed - (clock() - t0_real)
                if wait > 0:
                    producer.flush(); sleep(wait)
            producer.send(TOPIC, key=key, value=val)
            n += 1
        del t
        producer.flush()
    return n


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--speed", type=float, default=0.0)
    ap.add_argument("--stations", help="comma-separated station ids")
    ap.add_argument("--dry-run", nargs="?", const="-", metavar="FILE")
    a = ap.parse_args(argv)
    st = {int(x) for x in a.stations.split(",")} if a.stations else None
    t = time.time()
    if a.dry_run is not None:
        fh = sys.stdout if a.dry_run == "-" else open(a.dry_run, "w", encoding="utf-8")
        n = replay(LAKE, JsonlSink(fh), a.start, a.end, a.speed, st)
        if fh is not sys.stdout: fh.close()
    else:
        p = make_producer(os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        n = replay(LAKE, p, a.start, a.end, a.speed, st); p.close()
    el = time.time() - t
    print(f"sent {n} messages in {el:.1f}s ({n / max(el, 1e-9):,.0f} msg/s)", file=sys.stderr)
    return n


if __name__ == "__main__":
    main()
