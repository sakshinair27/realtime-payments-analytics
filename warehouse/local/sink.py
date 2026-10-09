"""Local stand-in for Snowpipe: Kafka `transactions` -> Postgres raw.raw_transactions.

Mirrors the Snowflake Kafka connector's SNOWPIPE behaviour closely enough that the dbt
models run unchanged on either warehouse:
  * buffers records and flushes every --flush-seconds (connector: buffer.flush.time),
  * writes RECORD_METADATA / RECORD_CONTENT in the connector's layout,
  * INGESTED_AT is defaulted by the database at load time, so every row in a flush
    shares one processing timestamp, like rows from one Snowpipe file,
  * commits Kafka offsets only after the load commits (at-least-once, so duplicates
    are possible and staging must dedupe).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import Json, execute_values


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
        user=os.getenv("PGUSER", "fintech"), password=os.getenv("PGPASSWORD", "fintech"),
        dbname=os.getenv("PGDATABASE", "fintech"),
    )


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--group", default="local-snowpipe")
    p.add_argument("--flush-seconds", type=float, default=10.0)
    p.add_argument("--flush-records", type=int, default=5000)
    args = p.parse_args(argv)

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    conn = None
    for _ in range(30):
        try:
            conn = pg_connect()
            break
        except psycopg2.OperationalError:
            time.sleep(2)
    if conn is None:
        print("sink: could not connect to Postgres", file=sys.stderr)
        return 1

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    buffer: list[tuple] = []
    last_flush = time.monotonic()
    loaded = 0
    print(f"sink: {args.bootstrap}/{args.topic} -> postgres raw.raw_transactions "
          f"(flush every {args.flush_seconds}s)", file=sys.stderr, flush=True)

    def flush():
        nonlocal buffer, last_flush, loaded
        if buffer:
            with conn, conn.cursor() as cur:
                execute_values(cur,
                               "insert into raw.raw_transactions (record_metadata, record_content) values %s",
                               buffer, page_size=1000)
            consumer.commit(asynchronous=False)
            loaded += len(buffer)
            print(f"sink: loaded batch of {len(buffer)} (total {loaded})", file=sys.stderr, flush=True)
        buffer = []
        last_flush = time.monotonic()

    while not stop:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"sink: kafka error {msg.error()}", file=sys.stderr, flush=True)
            else:
                ts_type, ts_ms = msg.timestamp()
                meta = {
                    "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                    "CreateTime": ts_ms, "key": msg.key().decode() if msg.key() else None,
                }
                try:
                    content = json.loads(msg.value())
                except (TypeError, ValueError):
                    content = {"__unparseable__": msg.value().decode(errors="replace")}
                buffer.append((Json(meta), Json(content)))
        if len(buffer) >= args.flush_records or time.monotonic() - last_flush >= args.flush_seconds:
            flush()

    flush()
    consumer.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
