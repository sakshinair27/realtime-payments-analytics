"""Load a deterministic batch of generator events straight into raw.raw_transactions.

Used by CI to test dbt without Kafka: rows are written in the same layout the Kafka
connector / local sink produce (RECORD_METADATA + RECORD_CONTENT, INGESTED_AT defaulted
at load time), in several committed batches like real loader flushes, so duplicates and
late events reach dbt exactly as they would in production.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generator.generator import TransactionSimulator

PARTITIONS = 3


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--events", type=int, default=6000, help="~20 minutes at 300/min")
    p.add_argument("--batch", type=int, default=500, help="rows per committed load")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-time", default="2026-01-01T00:00:00Z")
    args = p.parse_args(argv)

    start = datetime.fromisoformat(args.start_time.replace("Z", "+00:00")).astimezone(UTC)
    sim = TransactionSimulator(args.seed, start, rate_per_min=300, dup_rate=0.01, late_rate=0.03)
    offsets = [0] * PARTITIONS

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"), port=int(os.getenv("PGPORT", "5433")),
        user=os.getenv("PGUSER", "fintech"), password=os.getenv("PGPASSWORD", "fintech"),
        dbname=os.getenv("PGDATABASE", "fintech"),
    )
    rows, loaded = [], 0
    for _ in range(args.events):
        deliver_at, payload = sim.next_event()
        key = payload["transaction_id"]
        partition = int(hashlib.md5(key.encode()).hexdigest(), 16) % PARTITIONS
        meta = {"topic": "transactions", "partition": partition, "offset": offsets[partition],
                "CreateTime": int(deliver_at.timestamp() * 1000), "key": key}
        offsets[partition] += 1
        rows.append((Json(meta), Json(payload)))
        if len(rows) == args.batch:
            loaded += load(conn, rows)
            rows = []
    loaded += load(conn, rows)
    conn.close()
    print(f"seed_raw: loaded {loaded} rows into raw.raw_transactions")
    return 0


def load(conn, rows) -> int:
    if not rows:
        return 0
    with conn, conn.cursor() as cur:  # one transaction per batch = one INGESTED_AT per load
        execute_values(cur, "insert into raw.raw_transactions (record_metadata, record_content) values %s", rows)
    return len(rows)


if __name__ == "__main__":
    sys.exit(main())
