"""Synthetic payment-transaction generator.

Emits JSON transactions to the Kafka topic `transactions` (or stdout with --stdout).

Determinism: every field, including the event `timestamp`, is derived from an RNG seeded
on (--seed, --start-time) and a simulated clock that starts at --start-time. Two runs with
the same --seed and --start-time produce byte-identical payloads; a restart without
--start-time starts a new, non-colliding stream. In --realtime mode (the default) the
generator just sleeps until wall-clock catches up with the simulated clock, so pacing
is live but content is still reproducible.

Realism knobs that make downstream modelling non-trivial:
  * duplicates   - a small share of events is re-sent (producer retry / at-least-once).
  * late events  - a small share is delivered 5-90s after its event time
                   (event time vs processing time).
  * incidents    - a deterministic 2-minute processor outage every 15 simulated minutes
                   spikes `processor_unavailable` declines.
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

MERCHANT_CATEGORIES = {
    # category: (weight, lognormal mu, lognormal sigma, base decline prob)
    "grocery": (0.22, 3.6, 0.6, 0.05),
    "restaurants": (0.18, 3.3, 0.5, 0.05),
    "fuel": (0.10, 3.8, 0.3, 0.04),
    "apparel": (0.09, 4.2, 0.7, 0.08),
    "electronics": (0.07, 5.3, 0.9, 0.13),
    "travel": (0.06, 5.9, 0.8, 0.14),
    "entertainment": (0.08, 3.5, 0.6, 0.07),
    "healthcare": (0.06, 4.4, 0.8, 0.06),
    "utilities": (0.06, 4.6, 0.4, 0.05),
    "digital_goods": (0.08, 2.8, 0.9, 0.16),
}

CARD_TYPES = {"visa": 0.48, "mastercard": 0.32, "amex": 0.12, "discover": 0.08}
CARD_DECLINE_MULT = {"visa": 1.0, "mastercard": 1.05, "amex": 0.8, "discover": 1.2}

DECLINE_REASONS = {
    # reason: (weight, retryable)
    "insufficient_funds": (0.38, True),
    "do_not_honor": (0.22, True),
    "suspected_fraud": (0.12, False),
    "expired_card": (0.08, False),
    "invalid_cvv": (0.08, False),
    "processor_unavailable": (0.12, True),
}

INCIDENT_EVERY_MIN = 15
INCIDENT_LENGTH_MIN = 2


def _weighted(rng: random.Random, table: dict) -> str:
    keys = list(table)
    weights = [v[0] if isinstance(v, tuple) else v for v in table.values()]
    return rng.choices(keys, weights=weights, k=1)[0]


class TransactionSimulator:
    def __init__(self, seed: int, start: datetime, rate_per_min: float,
                 dup_rate: float, late_rate: float):
        # Seed on (seed, start) so a restarted generator doesn't replay the previous run's
        # transaction_ids, which staging would then collapse as duplicates. Output is still
        # fully reproducible given the same --seed and --start-time.
        self.rng = random.Random(f"{seed}|{start.isoformat()}")
        self.start = start
        self.clock = start
        self.rate_per_sec = rate_per_min / 60.0
        self.dup_rate = dup_rate
        self.late_rate = late_rate
        self.seq = 0
        # Min-heap of (deliver_at, seq, payload) so late/duplicate events are emitted
        # in delivery order while keeping their original event timestamp.
        self.pending: list[tuple[datetime, int, dict]] = []

    def _in_incident(self, ts: datetime) -> bool:
        minute = int((ts - self.start).total_seconds() // 60)
        return minute % INCIDENT_EVERY_MIN >= INCIDENT_EVERY_MIN - INCIDENT_LENGTH_MIN

    def _make(self, ts: datetime) -> dict:
        rng = self.rng
        category = _weighted(rng, MERCHANT_CATEGORIES)
        _, mu, sigma, base_decline = MERCHANT_CATEGORIES[category]
        card = _weighted(rng, CARD_TYPES)
        amount = round(min(math.exp(rng.gauss(mu, sigma)), 9_999.0), 2)

        p_decline = base_decline * CARD_DECLINE_MULT[card]
        p_decline *= 1.0 + 0.15 * math.log10(max(amount, 1.0) / 50.0)  # big tickets decline more
        incident = self._in_incident(ts)
        if incident:
            p_decline += 0.20
        p_decline = min(max(p_decline, 0.01), 0.9)

        if rng.random() < p_decline:
            status = "declined"
            reason = "processor_unavailable" if incident and rng.random() < 0.75 \
                else _weighted(rng, DECLINE_REASONS)
            retryable = DECLINE_REASONS[reason][1]
            retry_count = rng.choices([0, 1, 2, 3], weights=[45, 30, 15, 10])[0] if retryable else 0
        else:
            status = "approved"
            reason = None
            # Some approvals only succeeded after client-side retries.
            retry_count = rng.choices([0, 1, 2], weights=[93, 5, 2])[0]

        return {
            "transaction_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            "timestamp": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "amount": amount,
            "currency": "USD",
            "merchant_category": category,
            "card_type": card,
            "status": status,
            "decline_reason": reason,
            "retry_count": retry_count,
        }

    def next_event(self) -> tuple[datetime, dict]:
        """Return (deliver_at, payload) for the next event in delivery order."""
        while not self.pending or self.pending[0][0] > self.clock:
            self.clock += timedelta(seconds=self.rng.expovariate(self.rate_per_sec))
            event = self._make(self.clock)
            deliver_at = self.clock
            if self.rng.random() < self.late_rate:
                # Event happened earlier than it is delivered: shift event time back.
                lag = self.rng.uniform(5, 90)
                event["timestamp"] = (self.clock - timedelta(seconds=lag)) \
                    .isoformat(timespec="milliseconds").replace("+00:00", "Z")
            self._push(deliver_at, event)
            if self.rng.random() < self.dup_rate:
                self._push(deliver_at + timedelta(seconds=self.rng.uniform(0.5, 20)), dict(event))
        deliver_at, _, payload = heapq.heappop(self.pending)
        return deliver_at, payload

    def _push(self, deliver_at: datetime, payload: dict) -> None:
        self.seq += 1
        heapq.heappush(self.pending, (deliver_at, self.seq, payload))


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--rate", type=float, default=300, help="mean events per minute")
    p.add_argument("--start-time", help="ISO-8601 UTC start of the simulated clock (default: now)")
    p.add_argument("--count", type=int, default=0, help="stop after N events (0 = run forever)")
    p.add_argument("--dup-rate", type=float, default=0.01)
    p.add_argument("--late-rate", type=float, default=0.03)
    p.add_argument("--no-realtime", action="store_true", help="emit as fast as possible")
    p.add_argument("--stdout", action="store_true", help="print JSON lines instead of producing to Kafka")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.start_time:
        start = datetime.fromisoformat(args.start_time.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        start = datetime.now(timezone.utc).replace(microsecond=0)
    sim = TransactionSimulator(args.seed, start, args.rate, args.dup_rate, args.late_rate)

    producer = None
    if not args.stdout:
        from confluent_kafka import Producer
        producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "client.id": "txn-generator",
        })

    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"generator: seed={args.seed} start={start.isoformat()} rate={args.rate}/min "
          f"-> {'stdout' if args.stdout else args.bootstrap + '/' + args.topic}", file=sys.stderr, flush=True)

    sent = 0
    wall0, sim0 = time.monotonic(), start
    last_report = time.monotonic()
    while not stop and (args.count == 0 or sent < args.count):
        deliver_at, payload = sim.next_event()
        if not args.no_realtime:
            wait = (deliver_at - sim0).total_seconds() - (time.monotonic() - wall0)
            if wait > 0:
                time.sleep(wait)
        line = json.dumps(payload, separators=(",", ":"))
        if producer is None:
            print(line, flush=False)
        else:
            producer.produce(args.topic, key=payload["transaction_id"], value=line)
            producer.poll(0)
        sent += 1
        if time.monotonic() - last_report > 30:
            print(f"generator: sent={sent}", file=sys.stderr, flush=True)
            last_report = time.monotonic()

    if producer is not None:
        producer.flush(10)
    sys.stdout.flush()
    print(f"generator: done, sent={sent}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
