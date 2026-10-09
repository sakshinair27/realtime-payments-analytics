"""Generator contract: reproducible, no ID reuse across runs, and payloads downstream can trust."""
import json
from datetime import UTC, datetime

from generator.generator import DECLINE_REASONS, TransactionSimulator

START = datetime(2026, 1, 1, tzinfo=UTC)


def events(n, seed=42, start=START, **kw):
    sim = TransactionSimulator(seed, start, rate_per_min=300, dup_rate=kw.get("dup", 0.01),
                               late_rate=kw.get("late", 0.03))
    return [sim.next_event()[1] for _ in range(n)]


def test_same_seed_and_start_is_byte_identical():
    a = [json.dumps(e) for e in events(2000)]
    b = [json.dumps(e) for e in events(2000)]
    assert a == b


def test_restart_never_reuses_transaction_ids():
    # Regression: seeding on --seed alone replayed the previous run's IDs after a restart,
    # and staging silently dropped the new events as "duplicates".
    first = {e["transaction_id"] for e in events(2000)}
    restarted = {e["transaction_id"] for e in events(2000, start=START.replace(minute=5))}
    assert first.isdisjoint(restarted)


def test_duplicates_are_exact_resends():
    seen = {}
    dupes = 0
    for e in events(5000, dup=0.05):
        if e["transaction_id"] in seen:
            dupes += 1
            assert seen[e["transaction_id"]] == e  # same payload, not a different txn
        seen[e["transaction_id"]] = e
    assert dupes > 0


def test_payload_contract():
    for e in events(3000):
        assert e["status"] in {"approved", "declined"}, e
        assert (e["status"] == "declined") == (e["decline_reason"] is not None), e
        if e["decline_reason"]:
            assert e["decline_reason"] in DECLINE_REASONS, e
            if not DECLINE_REASONS[e["decline_reason"]][1]:  # hard declines are never retried
                assert e["retry_count"] == 0, e
        assert 0 < e["amount"] <= 9999, e
        assert e["currency"] == "USD", e
        assert e["timestamp"].endswith("Z"), e


def test_late_events_are_backdated_not_future():
    sim = TransactionSimulator(42, START, 300, dup_rate=0.0, late_rate=0.2)
    late = 0
    for _ in range(3000):
        deliver_at, e = sim.next_event()
        event_ts = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
        assert event_ts <= deliver_at
        late += (deliver_at - event_ts).total_seconds() >= 5
    assert late > 0
