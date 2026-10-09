"""Dashboard trimming: hide pipeline stop/start artifacts, never hide a real drop."""
import pandas as pd

from dashboard.timeseries import trim_partial_edges, with_gaps


def minutes(rows):
    return pd.DataFrame({"minute_ts": [pd.Timestamp(f"2026-10-08 15:{m:02d}") for m, _ in rows],
                         "txn_count": [c for _, c in rows]})


def kept(df):
    return [(t.minute, c) for t, c in zip(df["minute_ts"], df["txn_count"], strict=True)]


def test_restart_drops_partial_minutes_on_both_sides_of_gap():
    # Regression from a real stop at 15:08 / restart at 15:21.
    df = minutes([(4, 306), (5, 283), (6, 309), (7, 291), (8, 165), (21, 1), (22, 15), (23, 282), (24, 300)])
    assert kept(trim_partial_edges(df)) == [(4, 306), (5, 283), (6, 309), (7, 291), (23, 282), (24, 300)]


def test_real_drop_while_running_is_kept():
    df = minutes([(1, 300), (2, 310), (3, 120), (4, 90), (5, 295)])
    assert kept(trim_partial_edges(df)) == kept(df)


def test_drop_in_latest_minute_is_kept():
    df = minutes([(1, 300), (2, 310), (3, 305), (4, 80)])
    assert kept(trim_partial_edges(df)) == kept(df)


def test_partial_first_minute_after_startup_is_trimmed():
    df = minutes([(1, 40), (2, 300), (3, 310)])
    assert kept(trim_partial_edges(df)) == [(2, 300), (3, 310)]


def test_with_gaps_marks_downtime_as_missing():
    out = with_gaps(minutes([(1, 300), (2, 310), (6, 305)]))
    assert len(out) == 6
    assert out["txn_count"].isna().sum() == 3


def test_empty_input():
    empty = minutes([])
    assert trim_partial_edges(empty).empty
    assert with_gaps(empty).empty
