"""Pure time-series helpers for the dashboard (no Streamlit), so they can be unit-tested."""
from __future__ import annotations

import pandas as pd


def trim_partial_edges(settled: pd.DataFrame) -> pd.DataFrame:
    """Drop partial minutes next to a stop/start of the pipeline.

    Where minutes are missing, the pipeline was down. The minute it stopped in and the first
    minute(s) after it restarted (which only hold backdated late events) are partial and would
    read as a volume crash. Only minutes adjacent to a gap or the window's start are trimmed,
    so a genuine drop while the pipeline is running is never hidden.
    """
    if settled.empty:
        return settled
    floor = 0.8 * settled["txn_count"].median()
    segment = (settled["minute_ts"].diff() > pd.Timedelta(minutes=1)).cumsum()
    keep = []
    for seg_id, seg in settled.groupby(segment, sort=True):
        low = seg["txn_count"] < floor
        first_full = low.values.argmin() if not low.all() else len(seg)
        seg = seg.iloc[first_full:]
        if seg_id != segment.iloc[-1] and not seg.empty:  # trailing edge before a gap
            low = seg["txn_count"] < floor
            last_full = len(seg) - low.values[::-1].argmin() if not low.all() else 0
            seg = seg.iloc[:last_full]
        keep.append(seg)
    return pd.concat(keep) if keep else settled.iloc[0:0]


def with_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Reindex onto a continuous minute grid so downtime plots as a gap, not a slope."""
    if df.empty:
        return df
    grid = pd.date_range(df["minute_ts"].min(), df["minute_ts"].max(), freq="1min", name="minute_ts")
    return df.set_index("minute_ts").reindex(grid).reset_index()
