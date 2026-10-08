"""Live transaction dashboard. Reads only the dbt marts; refreshes on an interval."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from warehouse.client import WAREHOUSE, query  # noqa: E402

# Categorical slots in fixed order, assigned per entity (never by rank) so a reason or
# category keeps its color as the window changes.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
REASONS = ["insufficient_funds", "do_not_honor", "processor_unavailable",
           "suspected_fraud", "expired_card", "invalid_cvv"]
REASON_COLORS = dict(zip(REASONS, SERIES))
PRIMARY = SERIES[0]
GRID = "rgba(137,135,129,0.25)"

st.set_page_config(page_title="Transaction Monitor", page_icon="💳", layout="wide")


def style(fig: go.Figure, height: int = 280, y_title: str | None = None, pct: bool = False) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=30, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None),
        bargap=0.15,
    )
    fig.update_xaxes(showgrid=False, title=None)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, title=y_title, rangemode="tozero",
                     tickformat=".0%" if pct else None)
    return fig


@st.cache_data(ttl=5, show_spinner=False)
def load(window_min: int) -> dict[str, pd.DataFrame]:
    # Pull a little extra so the window is full after dropping the in-progress minute.
    since = f"(select max(minute_ts) from {{schema}}.fct_minute_metrics)"
    lookback = f"{since} - interval '{window_min + 1} minutes'"
    return {
        "minute": query(f"select * from {{schema}}.fct_minute_metrics "
                        f"where minute_ts > {lookback} order by minute_ts"),
        "declines": query(f"select minute_ts, decline_reason, decline_count "
                          f"from {{schema}}.fct_declines_by_reason_minute "
                          f"where minute_ts > {lookback} order by minute_ts"),
        "merchant": query(f"select minute_ts, merchant_category, txn_count, approved_count, total_amount "
                          f"from {{schema}}.fct_merchant_category_minute where minute_ts > {lookback}"),
    }


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


def numeric(df: pd.DataFrame) -> pd.DataFrame:
    # Warehouse NUMERIC columns arrive as Decimal objects.
    for c in df.columns:
        if c not in ("minute_ts", "decline_reason", "merchant_category") and df[c].dtype == object:
            df[c] = pd.to_numeric(df[c])
    if "minute_ts" in df:
        df["minute_ts"] = pd.to_datetime(df["minute_ts"])
    return df


st.title("Transaction monitor")

with st.sidebar:
    window = st.select_slider("Window (minutes)", options=[15, 30, 60, 120, 240], value=30)
    refresh = st.select_slider("Refresh every (s)", options=[5, 10, 15, 30, 60], value=10)
    st.caption(f"Warehouse: **{WAREHOUSE}** · marts in `analytics` · rebuilt by dbt every ~30s.")
    st.caption("Minutes are **event time**. The newest minute is still filling in (late "
               "events, loader + dbt cadence), so it is shown separately and excluded from trends.")


@st.fragment(run_every=f"{refresh}s")
def live():
    try:
        data = {k: numeric(v) for k, v in load(window).items()}
    except Exception as exc:  # warehouse not up yet, marts not built yet, etc.
        st.warning(f"Waiting for data: {exc}")
        return
    minute, declines, merchant = data["minute"], data["declines"], data["merchant"]
    if minute.empty:
        st.info("No transactions in the marts yet. Is the generator running and has dbt built?")
        return

    # The newest event-minute is incomplete; trends use settled minutes only.
    open_minute = minute["minute_ts"].max()
    settled = trim_partial_edges(minute[minute["minute_ts"] < open_minute].tail(window))
    # Breakdowns use exactly the settled minutes, so trimmed partial minutes don't leak back in.
    declines = declines[declines["minute_ts"].isin(settled["minute_ts"])]
    merchant = merchant[merchant["minute_ts"].isin(settled["minute_ts"])]
    if settled.empty:
        st.info("Collecting the first full minute…")
        return

    behind = int((open_minute - settled["minute_ts"].max()).total_seconds() // 60) - 1
    if behind >= 2:
        st.warning(f"No complete minute for the last {behind} minutes: the pipeline is restarting or "
                   f"stalled. Figures below are as of {settled['minute_ts'].max():%H:%M} UTC.")

    last, prev = settled.iloc[-1], settled.iloc[-2] if len(settled) > 1 else None
    w_txn = settled["txn_count"].sum()
    w_rate = settled["approved_count"].sum() / w_txn
    w_ticket = settled["total_amount"].sum() / w_txn

    c = st.columns(5)
    c[0].metric("Transactions / min", f"{int(last.txn_count):,}",
                None if prev is None else f"{int(last.txn_count - prev.txn_count):+,} vs prior min")
    c[1].metric("Approval rate (last min)", f"{last.approval_rate:.1%}",
                None if prev is None else f"{(last.approval_rate - prev.approval_rate) * 100:+.1f} pts")
    c[2].metric(f"Approval rate ({len(settled)}m)", f"{w_rate:.1%}")
    c[3].metric(f"Avg ticket ({len(settled)}m)", f"${w_ticket:,.2f}")
    c[4].metric("p95 ingest lag", f"{last.p95_ingest_lag_s:.1f}s",
                help="Event time → warehouse ingested_at. Includes loader flush interval.")

    plot = with_gaps(settled)  # line charts: downtime shows as a break

    left, right = st.columns(2)
    with left:
        st.subheader("Transactions per minute")
        fig = go.Figure(go.Scatter(x=plot["minute_ts"], y=plot["txn_count"], mode="lines",
                                   line=dict(color=PRIMARY, width=2), name="Transactions",
                                   hovertemplate="%{y:,} txns<extra></extra>"))
        st.plotly_chart(style(fig), width="stretch")
    with right:
        st.subheader("Approval rate")
        fig = go.Figure(go.Scatter(x=plot["minute_ts"], y=plot["approval_rate"], mode="lines",
                                   line=dict(color=PRIMARY, width=2), name="Approval rate",
                                   hovertemplate="%{y:.1%}<extra></extra>"))
        style(fig, pct=True).update_yaxes(rangemode="normal")
        st.plotly_chart(fig, width="stretch")

    left, right = st.columns(2)
    with left:
        st.subheader("Declines by reason")
        fig = px.bar(declines, x="minute_ts", y="decline_count", color="decline_reason",
                     color_discrete_map=REASON_COLORS, category_orders={"decline_reason": REASONS},
                     labels={"decline_count": "Declines", "decline_reason": "Reason"})
        fig.update_traces(marker_line_width=0, hovertemplate="%{fullData.name}: %{y}<extra></extra>")
        st.plotly_chart(style(fig), width="stretch")
    with right:
        st.subheader(f"Top merchant categories ({len(settled)}m)")
        top = (merchant.groupby("merchant_category", as_index=False)
               .agg(txn_count=("txn_count", "sum"), approved=("approved_count", "sum"),
                    amount=("total_amount", "sum"))
               .sort_values("txn_count", ascending=True))
        top["approval_rate"] = top["approved"] / top["txn_count"]
        fig = go.Figure(go.Bar(x=top["txn_count"], y=top["merchant_category"], orientation="h",
                               marker=dict(color=PRIMARY), text=top["txn_count"], textposition="outside",
                               customdata=top[["approval_rate", "amount"]],
                               hovertemplate="%{y}: %{x:,} txns · %{customdata[0]:.1%} approved"
                                             " · $%{customdata[1]:,.0f}<extra></extra>"))
        style(fig).update_layout(hovermode="closest")
        # Headroom so the outside value label on the longest bar isn't clipped.
        fig.update_xaxes(showgrid=False, showticklabels=False, range=[0, top["txn_count"].max() * 1.15])
        fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width="stretch")

    st.subheader("Event time vs processing time")
    lag = plot[["minute_ts", "avg_ingest_lag_s", "p95_ingest_lag_s"]].melt(
        "minute_ts", var_name="stat", value_name="seconds")
    lag["stat"] = lag["stat"].map({"avg_ingest_lag_s": "Average lag", "p95_ingest_lag_s": "p95 lag"})
    fig = px.line(lag, x="minute_ts", y="seconds", color="stat",
                  color_discrete_sequence=SERIES[:2], labels={"seconds": "Seconds"})
    fig.update_traces(line_width=2, hovertemplate="%{fullData.name}: %{y:.1f}s<extra></extra>")
    st.plotly_chart(style(fig, height=240, y_title="seconds"), width="stretch")
    st.caption(f"Late arrivals (>30s) in window: {int(settled['late_arrivals'].sum()):,} · "
               f"duplicate deliveries dropped by staging: {int(settled['duplicates_dropped'].sum()):,} · "
               f"open minute {open_minute:%H:%M} UTC so far: {int(minute.iloc[-1].txn_count):,} txns · "
               f"refreshed {datetime.now(timezone.utc):%H:%M:%S} UTC")

    with st.expander("Table view"):
        st.dataframe(settled.sort_values("minute_ts", ascending=False), width="stretch",
                     hide_index=True)


live()
