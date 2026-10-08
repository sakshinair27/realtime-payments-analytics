-- Breakdown marts must add back up to the minute-level totals.
with minute as (
    select minute_ts, txn_count, declined_count from {{ ref('fct_minute_metrics') }}
),
declines as (
    select minute_ts, sum(decline_count) as declined_count
    from {{ ref('fct_declines_by_reason_minute') }} group by 1
),
merchants as (
    select minute_ts, sum(txn_count) as txn_count
    from {{ ref('fct_merchant_category_minute') }} group by 1
)
select m.minute_ts, m.txn_count, mc.txn_count as merchant_txn_count,
       m.declined_count, coalesce(d.declined_count, 0) as reason_declined_count
from minute m
left join declines d on d.minute_ts = m.minute_ts
left join merchants mc on mc.minute_ts = m.minute_ts
where m.declined_count <> coalesce(d.declined_count, 0)
   or m.txn_count <> coalesce(mc.txn_count, 0)
