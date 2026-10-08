-- Per-minute KPIs, bucketed on EVENT time (when the payment happened), not ingest time.
-- Late arrivals land in their true minute, so recent minutes keep filling in for a while;
-- `last_ingested_at` and `late_arrivals` show how settled each minute is.

select
    date_trunc('minute', event_ts)                                         as minute_ts,
    count(*)                                                               as txn_count,
    sum(case when is_approved then 1 else 0 end)                           as approved_count,
    sum(case when is_approved then 0 else 1 end)                           as declined_count,
    cast(sum(case when is_approved then 1 else 0 end) as float) / count(*) as approval_rate,
    sum(amount)                                                            as total_amount,
    cast(avg(amount) as numeric(12, 2))                                    as avg_ticket,
    sum(case when is_approved and retry_count > 0 then 1 else 0 end)       as approved_after_retry,
    avg(ingest_lag_s)                                                      as avg_ingest_lag_s,
    percentile_cont(0.95) within group (order by ingest_lag_s)             as p95_ingest_lag_s,
    sum(case when ingest_lag_s > 30 then 1 else 0 end)                     as late_arrivals,
    sum(delivery_count - 1)                                                as duplicates_dropped,
    max(ingested_at)                                                       as last_ingested_at
from {{ ref('stg_transactions') }}
group by 1
