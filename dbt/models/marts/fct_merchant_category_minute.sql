-- Volume and approval per event-minute and merchant category.

select
    date_trunc('minute', event_ts)                                         as minute_ts,
    merchant_category,
    count(*)                                                               as txn_count,
    sum(case when is_approved then 1 else 0 end)                           as approved_count,
    cast(sum(case when is_approved then 1 else 0 end) as float) / count(*) as approval_rate,
    sum(amount)                                                            as total_amount,
    cast(avg(amount) as numeric(12, 2))                                    as avg_ticket
from {{ ref('stg_transactions') }}
group by 1, 2
