-- Declines per event-minute and reason.

select
    date_trunc('minute', event_ts)  as minute_ts,
    decline_reason,
    count(*)                        as decline_count,
    sum(amount)                     as declined_amount,
    sum(case when decline_reason in ('insufficient_funds', 'do_not_honor', 'processor_unavailable')
             then 1 else 0 end)     as soft_decline_count
from {{ ref('stg_transactions') }}
where not is_approved
group by 1, 2
