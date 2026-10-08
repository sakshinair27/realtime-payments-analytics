-- One row per transaction_id, typed.
-- Raw is append-only and at-least-once: the generator re-sends some events and the
-- loader can redeliver after a crash, so the same transaction_id can land many times.
-- We keep the FIRST delivery (earliest ingested_at, then Kafka position) because that is
-- when the event actually became visible to the warehouse.

with source as (

    select * from {{ source('raw', 'raw_transactions') }}

),

parsed as (

    select
        {{ json_text('record_content', 'transaction_id') }}                      as transaction_id,
        {{ iso_to_utc(json_text('record_content', 'timestamp')) }}               as event_ts,
        cast({{ json_text('record_content', 'amount') }} as numeric(12, 2))       as amount,
        upper({{ json_text('record_content', 'currency') }})                     as currency,
        lower({{ json_text('record_content', 'merchant_category') }})            as merchant_category,
        lower({{ json_text('record_content', 'card_type') }})                    as card_type,
        lower({{ json_text('record_content', 'status') }})                       as status,
        lower({{ json_text('record_content', 'decline_reason') }})               as decline_reason,
        cast({{ json_text('record_content', 'retry_count') }} as integer)         as retry_count,

        cast({{ json_text('record_metadata', 'partition') }} as integer)          as kafka_partition,
        cast({{ json_text('record_metadata', 'offset') }} as bigint)              as kafka_offset,
        {{ epoch_ms_to_utc(json_text('record_metadata', 'CreateTime')) }}        as kafka_ts,
        {{ tz_to_utc('ingested_at') }}                                           as ingested_at

    from source
    where {{ json_text('record_content', 'transaction_id') }} is not null

),

ranked as (

    select
        parsed.*,
        row_number() over (
            partition by transaction_id
            order by ingested_at, kafka_partition, kafka_offset
        )                                                   as delivery_rank,
        count(*) over (partition by transaction_id)         as delivery_count
    from parsed

)

select
    transaction_id,
    event_ts,
    amount,
    currency,
    merchant_category,
    card_type,
    status,
    decline_reason,
    retry_count,
    status = 'approved'                                     as is_approved,
    kafka_partition,
    kafka_offset,
    kafka_ts,
    ingested_at,
    -- event time -> processing time
    {{ seconds_between('event_ts', 'ingested_at') }}        as ingest_lag_s,
    delivery_count
from ranked
where delivery_rank = 1
