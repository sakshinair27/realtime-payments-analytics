-- Dedupe must collapse duplicates without losing any transaction.
-- Raw keeps growing while dbt runs, so compare against raw as of the staging snapshot's
-- high-water mark (loads are batched and ingested_at is per batch, so this is a clean cut).
with watermark as (
    select max(ingested_at) as max_ingested_at from {{ ref('stg_transactions') }}
),
raw_ids as (
    select count(distinct {{ json_text('record_content', 'transaction_id') }}) as n
    from {{ source('raw', 'raw_transactions') }}
    cross join watermark
    where {{ tz_to_utc('ingested_at') }} <= watermark.max_ingested_at
),
stg as (
    select count(*) as n from {{ ref('stg_transactions') }}
)
select raw_ids.n as raw_distinct, stg.n as stg_rows
from raw_ids cross join stg
where raw_ids.n <> stg.n
