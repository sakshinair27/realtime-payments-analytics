{{ config(severity='warn') }}
-- Processing time can't precede the Kafka produce time (beyond a little clock skew).
select transaction_id, kafka_ts, ingested_at
from {{ ref('stg_transactions') }}
where ingested_at < kafka_ts - interval '60 seconds'
