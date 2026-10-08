-- Local Postgres mirror of the Snowflake landing table (warehouse/snowflake/setup.sql).
-- Same shape as what the Snowflake Kafka connector writes: RECORD_METADATA + RECORD_CONTENT,
-- plus INGESTED_AT stamped by the database at load time (processing time).
create schema if not exists raw;
create schema if not exists analytics;

create table if not exists raw.raw_transactions (
    record_metadata jsonb not null,   -- topic / partition / offset / CreateTime / key
    record_content  jsonb not null,   -- the event payload as produced
    ingested_at     timestamptz not null default now()
);

-- Append-only: the loader may insert, nothing may update or delete.
create or replace function raw.forbid_mutation() returns trigger language plpgsql as $$
begin
    raise exception 'raw.raw_transactions is append-only';
end $$;

drop trigger if exists raw_transactions_append_only on raw.raw_transactions;
create trigger raw_transactions_append_only
    before update or delete on raw.raw_transactions
    for each statement execute function raw.forbid_mutation();
