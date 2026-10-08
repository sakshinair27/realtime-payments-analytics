-- One-time Snowflake setup. Run as ACCOUNTADMIN (or SECURITYADMIN + SYSADMIN).
-- Replace <PUBLIC_KEY> with the body of keys/rsa_key.pub (no header/footer lines);
-- see README "Snowflake mode" for generating the key pair.

use role securityadmin;
create role if not exists fintech_loader;      -- used by the Kafka connector / Snowpipe
create role if not exists fintech_transformer; -- used by dbt and the dashboard
grant role fintech_loader to role sysadmin;
grant role fintech_transformer to role sysadmin;

create user if not exists kafka_connector
    default_role = fintech_loader
    rsa_public_key = '<PUBLIC_KEY>';
grant role fintech_loader to user kafka_connector;

use role sysadmin;
create warehouse if not exists fintech_wh
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true;
create database if not exists fintech;
create schema if not exists fintech.raw;
create schema if not exists fintech.analytics;

grant usage on warehouse fintech_wh to role fintech_loader;
grant usage on warehouse fintech_wh to role fintech_transformer;
grant usage on database fintech to role fintech_loader;
grant usage on database fintech to role fintech_transformer;

-- The connector creates its own internal stage and one Snowpipe per topic partition
-- in this schema, so it needs CREATE STAGE / CREATE PIPE here.
grant usage, create table, create stage, create pipe on schema fintech.raw to role fintech_loader;

-- Landing table. The connector writes RECORD_METADATA (topic, partition, offset,
-- CreateTime, key) and RECORD_CONTENT (the JSON payload). Its pipe's COPY lists only
-- those two columns, so INGESTED_AT takes its default: the Snowpipe load time.
-- Extra columns on a pre-created table must be nullable, so INGESTED_AT is nullable.
create table if not exists fintech.raw.raw_transactions (
    record_metadata variant,
    record_content  variant,
    ingested_at     timestamp_ltz default current_timestamp()
);
-- The connector needs to own the table it loads into.
grant ownership on table fintech.raw.raw_transactions to role fintech_loader copy current grants;

-- Append-only for everyone downstream: read-only access to raw.
grant usage on schema fintech.raw to role fintech_transformer;
grant select on all tables in schema fintech.raw to role fintech_transformer;
grant select on future tables in schema fintech.raw to role fintech_transformer;
grant usage, create table, create view on schema fintech.analytics to role fintech_transformer;

-- Grant fintech_transformer to the human / service user that runs dbt + the dashboard:
-- grant role fintech_transformer to user <YOUR_USER>;

-- Useful once data is flowing:
--   show pipes in schema fintech.raw;
--   select system$pipe_status('<pipe name from show pipes>');
--   select * from table(information_schema.copy_history(
--       table_name => 'FINTECH.RAW.RAW_TRANSACTIONS', start_time => dateadd(hour, -1, current_timestamp())));
