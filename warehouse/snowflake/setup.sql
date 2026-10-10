-- One-time Snowflake setup. Run as ACCOUNTADMIN (or SECURITYADMIN + SYSADMIN).
-- Replace <LOADER_PUBLIC_KEY> / <DBT_PUBLIC_KEY> with the bodies of keys/rsa_key.pub and
-- keys/dbt_key.pub (no header/footer lines), or run warehouse/snowflake/render_setup.py,
-- which writes a filled-in copy to keys/setup_filled.sql.

use role securityadmin;
create role if not exists fintech_loader;      -- used by the Kafka connector / Snowpipe
create role if not exists fintech_transformer; -- used by dbt and the dashboard
grant role fintech_loader to role sysadmin;
grant role fintech_transformer to role sysadmin;

-- Both technical users are TYPE = SERVICE with key-pair auth only: no password, so they are
-- exempt from the MFA that Snowflake enforces on password logins, and can't be phished.
create user if not exists kafka_connector
    type = service
    default_role = fintech_loader
    rsa_public_key = '<LOADER_PUBLIC_KEY>';
grant role fintech_loader to user kafka_connector;

create user if not exists fintech_dbt
    type = service
    default_role = fintech_transformer
    default_warehouse = fintech_wh
    rsa_public_key = '<DBT_PUBLIC_KEY>';
grant role fintech_transformer to user fintech_dbt;

-- Let the human running this script browse the results in Snowsight too.
set me = current_user();
grant role fintech_transformer to user identifier($me);

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
grant usage, create table, create view on schema fintech.analytics to role fintech_transformer;

use role securityadmin;  -- future grants need MANAGE GRANTS, which SYSADMIN lacks
grant select on future tables in schema fintech.raw to role fintech_transformer;

-- Useful once data is flowing:
--   show pipes in schema fintech.raw;
--   select system$pipe_status('<pipe name from show pipes>');
--   select * from table(information_schema.copy_history(
--       table_name => 'FINTECH.RAW.RAW_TRANSACTIONS', start_time => dateadd(hour, -1, current_timestamp())));
