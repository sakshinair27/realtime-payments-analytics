{#
  The landing table has the Snowflake Kafka connector's layout on both warehouses:
  RECORD_METADATA / RECORD_CONTENT (VARIANT on Snowflake, JSONB on Postgres) + INGESTED_AT.
  These macros hide the dialect differences so staging is written once.
#}

{# JSON field as text #}
{% macro json_text(col, key) %}{{ return(adapter.dispatch('json_text')(col, key)) }}{% endmacro %}
{% macro snowflake__json_text(col, key) %}get({{ col }}, '{{ key }}')::varchar{% endmacro %}
{% macro postgres__json_text(col, key) %}({{ col }} ->> '{{ key }}'){% endmacro %}

{# ISO-8601 string with offset -> UTC timestamp without time zone #}
{% macro iso_to_utc(expr) %}{{ return(adapter.dispatch('iso_to_utc')(expr)) }}{% endmacro %}
{% macro snowflake__iso_to_utc(expr) %}convert_timezone('UTC', try_to_timestamp_tz({{ expr }}))::timestamp_ntz{% endmacro %}
{% macro postgres__iso_to_utc(expr) %}(({{ expr }})::timestamptz at time zone 'UTC'){% endmacro %}

{# epoch milliseconds (as text) -> UTC timestamp without time zone #}
{% macro epoch_ms_to_utc(expr) %}{{ return(adapter.dispatch('epoch_ms_to_utc')(expr)) }}{% endmacro %}
{% macro snowflake__epoch_ms_to_utc(expr) %}to_timestamp_ntz(({{ expr }})::number, 3){% endmacro %}
{% macro postgres__epoch_ms_to_utc(expr) %}(to_timestamp(({{ expr }})::bigint / 1000.0) at time zone 'UTC'){% endmacro %}

{# warehouse-local tz-aware timestamp -> UTC timestamp without time zone #}
{% macro tz_to_utc(col) %}{{ return(adapter.dispatch('tz_to_utc')(col)) }}{% endmacro %}
{% macro snowflake__tz_to_utc(col) %}convert_timezone('UTC', {{ col }})::timestamp_ntz{% endmacro %}
{% macro postgres__tz_to_utc(col) %}({{ col }} at time zone 'UTC'){% endmacro %}

{# seconds from a to b, fractional #}
{% macro seconds_between(a, b) %}{{ return(adapter.dispatch('seconds_between')(a, b)) }}{% endmacro %}
{% macro snowflake__seconds_between(a, b) %}(datediff('millisecond', {{ a }}, {{ b }}) / 1000.0){% endmacro %}
{% macro postgres__seconds_between(a, b) %}extract(epoch from ({{ b }} - {{ a }}))::float{% endmacro %}
