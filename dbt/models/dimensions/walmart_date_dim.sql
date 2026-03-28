-- SCD Type 1 — Upsert (no history preserved)
-- Unique key: date_id
-- On subsequent runs: existing rows are updated in place, new dates are inserted.
-- The is_incremental() filter prevents reprocessing all rows on every run.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['date_id'],
    database = 'WALMART',
    schema = 'PUBLIC_BRONZE',
    alias = 'walmart_date_dim'
) }}

WITH source AS (
    SELECT DISTINCT
        TO_CHAR(store_date, 'YYYYMMDD')::INT    AS date_id,
        store_date,
        isholiday::VARCHAR                      AS isholiday,
        CURRENT_TIMESTAMP()                     AS insert_date,
        CURRENT_TIMESTAMP()                     AS update_date
    FROM WALMART.PUBLIC_RAW.department_raw
)

SELECT *
FROM source

{% if is_incremental() %}
    -- On incremental runs, only process dates not already in the target table
    WHERE date_id NOT IN (SELECT date_id FROM {{ this }})
{% endif %}
