-- SCD Type 1 — Upsert (no history preserved)
-- Composite unique key: (store_id, dept_id)
-- Joins stores_raw and department_raw to get full store + department combinations.

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['store_id', 'dept_id'],
    database = 'WALMART',
    schema = 'PUBLIC_BRONZE',
    alias = 'walmart_store_dim'
) }}

WITH source AS (
    SELECT DISTINCT
        s.store_id,
        d.dept_id,
        s.store_type,
        s.store_size,
        CURRENT_TIMESTAMP() AS insert_date,
        CURRENT_TIMESTAMP() AS update_date
    FROM WALMART.PUBLIC_RAW.stores_raw s
    JOIN WALMART.PUBLIC_RAW.department_raw d
        ON s.store_id = d.store_id
)

SELECT *
FROM source

{% if is_incremental() %}
    -- On incremental runs, only process store/dept combos not already loaded
    WHERE (store_id, dept_id) NOT IN (
        SELECT store_id, dept_id FROM {{ this }}
    )
{% endif %}
