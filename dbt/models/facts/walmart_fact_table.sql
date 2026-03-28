-- Final fact table: reads from SCD2 snapshot, surfaces only current (active) records.
-- dbt_valid_to IS NULL = the active/current version of each record.
-- dbt_valid_from / dbt_valid_to are exposed as vrsn_start_date / vrsn_end_date.

{{ config(
    materialized = 'table',
    database = 'WALMART',
    schema = 'PUBLIC_BRONZE',
    alias = 'walmart_fact_table'
) }}

SELECT
    store_id,
    dept_id,
    store_date,
    store_weekly_sales,
    fuel_price,
    store_temperature,
    unemployment,
    cpi,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    insert_date,
    update_date,
    dbt_valid_from  AS vrsn_start_date,
    dbt_valid_to    AS vrsn_end_date
FROM WALMART.SNAPSHOTS.walmart_fact_table_snapshot
WHERE dbt_valid_to IS NULL
