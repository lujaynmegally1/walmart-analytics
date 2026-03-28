{% snapshot walmart_fact_table_snapshot %}
{{
   config(
     target_database='WALMART',
     target_schema='SNAPSHOTS',
     unique_key=['store_id', 'dept_id','store_date'],
     strategy='check',
     check_cols=[
       'store_weekly_sales',
       'fuel_price',
       'store_temperature',
       'unemployment',
       'cpi',
       'markdown1',
       'markdown2',
       'markdown3',
       'markdown4',
       'markdown5']
   )
}}

   -- SCD Type 2 — Versioned history via dbt snapshot
-- When any check_col changes for a (store_id, dept_id, store_date) combo:
--   1. The existing row is "closed" (dbt_valid_to is set to current timestamp)
--   2. A new row is inserted with dbt_valid_from = current timestamp, dbt_valid_to = NULL
-- dbt_valid_to IS NULL = current/active record
 
WITH source AS (
   SELECT
       f.store_date,
       f.store_id,
       d.dept_id,
       d.weekly_sales AS store_weekly_sales,
       f.fuel_price,
       f.temperature AS store_temperature,
       f.unemployment,
       f.cpi,
       f.markdown1,
       f.markdown2,
       f.markdown3,
       f.markdown4,
       f.markdown5,
       CURRENT_TIMESTAMP() AS insert_date,
       CURRENT_TIMESTAMP() AS update_date
   FROM WALMART.PUBLIC_RAW.fact_raw f
   JOIN WALMART.PUBLIC_RAW.department_raw d
   ON f.store_id = d.store_id
   AND f.store_date = d.store_date
)
SELECT *
FROM source
{% endsnapshot %}

-- When a change is made in weekly sales, it overwrites that value to all other store_date and store id columns 
