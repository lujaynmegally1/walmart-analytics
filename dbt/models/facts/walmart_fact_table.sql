{{ config(
  materialized='incremental',
  incremental_strategy = 'merge',
  unique_key= ['store_id', 'dept_id'],
  database='WALMART',
  schema='BRONZE',
  alias='walmart_store_dim'
) }}


{{ config(
   materialized='table',
   database='WALMART',
   schema='BRONZE',
   alias='walmart_fact_table'
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
   dbt_valid_from AS vrsn_start_date,
   dbt_valid_to AS vrsn_end_date
FROM WALMART.SNAPSHOTS.walmart_fact_table_snapshot
WHERE dbt_valid_to IS NULL
