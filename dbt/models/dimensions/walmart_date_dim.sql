{{ config(
   materialized='incremental',
   incremental_strategy = 'merge',
   unique_key= ['date_id'],
   database='WALMART',
   schema='BRONZE',
   alias='walmart_date_dim'
) }}


SELECT
   store_id,
   TO_CHAR(store_date, 'YYYYMMDD')::INT AS date_id,
   store_date,
   isholiday,
   CURRENT_TIMESTAMP() AS insert_date,
   CURRENT_TIMESTAMP() AS update_date
FROM WALMART.PUBLIC_RAW.department_raw

-- incomplete... 
