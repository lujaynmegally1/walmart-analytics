{{ config(
  materialized='incremental',
  incremental_strategy = 'merge',
  unique_key= ['store_id', 'dept_id'],
  database='WALMART',
  schema='BRONZE',
  alias='walmart_store_dim'
) }}


WITH source AS (
  SELECT
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
