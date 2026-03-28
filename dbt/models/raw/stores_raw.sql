{{ config(
   materialized = 'table',
   database = 'WALMART',
   schema = 'PUBLIC',
   alias = 'stores_raw'
) }}


SELECT
   $1::INT                         AS store_id,
   $2::VARCHAR(10)                 AS store_type,
   $3::INT                         AS store_size
FROM @WALMART.PUBLIC.WALMART_DATA_STAGE/stores.csv
