{{ config(
   materialized = 'table',
   database = 'WALMART',
   schema = 'PUBLIC',
   alias = 'department_raw'
) }}


SELECT
   $1::INT        AS store_id,
   $2::INT        AS dept_id,
   $3::DATE       AS store_date,
   $4::NUMBER(10,2) AS weekly_sales,
   $5::BOOLEAN   AS isholiday
FROM @WALMART.PUBLIC.WALMART_DATA_STAGE/department.csv
