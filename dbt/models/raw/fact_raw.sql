{{ config(
   materialized = 'table',
   database = 'WALMART',
   schema = 'PUBLIC',
   alias = 'fact_raw'
) }}




SELECT
   $1::INT                             AS store_id,
   $2::DATE                            AS store_date,
   $3::NUMBER(10,2)                    AS temperature,
   $4::NUMBER(10,3)                    AS fuel_price,
   $5::NUMBER(15,2)                    AS markdown1,
   $6::NUMBER(10,2)                    AS markdown2,
   $7::NUMBER(15,2)                    AS markdown3,
   $8::NUMBER(15,2)                    AS markdown4,
   $9::NUMBER(15,2)                    AS markdown5,
   $10::INT                            AS cpi,
   $11::INT                            AS unemployment,
   $12::BOOLEAN                        AS isholiday
FROM @WALMART.PUBLIC.WALMART_DATA_STAGE/fact.csv


