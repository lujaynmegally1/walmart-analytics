-- Gold Layer — Run AFTER dbt models are built
-- Creates a pre-joined, deduplicated table for Python use.
-- The full 3-table join in pandas generates billions of rows;
-- this pre-aggregates in Snowflake so the pull to local is fast.
-- =============================================================
 
-- Step 1: Deduplicate fact table (keep latest row per store/dept)
CREATE OR REPLACE TEMP TABLE FACT_REDUCED AS
SELECT *
FROM (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY store_id, dept_id
            ORDER BY store_date DESC
        ) AS rn
    FROM WALMART.PUBLIC_BRONZE.WALMART_FACT_TABLE f
)
WHERE rn = 1;
 
-- Step 2: Filter to larger stores only
CREATE OR REPLACE TEMP TABLE STORES_FILTERED AS
SELECT DISTINCT
    store_id,
    dept_id,
    store_type,
    store_size
FROM WALMART.PUBLIC_BRONZE.WALMART_STORE_DIM
WHERE store_size >= 150000;
 
-- Step 3: Create the joined gold table
CREATE OR REPLACE TABLE WALMART.PUBLIC_GOLD.WALMART_JOINED_REDUCED AS
SELECT
    f.store_id,
    f.dept_id,
    f.store_weekly_sales,
    f.store_date,
    f.fuel_price,
    f.store_temperature,
    f.unemployment,
    f.cpi,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    s.store_type,
    s.store_size,
    d.isholiday
FROM FACT_REDUCED f
JOIN STORES_FILTERED s
    ON f.store_id = s.store_id
   AND f.dept_id = s.dept_id
LEFT JOIN WALMART.PUBLIC_BRONZE.WALMART_DATE_DIM d
    ON f.store_date = d.store_date;
 
