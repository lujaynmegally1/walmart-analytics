
-- Walmart Analytics — Snowflake Setup
-- Run these statements directly in the Snowflake UI in order.
--
-- NOTE: All table creation (raw, dimension, fact, snapshot) is
-- handled by dbt — see the dbt/ folder. This file only covers
-- the one-time infrastructure setup needed before dbt can run.
 
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE DATABASE WALMART;

-- Storage Integration (Snowflake <-> S3)
 
CREATE OR REPLACE STORAGE INTEGRATION SNOWFLAKE_WALMART_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::127549748722:role/Walmart_project_snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://walmart-bi-dea/data/');
 
-- After running above, run DESC and copy the two output values:
DESC INTEGRATION SNOWFLAKE_WALMART_S3_INT;
-- Copy: STORAGE_AWS_EXTERNAL_ID
-- Copy: STORAGE_AWS_IAM_USER_ARN
-- Paste both into the AWS IAM trust relationship for Walmart_project_snowflake_role
 

-- File Format

 
CREATE OR REPLACE FILE FORMAT WALMART_CSV_FF
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', 'NA')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;
 

-- External Stage

 
CREATE OR REPLACE STAGE WALMART.PUBLIC.WALMART_DATA_STAGE
  STORAGE_INTEGRATION = SNOWFLAKE_WALMART_S3_INT
  URL = 's3://walmart-bi-dea/data/'
  FILE_FORMAT = WALMART.PUBLIC.WALMART_CSV_FF;
 
-- Verify all 3 CSV files are visible
LIST @WALMART_DATA_STAGE;
 

-- Grants for dbt (ACCOUNTADMIN role)

 
GRANT USAGE ON DATABASE WALMART TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA WALMART.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT USAGE ON FILE FORMAT WALMART.PUBLIC.WALMART_CSV_FF TO ROLE ACCOUNTADMIN;
GRANT USAGE ON STAGE WALMART.PUBLIC.WALMART_DATA_STAGE TO ROLE ACCOUNTADMIN;
 
