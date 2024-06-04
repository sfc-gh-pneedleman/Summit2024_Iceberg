/*
 Step 1. Create External Volume and Configure Azure Storage 
 Step 2. Create Database, Schema and Warehouse, and Tables
 Step 3. Build Ingestion Pipeline: Streaming Table -> Dynamic Table -> Task to Insert to Iceberg Table
 Step 4. Create Iceberg User with Read-only Permissions to Iceberg Table

*/
USE ROLE ACCOUNTADMIN;

--make sure to set Public Key for User you which to use snowpipe streaming with
ALTER USER <user> SET RSA_PUBLIC_KEY='MIIBIj.....';


--change base url and tanant ID based on your storage container 
CREATE OR REPLACE  EXTERNAL VOLUME AZURE_EXT_VOLUME
   STORAGE_LOCATIONS =
    (
      (
        NAME = 'azure-iceberg-volume'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://<container>.blob.core.windows.net/<location>/'
        AZURE_TENANT_ID = '<tenant id>'
      )
    );

--get info from external voluyme 
DESC EXTERNAL VOLUME AZURE_EXT_VOLUME;

--grants to sysadmin to use external volume and execute tasks 
GRANT USAGE ON EXTERNAL VOLUME AZURE_EXT_VOLUME TO ROLE SYSADMIN;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

--create DB, schema and WH 
USE ROLE SYSADMIN;
CREATE DATABASE SUMMIT_24;
CREATE SCHEMA RAW;
CREATE SCHEMA CURATED;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_XS_WH WAREHOUSE_SIZE = 'X-Small' AUTO_SUSPEND = 60;


--create a transaction table to process input streaming data. Enable change tracking 
create  TABLE IF NOT EXISTS SUMMIT_24.RAW.TXN_STREAMING (
	RECORD_METADATA VARIANT,
	RECORD_CONTENT VARIANT
);
ALTER TABLE RAW.TXN_STREAMING SET CHANGE_TRACKING = TRUE;

--create Final iceberg table for sales trasnaction with SNowflake Managed Catalog and External Volume
CREATE ICEBERG TABLE IF NOT EXISTS ICEBERG_SALES_TXN
  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='AZURE_EXT_VOLUME'
  BASE_LOCATION='ICEBERG_SALES_TXN'  ( 
	TXN_ID VARCHAR,
	TXN_QUANTITY NUMBER(38,0),
	TXN_DATE VARCHAR,
	PRODUCT_ID VARCHAR,
	PRODUCT_DESC VARCHAR,
    PRODUCT_UNIT_PRICE FLOAT,
    PRODUCT_REVIEW VARCHAR,
    PRODUCT_REVIEW_SENTIMENT_SCORE VARCHAR,
	PAYMENT_METHOD VARCHAR,
	CUSTOMER_ID VARCHAR,
    CUSTOMER_ADDR_RAW VARCHAR,
    CUSTOMER_ADDR_STREET_NO VARCHAR,
    CUSTOMER_ADDR_STREET_NAME VARCHAR,
    CUSTOMER_ADDR_UNIT VARCHAR,
    CUSTOMER_ADDR_CITY VARCHAR,
    CUSTOMER_ADDR_STATE VARCHAR,
    CUSTOMER_ADDR_ZIP VARCHAR    
    );


--create DT table that processes the Stream Data 
create dynamic table IF NOT EXISTS SUMMIT_24.CURATED.SALES_TXN(
	TXN_ID,
	TXN_QUANTITY,
	TXN_DATE,
	PRODUCT_ID,
	PRODUCT_DESC,
    PRODUCT_UNIT_PRICE,
    PRODUCT_REVIEW,
	PAYMENT_METHOD,
	CUSTOMER_ID,
    CUSTOMER_ADDRESS
) lag = '1 minute' refresh_mode = INCREMENTAL initialize = ON_CREATE warehouse = COMPUTE_XS_WH
 as
SELECT 
  RECORD_CONTENT:txn_id::string as TXN_ID,
  RECORD_CONTENT:txn_quantity::number as txn_QUANTITY,
  RECORD_CONTENT:txn_date::string as txn_date,
  RECORD_CONTENT:product_id::string as PRODUCT_ID,
  RECORD_CONTENT:product_desc::string as PRODUCT_DESC,
  RECORD_CONTENT:product_unit_price::float as PRODUCT_UNIT_PRICE,
  RECORD_CONTENT:product_review::string as PRODUCT_REVIEW,
  RECORD_CONTENT:payment_method::string as PAYMENT_METHOD,
  RECORD_CONTENT:customer_id::string as CUSTOMER_ID,
  RECORD_CONTENT:customer_address::string as CUSTOMER_ADDRESS,
 FROM SUMMIT_24.RAW.TXN_STREAMING
;

--create stream on sales TXN to process changed data
CREATE STREAM IF NOT EXISTS CURATED.SALES_TXN_STREAM ON DYNAMIC TABLE CURATED.SALES_TXN;


--create task to load iceberg table from stream
--use cortex functions for enchanced processing 
create TASK IF NOT EXISTS CURATED.INSERT_SALES_TXN_TO_ICEBERG
	--schedule='1 MINUTE'  -- remove schedule to create triggered task 
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='MEDIUM'
	when SYSTEM$STREAM_HAS_DATA('SALES_TXN_STREAM')
	as INSERT INTO ICEBERG_SALES_TXN 
    SELECT * EXCLUDE ADDRESS_JSON FROM 
    (SELECT 
         TXN_ID,
    	TXN_QUANTITY,
    	TXN_DATE,
    	PRODUCT_ID,
    	PRODUCT_DESC,
        PRODUCT_UNIT_PRICE,
        PRODUCT_REVIEW,
        SNOWFLAKE.CORTEX.SENTIMENT(PRODUCT_REVIEW) as PRODUCT_REVIEW_SENTIMENT_SCORE,
    	PAYMENT_METHOD,
    	CUSTOMER_ID,
        CUSTOMER_ADDRESS,
        TRY_PARSE_JSON( SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-7b', 
            'Parse the given address into following JSON values without any comments:
            [addressNumber, streetName, unitNumber, city, state, zip]' || ' content: ' 
            || CUSTOMER_ADDRESS ))::variant as Address_JSON,
       Address_JSON:"addressNumber"::string Street_NUM,
       Address_JSON:"streetName"::string Street_Name,
       Address_JSON:"unitNumber"::string Unit_Nmmber,
       Address_JSON:"city"::string City,   
       Address_JSON:"state"::string State,
       Address_JSON:"zip"::string ZIP_CODE
    FROM CURATED.SALES_TXN_STREAM);


--Create Read-ONly Iceberg user with SELECT Grants 
--note that no warehouse (compute) was given 
USE ROLE SECURITYADMIN;
CREATE USER ICEBERG_USER;
ALTER USER ICEBERG_USER SET PASSWORD = '<enter a password>'
CREATE ROLE ICEBERG_READER;
GRANT USAGE ON DATABASE SUMMIT_24 TO ROLE ICEBERG_READER;
GRANT USAGE ON SCHEMA SUMMIT_24.CURATED TO ROLE ICEBERG_READER;
GRANT SELECT ON TABLE  SUMMIT_24.CURATED.ICEBERG_SALES_TXN  TO ROLE ICEBERG_READER;
GRANT ROLE ICEBERG_READER TO USER ICEBERG_USER;