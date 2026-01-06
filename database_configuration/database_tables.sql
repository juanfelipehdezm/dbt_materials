CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;
CREATE SCHEMA IF NOT EXISTS AIRBNB.DEV;

USE DATABASE AIRBNB;
USE SCHEMA RAW;

------- TABLES --------------------------
-----------------------------------------

CREATE OR REPLACE TABLE raw_listings(
    id INTEGER,
    listing_url STRING,
    name STRING,
    room_type STRING,
    minimum_nights INTEGER,
    host_id INTEGER,
    price STRING,
    created_at DATETIME,
    updated_at DATETIME
);

CREATE OR REPLACE TABLE raw_reviews(
    listing_id INTEGER,
    date DATETIME,
    reviewer_name STRING,
    comments STRING,
    sentiment STRING
);

CREATE OR REPLACE TABLE raw_hosts(
    id INTEGER,
    name STRING,
    is_superhost STRING,
    created_at DATETIME,
    updated_at DATETIME
);

------- STAGES AND FILE FORMAT ---------
-----------------------------------------

CREATE OR REPLACE FILE FORMAT csv_file_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

    
CREATE OR REPLACE STAGE listings_stage
    URL = 's3://dbt-datasets/listings.csv'
    FILE_FORMAT = (FORMAT_NAME = csv_file_format);


CREATE OR REPLACE STAGE reviews_stage
    URL = 's3://dbt-datasets/reviews.csv'
    FILE_FORMAT = (FORMAT_NAME = csv_file_format);

        
CREATE OR REPLACE STAGE hosts_stage
    URL = 's3://dbt-datasets/hosts.csv'
    FILE_FORMAT = (FORMAT_NAME = csv_file_format);


------- COPY INTO, loading data ---------
-----------------------------------------

COPY INTO raw_listings (
    ID, LISTING_URL, NAME, ROOM_TYPE, MINIMUM_NIGHTS, HOST_ID, PRICE, CREATED_AT, UPDATED_AT
)
FROM @listings_stage
FILE_FORMAT = (FORMAT_NAME = csv_file_format);

COPY INTO raw_reviews(
    LISTING_ID, DATE, REVIEWER_NAME, COMMENTS, SENTIMENT
)
FROM @reviews_stage
FILE_FORMAT = (FORMAT_NAME = csv_file_format);

COPY INTO raw_hosts(
    ID, NAME, IS_SUPERHOST, CREATED_AT, UPDATED_AT
)
FROM @hosts_stage
FILE_FORMAT = (FORMAT_NAME = csv_file_format);
