-- Create an IAM + rol in AWS then
-- Create a Storage Integration 
CREATE or replace STORAGE INTEGRATION <your_integration>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::your_arn'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('your_URL');

--Update the Truste relationships for our rol in AWS with informations using 
-- query: "DESC INTEGRATION <your_integration> ";
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::your_STORAGE_AWS_IAM_USER_ARN"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "your_STORAGE_AWS_EXTERNAL_ID"
                }
            }
        }
    ]
}


-- Create a file format
CREATE FILE FORMAT IF NOT EXISTS csv_format
  TYPE = 'CSV' 
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Create 
CREATE OR REPLACE STAGE ext_Stage_Airbnb_S3
FILE_FORMAT = csv_format
STORAGE_INTEGRATION = s3_integration_airbnb
URL='your_URL';

list @ext_Stage_Airbnb_S3;

-- Creer les warehouse selon les workload
-- 1. Warehouse ingestion (Pipes/Tasks légé)
CREATE WAREHOUSE WH_INGEST
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- 2. Warehouse transformations DBT
--  Warehouse avec auto-scaling (scale-out)
CREATE WAREHOUSE WH_TRANSFORM_DBT
  WAREHOUSE_SIZE = 'SMALL'
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE;
  
-- Create a Snowpip for continus Batsch load 
-- Pipe pour bookings
CREATE PIPE bookings_pipe 
  AUTO_INGEST = TRUE AS
  COPY INTO AIRBNB.STAGING.BOOKINGS FROM @ext_Stage_Airbnb_S3
  PATTERN = '.*bookings.*[.]csv';

-- Pipe pour hosts
CREATE PIPE hosts_pipe 
  AUTO_INGEST = TRUE AS
  COPY INTO AIRBNB.STAGING.HOSTS FROM @ext_Stage_Airbnb_S3
  PATTERN = '.*hosts.*[.]csv';

-- Pipe pour listings
CREATE PIPE listings_pipe 
  AUTO_INGEST = TRUE AS
  COPY INTO AIRBNB.STAGING.LISTINGS FROM @ext_Stage_Airbnb_S3
  PATTERN = '.*listings.*[.]csv';

-- Configure S3 Event Notification in AWS
-- AWS Console → S3 → bucket 
-- Properties → Event notifications → Create event notification

-- Fill in:
-- Name: snowpipe-notification
-- Prefix: 
-- Event types: All object create events
-- Destination: Select SQS Queue
-- SQS queue: Choose Enter SQS queue ARN
-- SQS queue ARN: Paste the notification_channel value from: show pipes;


-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('bookings_pipe');
SELECT SYSTEM$PIPE_STATUS('listings_pipe');
SELECT SYSTEM$PIPE_STATUS('hosts_pipe');

-- Creer des stream pour capturer les nouvelles lignes
CREATE OR REPLACE STREAM stream_bookings
ON TABLE AIRBNB.STAGING.BOOKINGS
APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM stream_listings
ON TABLE AIRBNB.STAGING.LISTINGS
APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM stream_hosts
ON TABLE AIRBNB.STAGING.HOSTS
APPEND_ONLY = TRUE;

-- Creer une task qui declanche bronze en incremntal avec STREAM
-- si stream detect des nouveaux insert

--Root task (scheduler)
CREATE OR REPLACE TASK task_root
WAREHOUSE = WH_INGEST
SCHEDULE = '5 MINUTE'
AS
SELECT 1;

--task_bronze_bookings
CREATE OR REPLACE TASK task_bronze_bookings
WAREHOUSE = WH_INGEST
AFTER task_root
WHEN SYSTEM$STREAM_HAS_DATA('stream_bookings')
AS
INSERT INTO AIRBNB.BRONZE.BRONZE_BOOKINGS
    (BOOKING_ID, LISTING_ID, BOOKING_DATE, NIGHTS_BOOKED, BOOKING_AMOUNT,
     CLEANING_FEE, SERVICE_FEE, BOOKING_STATUS, CREATED_AT)
    SELECT 
        BOOKING_ID, LISTING_ID, BOOKING_DATE, NIGHTS_BOOKED, BOOKING_AMOUNT,
        CLEANING_FEE, SERVICE_FEE, BOOKING_STATUS, CREATED_AT
    FROM stream_bookings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id, created_at ORDER BY created_at DESC) = 1;

-- task_bronze_listings
CREATE OR REPLACE TASK task_bronze_listings
WAREHOUSE = WH_INGEST
AFTER task_root
WHEN SYSTEM$STREAM_HAS_DATA('stream_listings')
AS
INSERT INTO AIRBNB.BRONZE.BRONZE_LISTINGS
        (LISTING_ID,HOST_ID,PROPERTY_TYPE,ROOM_TYPE,CITY,COUNTRY,ACCOMMODATES,BEDROOMS,BATHROOMS,
        PRICE_PER_NIGHT,CREATED_AT)
    SELECT 
        LISTING_ID,HOST_ID,PROPERTY_TYPE,ROOM_TYPE,CITY,COUNTRY,ACCOMMODATES,BEDROOMS,BATHROOMS,
        PRICE_PER_NIGHT,CREATED_AT
    FROM stream_listings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id, created_at ORDER BY created_at DESC) = 1;

-- task_bronze_hosts
CREATE OR REPLACE TASK task_bronze_hosts
WAREHOUSE = WH_INGEST
AFTER task_root
WHEN SYSTEM$STREAM_HAS_DATA('stream_hosts')
AS
INSERT INTO AIRBNB.BRONZE.BRONZE_HOSTS
    (host_id, host_name, host_since, is_superhost, response_rate, created_at)
    SELECT 
        host_id, host_name, host_since, is_superhost, response_rate, created_at
    FROM stream_hosts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host_id, created_at ORDER BY created_at DESC) = 1;


--cette task a pour but de lancer dbt une seule fois
CREATE OR REPLACE TASK task_bronze_done 
WAREHOUSE = WH_INGEST
AFTER
    task_bronze_hosts,
    task_bronze_listings,
    task_bronze_bookings
AS
SELECT 1;


-- Creer une task qui declanche les model dbt (silver > gold) 
-- si stream detect des nouveaux insert
CREATE OR REPLACE TASK task_run_dbt
WAREHOUSE =  WH_TRANSFORM_DBT
AFTER task_bronze_done
AS
CALL run_dbt_pipeline();



-- Resume selon ordre du fils au parent
ALTER TASK task_run_dbt         RESUME; --1
ALTER TASK task_bronze_done     RESUME; --2
ALTER TASK task_bronze_done     RESUME; --2
ALTER TASK task_bronze_hosts    RESUME;--3
ALTER TASK task_bronze_listings RESUME;--3
ALTER TASK task_bronze_bookings RESUME; --3
ALTER TASK task_root            RESUME; --4


 
-- Stored procedure avec EXECUTE IMMEDIATE
CREATE OR REPLACE PROCEDURE run_dbt_pipeline()
  RETURNS STRING
  LANGUAGE SQL
AS
BEGIN
  -- Exécuter les scripts SQL du repo Git
  EXECUTE IMMEDIATE FROM @AIRBNB_REPO/branches/main/dbt/target/run/silver_bookings.sql;
  EXECUTE IMMEDIATE FROM @AIRBNB_REPO/branches/main/dbt/target/run/gold_bookings.sql;
  RETURN 'dbt pipeline completed';
END;