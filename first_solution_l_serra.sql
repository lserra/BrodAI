SHOW TABLES IN mm_redirect_logs 'mdb_field_*';

DESCRIBE mdb_field_account_no;
DESCRIBE mdb_field_acct_type;
DESCRIBE mdb_field_activechki;
DESCRIBE mdb_field_addr_abbrev;
DESCRIBE mdb_field_age;
DESCRIBE mdb_field_address_type_indicator;
DESCRIBE mdb_field_alzeimers;
DESCRIBE mdb_field_buy_health_beauty;

/*Testing the first solution*/
CREATE TABLE mm_redirect_logs.test_laercio_serra
WITH (
  format='PARQUET'
) AS
SELECT 
    -- a.sourceid as SOURCEID,
    a.email as email,
    b.entry as first_name,
    upper(substr(a.entry, 1, 1)) as gender
FROM 
    "mm_redirect_logs"."mdb_field_gender" a INNER JOIN
    "mm_redirect_logs"."mdb_field_firstname" b
    ON a.email = b.email
    AND a.sourceid = b.sourceid
WHERE a.sourceid = 1419;

CREATE EXTERNAL TABLE `test_laercio_serra`(
  -- `sourceid` bigint COMMENT '', 
  `email` string COMMENT '', 
  `first_name` string COMMENT '', 
  `gender` string COMMENT '')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  -- 's3://aws-athena-query-results-925821979506-us-east-1/Unsaved/2019/03/22/tables/ec2e29b6-b9ed-4fe1-81be-7a48e1ce3392'
  s3://mm-redirect-logs/warehouse/mm/masterdb/fields/test_laercio_serra
TBLPROPERTIES (
  'has_encrypted_data'='false')

-- (email)
-- gender
-- race/ethnicity => Don't exist the table race
-- age
-- income
SELECT DISTINCT
    -- a.sourceid as SOURCEID,
    a.email as email,
    b.entry as first_name,
    upper(substr(a.entry, 1, 1)) as gender,
    c.entry as age
FROM 
    "mm_redirect_logs"."mdb_field_gender" a 
    INNER JOIN "mm_redirect_logs"."mdb_field_firstname" b
    ON a.email = b.email AND a.sourceid = b.sourceid
    
    INNER JOIN "mm_redirect_logs"."mdb_field_age" c
    ON a.email = c.email AND a.sourceid = c.sourceid
-- WHERE a.sourceid = 1419;

-- Tests: spark_tests => Yesterday, I did a few tests to check the services what I need to do my job
-- And I have some questions and I found out a few issues

-- Lets start for the questions
-- Questions:
-- (email)
-- gender
-- race => Don't exist the table race
-- ethnicity => There are many tables. Which table should I use? [tables_ethnic]
-- age
-- income => There are many tables. Which table should I use: [tables_income]

-- Now lets talk about the issues
-- Issues:
-- AwsCloudwatchAccessDenied => I can't view the Log to monitor the job processing through Cloudwatch
-- AwsS3AccessDenied => I can't drop folder and files created by me on S3

-- Next steps:
-- At this moment I'm creating a pyspark code to run this night on AWS Glue

-- Lets talk tomorrow to know about the results at same time. Is it possible?
