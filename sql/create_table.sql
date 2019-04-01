/*Creating a test table*/
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
  s3://mm-redirect-logs/warehouse/mm/masterdb/fields/test_laercio_serra
TBLPROPERTIES (
  'has_encrypted_data'='false')
================================================================================
/*Converting the Gender table to PARQUET format*/
CREATE TABLE IF NOT EXISTS mm_redirect_logs.new_gender
WITH (
  format='PARQUET'
) AS
SELECT 
  trim(lower(email)) as email, 
  trim(lower(entry)) as entry
FROM "mm_redirect_logs"."mdb_field_gender"
WHERE lower(entry) in ('m', 'f', 'u');
================================================================================
/*Converting the Age table to PARQUET format*/
CREATE TABLE IF NOT EXISTS mm_redirect_logs.new_age
WITH (
  format='PARQUET'
) AS
SELECT
  lower(email) as email, 
  lpad(entry, 3, '0') as entry
FROM "mm_redirect_logs"."mdb_field_age"
WHERE 
  entry NOT LIKE 'value%';
================================================================================
/*Converting the Net_Income table to PARQUET format*/
CREATE TABLE IF NOT EXISTS mm_redirect_logs.new_netincome
WITH (
  format='PARQUET'
) AS
SELECT
  lower(email) as email, 
  entry
FROM "mm_redirect_logs"."mdb_field_netincome"
WHERE entry != 'NULL';
================================================================================
/*Converting the Ethnicity table to PARQUET format*/
CREATE TABLE IF NOT EXISTS mm_redirect_logs.new_ethnicity
WITH (
  format='PARQUET'
) AS
SELECT
  lower(email) as email, 
  lower(entry) as entry
FROM "mm_redirect_logs"."mdb_field_ethnicity"
WHERE entry NOT LIKE 'value%';
================================================================================
SELECT * FROM "mm_redirect_logs"."new_gender" limit 100;
SELECT * FROM "mm_redirect_logs"."new_age" limit 100;
SELECT * FROM "mm_redirect_logs"."new_netincome" limit 100;
SELECT * FROM "mm_redirect_logs"."new_ethnicity" limit 100;