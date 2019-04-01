/*Creating the test table*/
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
================================================================================
/*Recreating the Gender table*/
CREATE TABLE IF NOT EXISTS mm_redirect_logs.new_gender
WITH (
  format='PARQUET'
) AS
SELECT 
trim(replace(lower(email),'"', '')) as email, 
trim(replace(lower(entry),'"', '')) as entry
FROM "mm_redirect_logs"."mdb_field_gender"
WHERE lower(entry) in ('"m"', '"f"', '"u"');
================================================================================
/*Recreating the Gender table*/
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
SELECT entry, count(*) as QTY
FROM "mm_redirect_logs"."mdb_field_age"
GROUP BY entry
limit 100;
================================================================================
SELECT DISTINCT entry
FROM "mm_redirect_logs"."mdb_field_age"
limit 100;