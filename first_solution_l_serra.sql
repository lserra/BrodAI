/*Testing the first solution*/
CREATE TABLE mm_redirect_logs.test_laercio_serra
WITH (
  format='PARQUET'
) AS
SELECT 
    a.sourceid as SOURCEID,
    a.email as EMAIL,
    b.entry as FIRST_NAME,
    a.entry as GENDER
FROM 
    "mm_redirect_logs"."mdb_field_gender" a INNER JOIN
    "mm_redirect_logs"."mdb_field_firstname" b
    on a.email = b.email
    AND a.sourceid = b.sourceid
WHERE a.sourceid = 1419;

SHOW TABLES IN mm_redirect_logs 'mdb_field_*';
DESCRIBE mdb_field_account_no;
DESCRIBE mdb_field_acct_type;
DESCRIBE mdb_field_activechki;
DESCRIBE mdb_field_addr_abbrev;
DESCRIBE mdb_field_age;
DESCRIBE mdb_field_address_type_indicator;
DESCRIBE mdb_field_alzeimers;
DESCRIBE mdb_field_buy_health_beauty;