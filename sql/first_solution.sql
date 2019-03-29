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
