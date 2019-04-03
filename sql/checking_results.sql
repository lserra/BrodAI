-- To check values duplicated
SELECT * 
FROM "mm_data_lake"."mdb_fields_joined"
WHERE 
email = 'jamese.baker@att.net' OR
email = 'a.mcknight@gmail.com';

-- To check all values filled
SELECT * 
FROM "mm_data_lake"."mdb_fields_joined"
WHERE 
net_income IS NOT NULL AND
ethnicity IS NOT NULL AND
age IS NOT NULL AND
gender IS NOT NULL;

-- To check the 1000 first values
SELECT * 
FROM "mm_data_lake"."mdb_fields_joined"
limit 1000;