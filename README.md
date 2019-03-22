# Brod.ai

## Demand

There are around 1000's tables in AWS Athena where the columns are email, value, and sourceid.

Each one of those tables represents some sort of data point against that email.

The goal is to put all thos columns into a single table. Where the columns are email (unique), first_name, last_name, gender, etc.

And where each value is the accepted, single 'answer' for that email's various data points.

## Solutions

Here are my suggestions to deal with this situation:

- **Plan A**: Using AWS Glue/PySpark
- **Plan B**: Using Python API and AWS Athena (JDBC driver)

## DAS (Design Architecture Solution)

![DAS](https://github.com/lserra/BrodAI/blob/master/DAS.png?raw=true)

In general the solution will works this way:

- Extract the metadata from all tables and exporting to a JSON file.
- Load the JSON file to a dataframe in Spark (AWS Glue).
- Transform this dataframe adding a new column.
- Save this dataframe to a new table.
- Update this table in the AWS Data Catalog.
- Drop the old table.
- Go to the next table.

The DAS are the same for both. _So, why the Plan B_? Because the Plan B is more flexible than Plan A. I will try to do Plan A, but if not works I will try Plan B.

## Proposal

- Duration Project: 1 Week
- Starting on: 03/25/2019
- Due Date: 04/01/2019

PS.:This date is just a suggestion. But if I get to finish before this date suggested, I will delivery you before.

## Service Limits

There are some limitations related to AWS that we should be aware:

1. By default, limits on your account allow you to submit:

    - 20 DDL queries at the same time. DDL queries include CREATE TABLE and CREATE TABLE ADD PARTITION queries.

    - 20 DML queries at the same time. DML queries include SELECT and CREATE TABLE AS (CTAS) queries.

2. We can submit up to 20 queries of the same type (DDL or DML) at a time. If we submit a query that exceeds the query limit, the Athena API displays an error message:

> _**"You have exceeded the limit for the number of queries you can run concurrently. Reduce the number of concurrent queries submitted by this account. Contact customer support to request a concurrent query limit increase.”**_

3. We may encounter a limit for Amazon S3 buckets per account, which is 100. Athena also needs a separate bucket to log results.

4. The query timeout is 30 minutes.

5. The maximum allowed query string length is 262144 bytes, where the strings are encoded in UTF-8.

6. Athena APIs have the following default limits for the number of calls to the API per account (not per query):

API Name | Default Number of Calls per Second | Burst Capacity
---------|------------------------------------|---------------
BatchGetNamedQuery, BatchGetQueryExecution, ListNamedQueries, ListQueryExecutions| 5 | Up to 10
CreateNamedQuery, DeleteNamedQuery, GetNamedQuery, StartQueryExecution, StopQueryExecution | 5 | Up to 20
GetQueryExecution, GetQueryResults | 25 | Up to 50

For example, for _StartQueryExecution_, or any of the other APIs that have the same limits in the previous table, we can make up to 5 calls per second. In addition, if this API is not called for 4 seconds, our account accumulates a burst capacity of up to 20 calls. In this case, our application can make up to 20 calls to this API in burst mode.

If we use any of these APIs and exceed the default limit for the number of calls per second, or the burst capacity in our account, the Athena API issues an error similar to the following: 

> _**"ClientError: An error occurred (ThrottlingException) when calling the <API_name> operation: Rate exceeded." Reduce the number of calls per second, or the burst capacity for the API for this account. "**_

But if necessary we can contact AWS Support to request a limit increase.

## Issues

Currently, I don't have permission to create jobs on AWS Glue.

![AwsGlueAccessDenied](/home/lserra/Downloads/Upwork/AwsGlueAccessDenied.png)

## Requirements

To perform this job I need to have access permission to create jobs on AWS Glue.

## Best Practices

1. When we create schema in AWS Glue to query in Athena, consider the following:

    - A database name cannot be longer than 252 characters.

    - A table name cannot be longer than 255 characters.

    - A column name cannot be longer than 128 characters.

    - The only acceptable characters for database names, table names, and column names are lowercase letters, numbers, and the underscore character.

2. Creating Tables Using Athena for AWS Glue ETL Jobs. Tables that we create in Athena must have a table property added to them called a classification, which identifies the format of the data. This allows AWS Glue to use the tables for ETL jobs. The classification values can be csv, parquet, orc, avro, or json. An example CREATE TABLE statement in Athena follows:

```sql
CREATE EXTERNAL TABLE sampleTable (
  column1 INT,
  column2 INT
  ) STORED AS PARQUET
  TBLPROPERTIES (
  'classification'='parquet')
```

If the table property was not added when the table was created, we can add it using the AWS Glue console.

3. I recommend to use Parquet and ORC data formats. AWS Glue supports writing to both of these data formats, which can make it easier and faster for you to transform data to an optimal format for Athena.

4. To reduce the likelihood that Athena is unable to read the SMALLINT and TINYINT data types produced by an AWS Glue ETL job, convert SMALLINT and TINYINT to INT when using the wizard or writing a script for an ETL job.

5. We can configure AWS Glue ETL jobs to run automatically based on triggers. This feature is ideal when data from outside AWS is being pushed to an Amazon S3 bucket in a suboptimal format for querying in Athena.

## Tables found

Has been found around 677 tables ('mdb_field_*').

The complete list with all tables can be seen [here](/home/lserra/Downloads/Upwork/tables_found.csv).
