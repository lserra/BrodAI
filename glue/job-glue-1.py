# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, gender, ethnic, age, income
# ==============================================================================
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, regexp_extract, regexp_replace, udf
from pyspark.sql.types import StringType

# Params to be trigged by lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Initiating job and args
job = Job(glueContext)
job.init(args['JOB_NAME'])

# Loading a table from Glue data catalog
# email, gender, ethnic, age, income
df_email = glueContext.create_dynamic_frame.from_catalog(
    database="parquet",
    table_name="broker_crawler_broker"
    ).toDF()

df_gender = glueContext.create_dynamic_frame.from_catalog(
    database="parquet",
    table_name="broker_crawler_broker"
    ).toDF()

df_ethnic = glueContext.create_dynamic_frame.from_catalog(
    database="parquet",
    table_name="broker_crawler_broker"
    ).toDF()

df_age = glueContext.create_dynamic_frame.from_catalog(
    database="parquet",
    table_name="broker_crawler_broker"
    ).toDF()

df_income = glueContext.create_dynamic_frame.from_catalog(
    database="parquet",
    table_name="broker_crawler_broker"
    ).toDF()

# For each dataframe
# Lowering the case for columns
# To avoid Hive issues
for col in df_email.columns:
    df_email = df_email.withColumnRenamed(col, col.lower())

for col in df_age.columns:
    df_age = df_age.withColumnRenamed(col, col.lower())

for col in df_ethnic.columns:
    df_ethnic = df_ethnic.withColumnRenamed(col, col.lower())

for col in df_income.columns:
    df_income = df_income.withColumnRenamed(col, col.lower())

for col in df_gender.columns:
    df_gender = df_gender.withColumnRenamed(col, col.lower())

# Renaming column join id
# df = df.withColumnRenamed('lineitem_usageaccountid', 'linked_account_id')

# For each dataframe
# Selecting distinct values [email]
df_email_unique = df_email.select(['email']).distinct()
df_age_unique = df_age.select(['email']).distinct()
df_ethnic_unique = df_ethnic.select(['email']).distinct()
df_income_unique = df_income.select(['email']).distinct()
df_gender_unique = df_gender.select(['email']).distinct()

# Joining all emails only one dataframe
df_email_union = df_email_unique.union(
    df_age_unique.union(
        df_ethnic_unique.union(
            df_income_unique.union(
                df_gender_unique
            )
        )))

# Selecting distinct values [email]
df_email_union_unique = df_email_union.select(['email']).distinct()

# Selecting values from right table
billing_joined = df.join(
    orgs_filtered, ["linked_account_id"], "left_outer"
    )

# Selecting distinct values
broker_filtered = broker.select(['ccode', 'tcode']).distinct()

# Using left outer join to populate only billing table
billing_joined = billing_joined.
join(broker_filtered, ["ccode"], "left_outer")

# Dropping AWS account id
billing_joined = billing_joined.drop('linked_account_id')

# Selecting distinct values from right table
faturamento_filtered = faturamento.select(
    ['tcode', 'nomedocliente', 'macrosegmentosrie', 'estadodocliente']
    ).distinct()

# Using left outer join to populate only billing table
billing_joined = billing_joined.join(
    faturamento_filtered, ["tcode"], "left_outer"
    )

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    billing_joined, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://gbassan-athena/totvs/out/aws_billing/year=" +
            year + "/month=05/day=" + day + "/cloud=aws"
            },
        format="parquet"
        )
