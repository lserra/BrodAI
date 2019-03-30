# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, gender, ethnic, age, income
# Special Parameter: --enable-glue-datacatalog
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
# df_email = glueContext.create_dynamic_frame.from_catalog(
#     database="mm_redirect_logs",
#     table_name="broker_crawler_broker"
#     ).toDF()
df_age = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_age"
    ).toDF()

df_ethnic = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_ethnicity"
    ).toDF()

df_income = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_income"
    ).toDF()

df_gender = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_gender"
    ).toDF()

# For each dataframe
# Lowering the case for columns
# To avoid Hive issues
# for col in df_email.columns:
#     df_email = df_email.withColumnRenamed(col, col.lower())
for col in df_age.columns:
    df_age = df_age.withColumnRenamed(col, col.lower())

for col in df_ethnic.columns:
    df_ethnic = df_ethnic.withColumnRenamed(col, col.lower())

for col in df_income.columns:
    df_income = df_income.withColumnRenamed(col, col.lower())

for col in df_gender.columns:
    df_gender = df_gender.withColumnRenamed(col, col.lower())

# For each dataframe
# Selecting distinct values [email]
# df_email_unique = df_email.select('email').distinct()
df_age_unique = df_age.select('email').distinct()
df_ethnic_unique = df_ethnic.select('email').distinct()
df_income_unique = df_income.select('email').distinct()
df_gender_unique = df_gender.select('email').distinct()

# Joining all emails only one dataframe
df_email_union = df_age_unique.union(
        df_ethnic_unique.union(
            df_income_unique.union(
                df_gender_unique
            )))

# Selecting distinct values [email]
df_email_union_unique = df_email_union.select('email').distinct()

# Joining values: email and age
df_joined_age = df_email_union_unique.join(
    df_age, 'email', 'left_outer'
    ).select(
        df_email_union_unique['email'],
        df_age['entry'].alias('age')
        ).collect()

# Dropping columns duplicated: email
# df_joined_age.drop('email')

# Renaming column from entry to age
# df_joined_age.withColumnRenamed('entry', 'age')

# Joining values: email, age, and ethnic
df_joined_ethnic = df_joined_age.join(
    df_ethnic, 'email', 'left_outer'
    ).select(
        df_joined_age['email', 'age'],
        df_ethnic['entry'].alias('ethnic')
        ).collect()

# Dropping columns duplicated: email
# df_joined_ethnic.drop('email')

# Renaming column from entry to ethnic
# df_joined_ethnic.withColumnRenamed('entry', 'ethnic')

# Joining values: email, age, ethnic and income
df_joined_income = df_joined_ethnic.join(
    df_income, ['email'], 'left_outer'
    ).select(
        df_joined_ethnic['email', 'age', 'ethnic'],
        df_income['entry'].alias('income')
        ).collect()

# Dropping columns duplicated: email
# df_joined_income.drop('email')

# Renaming column from entry to ethnic
# df_joined_income.withColumnRenamed('entry', 'income')

# Joining values: email, age, ethnic, income and gender
df_joined_gender = df_joined_income.join(
    df_gender, ['email'], 'left_outer'
    ).select(
        df_joined_income['email', 'age', 'ethnic', 'income'],
        df_gender['entry'].alias('gender')
        ).collect()

# Dropping columns duplicated: email
# df_joined_gender.drop('email')

# Renaming column from entry to ethnic
# df_joined_gender.withColumnRenamed('entry', 'gender')

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_joined_gender, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/\
                mm_data_lake/results/"
            },
        format="parquet"
        )

job.commit()
