# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, gender, ethnic, age, income
# Job parameters:
# --conf: spark.driver.maxResultSize=2G
# --enable-glue-datacatalog
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

# Loading a table from Glue data catalog to DynamicFrames
dyf_age = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_age"
    )

dyf_ethnic = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_ethnicity"
    )

dyf_income = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_netincome"
    )

dyf_gender = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_gender"
    )

# Converting DynamicFrame to Spark dataframe
df_age = dyf_age.toDF()
df_ethnic = dyf_ethnic.toDF()
df_income = dyf_income.toDF()
df_gender = dyf_gender.toDF()

# Selecting distinct values [email] for each dataframe
df_age_unique = df_age.select('email').distinct()
df_ethnic_unique = df_ethnic.select('email').distinct()
df_income_unique = df_income.select('email').distinct()
df_gender_unique = df_gender.select('email').distinct()

# Union all emails only one dataframe
df_email_union = df_age_unique.union(
        df_ethnic_unique.union(
            df_income_unique.union(
                df_gender_unique
            )))

# Selecting distinct values to put all data into a single file,
# We need to convert it to a data frame, repartition it, and write it out.
df_email_union_unique = df_email_union.select('email').distinct().repartition(1)

# Converting a Spark datfarame to DynamicFrame
df_dyf = DynamicFrame.fromDF(
    df_email_union_unique, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/results/emails_unique"
            },
        format="parquet"
        )

job.commit()
