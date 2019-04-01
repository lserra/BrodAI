# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Cleaning up table: Age
# Moving the table to: 'mm_data_lake'
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
# email, age
df_age = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_age"
    ).toDF()

# Lowering the case for columns to avoid Hive issues
for col in df_age.columns:
    df_age = df_age.withColumnRenamed(col, col.lower())

# Dropping column
df_age.drop('sourceid')

# For each dataframe
# Selecting distinct values [email]
df_age_unique = df_age.select('email', 'entry').distinct()

# Renaming column from entry to age
df_age_unique.withColumnRenamed('entry', 'age')

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_age_unique, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/results/age/"
            },
        format="parquet"
        )

job.commit()
