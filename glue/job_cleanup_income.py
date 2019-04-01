# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Cleaning up table: Net_Income
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
df_income = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="new_netincome"
    ).toDF()

# Lowering the case for columns to avoid Hive issues
for col in df_income.columns:
    df_income = df_income.withColumnRenamed(col, col.lower()).collect()

# Dropping column
df_income.drop('sourceid')

# Selecting distinct values
df_income_unique = df_income.select('email', 'entry').distinct().collect()

# Renaming column from entry to gender
df_income_unique.withColumnRenamed('entry', 'gender').collect()

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_income_unique, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/results/net_income/"
            },
        format="parquet"
        )

job.commit()
