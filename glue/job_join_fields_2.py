# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, ethnicity
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
from pyspark.sql import functions as F
# from pyspark.sql.functions import col, regexp_extract, regexp_replace, udf
# from pyspark.sql.types import StringType

# Params to be trigged by lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Initiating job and args
job = Job(glueContext)
job.init(args['JOB_NAME'])

# Loading the tables from Glue data catalog
dyf_emails_unique = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_emails_unique"
    )

dyf_net_income = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_netincome"
    )

# Converting DynamicFrame to Spark dataframes
df_emails_unique = dyf_emails_unique.toDF()
df_net_income = dyf_net_income.toDF()

# Removing values duplicated (email + net_income)
# Grouping by email and returning the max net_income
df_net_income_clean = df_net_income.groupby('email').agg(F.max('net_income').alias("net_income"))

# Joining fields: email, net_income
df_joined = df_emails_unique.join(
    df_net_income_clean, ['email'], 'inner'
    ).select(
        df_emails_unique['email'],
        df_net_income_clean['net_income']
        )

# We need to convert it to a dataframe, repartition it, and write it out.
df_joined_net_income = df_joined.repartition(1)

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_joined_net_income, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/results/fields_joined/"
            },
        format="parquet"
        )

job.commit()
