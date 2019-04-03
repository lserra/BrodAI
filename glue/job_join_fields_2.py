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

# Params to be trigged by lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Initiating job and args
job = Job(glueContext)
job.init(args['JOB_NAME'])

# Loading the tables from Glue data catalog
dyf_fields_joined = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_fields_joined"
    )

dyf_ethnicity = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_ethnicity"
    )

# Converting DynamicFrame to Spark dataframes
df_fields_joined = dyf_fields_joined.toDF()
df_ethnicity = dyf_ethnicity.toDF()

# Removing values duplicated (email + ethnicity)
# Grouping by email and returning the max ethnicity
df_ethnicity_clean = df_ethnicity.groupby('email').agg(F.max('ethnicity').alias("ethnicity"))

# Joining fields: email, net_income, and ethnicity
df_joined = df_fields_joined.join(
    df_ethnicity_clean, ['email'], 'left_outer'
    ).select(
        df_fields_joined['email'],
        df_fields_joined['net_income'],
        df_ethnicity_clean['ethnicity']
        )

# We need to convert it to a dataframe, repartition it, and write it out.
df_joined_ethnicity = df_joined.repartition(1)

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_joined_ethnicity, glueContext, "dynamic"
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
