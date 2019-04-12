# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Job parameter: --conf: spark.driver.maxResultSize=2G
# ==============================================================================
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

# Params to be trigged by lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initiating job and args
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ==============================================================================
# Converting CSV file to Parquet file.
# Database: mm_redirect_logs
# Table Source: mdb_field_ethnicity
# Table Target: new_ethnicity
# ==============================================================================
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="mdb_field_ethnicity",
    transformation_ctx="datasource")

apply_mapping = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("email", "string", "email", "string"),
        ("entry", "string", "entry", "string")
        ],
    transformation_ctx="apply_mapping")

select_fields = SelectFields.apply(
    frame=apply_mapping,
    paths=["email", "entry"],
    transformation_ctx="select_fields"
    )

# Converting DynamicFrame to Spark dataframes
dyf_df = select_fields.toDF().repartition(1)

# Converting Spark to DynamicFrame dataframes
df_dyf = DynamicFrame.fromDF(
    dyf_df, glueContext, "dynamic"
    )

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/data_raw/ethnicity/"
            },
        format="parquet"
        )

job.commit()
