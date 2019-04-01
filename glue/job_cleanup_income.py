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
from pyspark.sql import SparkSession

# Params to be trigged by lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initiating job and args
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Loading a table from Glue data catalog
# email, gender
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="new_netincome",
    transformation_ctx="datasource0")

applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("email", "string", "email", "string"),
        ("entry", "string", "entry", "string")
        ],
    transformation_ctx="applymapping1")

selectfields2 = SelectFields.apply(
    frame=applymapping1,
    paths=["email", "entry"],
    transformation_ctx="selectfields2"
    )

resolvechoice3 = ResolveChoice.apply(
    frame=selectfields2,
    choice="MATCH_CATALOG",
    database="mm_data_lake",
    table_name="mdb_field_new_netincome",
    transformation_ctx="resolvechoice3"
    )

resolvechoice4 = ResolveChoice.apply(
    frame=resolvechoice3,
    choice="make_struct",
    transformation_ctx="resolvechoice4"
    )

# Dropping fields with NULL values
results1 = DropNullFields.apply(
    frame=resolvechoice4,
    transformation_ctx="results1"
    )

# Renaming column from entry to age
results2 = RenameField.apply(
    frame=results1,
    old_name="entry",
    new_name="net_income",
    transformation_ctx="results2"
    )

# Selecting distinct values to put all data into a single file, 
# We need to convert it to a data frame, repartition it, and write it out.
results3 = results2.select_fields(
    ['email', 'net_income']).toDF().distinct().repartition(1)

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(results3, glueContext, "dynamic")

# Writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=df_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://aws-glue-temporary-925821979506-us-east-1/mm_data_lake/results/netincome/"
            },
        format="parquet"
        )

job.commit()
