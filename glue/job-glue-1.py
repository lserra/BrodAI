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
    database="parquet", table_name="broker_crawler_broker"
    ).toDF()

faturamento = glueContext.create_dynamic_frame
.from_catalog(
    database="parquet",
    table_name="faturamento_crawler_faturamento")
.toDF()

# Lowering the case for columns (to avoid hive problems)
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower())

# Renaming join id
df = df.withColumnRenamed(
    'lineitem_usageaccountid', 'linked_account_id'
    )

# Loading DBR csv to get account_name and join to CUR dataframe
# orgs = spark.read.format("com.databricks.spark.csv")
# .option("header", "true")
# .option("inferSchema", "true")
# .option("delimiter", ';')
# .load(
#     's3://gbassan-athena/totvs/in/aws_orgs/poc_aws_billing_aws.csv'
#     )

# Selecting distinct values
orgs_filtered = orgs.select(
    ['linked_account_id', 'linked_account_name']
    ).distinct()

# Selecting values from right table
billing_joined = df.join(
    orgs_filtered, ["linked_account_id"], "left_outer"
    )

# Selecting distinct values
broker_filtered = broker.select(['ccode', 'tcode']).distinct()

# Renaming join id
billing_joined = billing_joined.withColumnRenamed(
    'linked_account_name', 'ccode'
    )

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

# Partitioning data
# now = datetime.datetime.now()

# print now.year, now.month, now.day, now.hour, now.minute, now.second
# year = str(now.year)
# month = str(now.month)
# if(now.day < 10):
#     day = "0" + str(now.day)
# else:
#     day = str(now.day)

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
