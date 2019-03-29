# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, gender, ethnic, age, income
# ==============================================================================
import datetime
import sys

from dateutil import parser, tz

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, regexp_extract, regexp_replace, udf
from pyspark.sql.types import StringType

# @paramsv initialization
# Params to be trigged by lambda function
# args = getResolvedOptions(sys.argv, ['FileUploaded'])
args = getResolvedOptions(
    sys.argv, ['JOB_NAME', 'Bucket', 'Folder', 'FileName']
    )

# Create a Glue context
# glueContext.setConf("spark.sql.parquet.binaryAsString","true")
# glueContext.setConf("spark.sql.parquet.int96AsTimestamp","true")
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Initiate job and args
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the table and infer schema
# 's3://gbassan-athena/teste_parquet/in/0dde8ad6-4b82-4a4a-b506-7873c3649ece/PoC-CloudAnalytics-1.csv.gz')
df_age = spark.read.format(
   "com.databricks.spark.csv").option(
   "header", "true").option(
   "inferSchema", "true").load(
   "s3://"+args['Bucket']+"/"+args['Folder']+"/"+args['FileName'])

# removing slashes from column names
billing_formated = df_age.toDF(*(c.replace('/', '_') for c in df_age.columns))

# remove ":" character from resource tag name to avoid presto error
billing_formated = billing_formated
.toDF(*(c.replace(':', '_') for c in billing_formated.columns))

# droping timestamps columns to avoid error
billing_formated = billing_formated
.drop('bill_BillingPeriodStartDate')
.drop('lineItem_UsageStartDate')
.drop('bill_BillingPeriodEndDate')
.drop('lineItem_UsageEndDate')

# regenerate columns with string type - extracted from identity_TimeIterval
billing_formated = billing_formated
.withColumn(
    "bill_BillingPeriodStartDate",
    regexp_extract('identity_TimeInterval', '.+?(?=\/)', 0)
    )
.withColumn(
    "lineItem_UsageStartDate_str",
    regexp_extract('identity_TimeInterval', '.+?(?=\/)', 0)
    )
.withColumn(
    "bill_BillingPeriodEndDate_str",
    regexp_extract('identity_TimeInterval', '\/([^\/]+)', 1)
    )
.withColumn(
    "lineItem_UsageEndDate_str",
    regexp_extract('identity_TimeInterval', '\/([^\/]+)', 1)
    )

billing_formated = billing_formated
.withColumn(
    "bill_BillingPeriodStartDate",
    regexp_replace('bill_BillingPeriodStartDate', 'T', ' ')
    )
.withColumn(
    "lineItem_UsageStartDate_str",
    regexp_replace('lineItem_UsageStartDate_str', 'T', ' ')
    )
.withColumn(
    "bill_BillingPeriodEndDate_str",
    regexp_replace('bill_BillingPeriodEndDate_str', 'T', ' ')
    )
.withColumn(
    "lineItem_UsageEndDate_str",
    regexp_replace('lineItem_UsageEndDate_str', 'T', ' ')
    )

# lowering the case for columns (to avoid hive problems)
for col in billing_formated.columns:
    billing_formated = billing_formated.withColumnRenamed(col, col.lower())

# rename join id
billing_formated = billing_formated.withColumnRenamed(
    'lineitem_usageaccountid', 'linked_account_id'
    )

# Load DBR csv to get account_name and join to CUR dataframe
orgs = spark.read.format(
   "com.databricks.spark.csv").option(
   "header", "true").option(
   "inferSchema", "true").option("delimiter", ';').load(
   's3://gbassan-athena/totvs/in/aws_orgs/poc_aws_billing_aws.csv')

# getting distinct values from right table
orgs_filtered = orgs.select(
    ['linked_account_id', 'linked_account_name']
    ).distinct()

billing_joined = billing_formated.join(
    orgs_filtered, ["linked_account_id"], "left_outer"
    )

# removing .aws substring from linked_account_name to join broker df
billing_joined = billing_joined.withColumn(
    'linked_account_name',
    regexp_replace('linked_account_name', '.aws', '')
    )

# getting tcode from broker data catalog
broker = glueContext.create_dynamic_frame.from_catalog(
    database="parquet", table_name="broker_crawler_broker"
    ).toDF()

# getting distinct values from right table
broker_filtered = broker.select(['ccode', 'tcode']).distinct()

# rename join id
billing_joined = billing_joined.withColumnRenamed(
    'linked_account_name', 'ccode'
    )

# using left join to populate only billing table
billing_joined = billing_joined.join(broker_filtered, ["ccode"], "left_outer")

# drop aws account id
billing_joined = billing_joined.drop('linked_account_id')

# getting columns from faturamento data catalog
# load DBR csv to get account_name and join to CUR dataframe
faturamento = glueContext.create_dynamic_frame
.from_catalog(
    database="parquet",
    table_name="faturamento_crawler_faturamento")
.toDF()

# getting distinct values from right table
faturamento_filtered = faturamento.select(
    ['tcode', 'nomedocliente', 'macrosegmentosrie', 'estadodocliente']
    ).distinct()

# using left join to populate only billing table
billing_joined = billing_joined.join(
    faturamento_filtered, ["tcode"], "left_outer"
    )

# converting to a dynamic DF again
billing_formated_dyf = DynamicFrame.fromDF(
    billing_joined, glueContext, "dynamic"
    )

# getting partition data
now = datetime.datetime.now()

# print now.year, now.month, now.day, now.hour, now.minute, now.second
year = str(now.year)
month = str(now.month)
if(now.day < 10):
    day = "0" + str(now.day)
else:
    day = str(now.day)

# writing parquet format to load on Data Catalog
glueContext.write_dynamic_frame.from_options(
        frame=billing_formated_dyf,
        connection_type="s3",
        connection_options={
            "path": "s3://gbassan-athena/totvs/out/aws_billing/year=" +
            year + "/month=05/day=" + day + "/cloud=aws"
            },
        format="parquet"
        )
