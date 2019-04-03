# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Joining fields from many tables into only one table
# Fields: email, net_income, ethnicity, age, and gender
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
dyf_emails_unique = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_emails_unique"
    )

dyf_net_income = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_netincome"
    )

dyf_ethnicity = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_ethnicity"
    )

dyf_age = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_age"
    )

dyf_gender = glueContext.create_dynamic_frame.from_catalog(
    database="mm_data_lake",
    table_name="mdb_field_new_gender"
    )

# Converting DynamicFrame to Spark dataframes
df_emails_unique = dyf_emails_unique.toDF()
df_net_income = dyf_net_income.toDF()
df_ethnicity = dyf_ethnicity.toDF()
df_age = dyf_age.toDF()
df_gender = dyf_gender.toDF()

# Removing values duplicated
# Grouping by email and returning the max value
df_net_income_clean = df_net_income.groupby('email').agg(F.max('net_income').alias("net_income"))
df_ethnicity_clean = df_ethnicity.groupby('email').agg(F.max('ethnicity').alias("ethnicity"))
df_age_clean = df_age.groupby('email').agg(F.max('age').alias("age"))
df_gender_clean = df_gender.groupby('email').agg(F.max('gender').alias("gender"))

# Joining field: net_income
df_joined_net_income = df_emails_unique.join(
    df_net_income_clean, ['email'], 'left_outer'
    ).select(
        df_emails_unique['email'],
        df_net_income_clean['net_income']
        ).distinct()

# Joining field: ethnicity
df_joined_ethnicity = df_joined_net_income.join(
    df_ethnicity_clean, ['email'], 'left_outer'
    ).select(
        df_joined_net_income['email'],
        df_joined_net_income['net_income'],
        df_ethnicity_clean['ethnicity']
        ).distinct()

# Joining field: age
df_joined_age = df_joined_ethnicity.join(
    df_age_clean, ['email'], 'left_outer'
    ).select(
        df_joined_ethnicity['email'],
        df_joined_ethnicity['net_income'],
        df_joined_ethnicity['ethnicity'],
        df_age_clean['age']
        ).distinct()

# Joining field: gender
df_joined_gender = df_joined_age.join(
    df_gender_clean, ['email'], 'left_outer'
    ).select(
        df_joined_age['email'],
        df_joined_age['net_income'],
        df_joined_age['ethnicity'],
        df_joined_age['age'],
        df_gender_clean['gender']
        ).distinct()

# We need to convert it to a dataframe, repartition it, and write it out.
df_fields_joined = df_joined_gender.repartition(1)

# Converting to a dynamic dataframe
df_dyf = DynamicFrame.fromDF(
    df_fields_joined, glueContext, "dynamic"
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
