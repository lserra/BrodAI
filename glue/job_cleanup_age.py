# ==============================================================================
# BrodAI
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Cleaning up table: Age
# Moving the table to: 'mm_data_lake'
# Job parameter: --conf: spark.driver.maxResultSize=2G
# ==============================================================================
import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, trim, lpad, length

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
# email, age
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="mm_redirect_logs",
    table_name="new_age",
    transformation_ctx="datasource")

apply_mapping = ApplyMapping.apply(
    frame=datasource,
    mappings=[
        ("email", "string", "email", "string"),
        ("entry", "string", "entry", "string")
        ],
    transformation_ctx="apply_mapping")

# Creating a DynamicFrame dataframe
select_fields = SelectFields.apply(
    frame=apply_mapping,
    paths=["email", "entry"],
    transformation_ctx="select_fields"
    )

# Renaming column "entry" to "age"
rename_col = RenameField.apply(
    frame=select_fields,
    old_name="entry",
    new_name="age",
    transformation_ctx="rename_col"
    )

# Dropping fields with NULL values
drop_null = DropNullFields.apply(
    frame=rename_col,
    transformation_ctx="drop_null"
    )

# Converting DynamicFrame to Spark dataframe
dyf_df = drop_null.toDF()

# Converting a string column to lower case.
lower_cols = dyf_df.select(
    lower(dyf_df['email']).alias('email'),
    lower(dyf_df['age']).alias('age')
    )

# Trim the spaces from both ends for the specified string column
trim_cols = lower_cols.select(
    trim(lower_cols['email']).alias('email'),
    trim(lower_cols['age']).alias('age')
    )

# Left-pad the string column to width len with pad.
# Ex.: 50 -> 050; 10 -> 010
pad_age = trim_cols.select(
    trim_cols['email'],
    lpad(trim_cols['age'], 3, '0').alias('age')
    )

# Filtering rows where length(age) = 3
# "!" = NOT LIKE in SQL
# where(!pad_age['age'].like('value%'))
filter_rows = pad_age.select(
    ['email', 'age']).where(length(pad_age['age']) == 3)

# Selecting distinct values to put all data into a single file
distinct_values = pad_age.select(
    ['email', 'age']).distinct().repartition(1)

# Converting Spark to DynamicFrame dataframe
df_dyf = DynamicFrame.fromDF(distinct_values, glueContext, "dynamic")

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
