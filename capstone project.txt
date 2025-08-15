#ETL JOB 1

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


customer_df=spark.sql(''' select * from project_db.customers_dataset_csv  ''')

customer_df.show(10,0)

drop_customer_dup = customer_df.dropDuplicates(["customer_id"])

drop_null = drop_customer_dup.dropna(subset=["email", "zip_code"])

output_path = "s3://gluetraining-sk11/Capstone_Project/Target/Customer_Data_Cleansing/"

drop_null.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(output_path)

job.commit()




---------------
#ETL JOB 2

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum , count, explode, col, first, year
import logging

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)  
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

customer_df = spark.read.parquet("s3://gluetraining-sk11/Capstone_Project/Target/Customer_Data_Cleansing/part-00000-e822c1a3-7559-4d92-83dc-01aeeb2cf0f2-c000.snappy.parquet")

logger.info(f"Starting Glue job: {args['JOB_NAME']}")

#geolocation_df = spark.sql(''' select * from  project_db.geolocation_dataset_json ''')

geolocation_df = glueContext.create_dynamic_frame.from_catalog(
    database="project_db",
    table_name="geolocation_dataset_json"
)

logger.info(f"gelocation data frame created  with count : {geolocation_df.count()} ")

df_geolocation = geolocation_df.toDF()

transaction_df = spark.sql(''' select * from  project_db.transactions_dataset_csv ''')

logger.info(f"transaction data frame created  with count : {transaction_df.count()} ")

df_joined = (
    customer_df.join(df_geolocation, on="zip_code", how="inner")
       .join(transaction_df, on="customer_id", how="inner")
)

df_agg = (
    df_joined.groupBy("customer_id")
      .agg(
          sum("transaction_amount").alias("total_transaction_amount"),
          count("transaction_amount").alias("transaction_count"),
           first("zip_code").alias("zip_code"), 
           first("city").alias("city"), 
           first("state").alias("state"),
           first("name").alias("name"),
            first("email").alias("email"),
             first(year(col("created_at"))).alias("year")  
          
      )
)

df_agg.show(10,0)

output_path = "s3://gluetraining-sk11/Capstone_Project/Target/Customer360/"
 
df_agg.write.mode("overwrite") \
    .partitionBy("state","year") \
    .option("compression", "snappy") \
    .parquet(output_path)

 
logger.info(f"Job Completed succesfully ")

job.commit()