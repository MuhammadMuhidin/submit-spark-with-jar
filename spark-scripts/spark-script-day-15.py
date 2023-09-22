import pyspark
import os
from pyspark.sql.types import *
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, dense_rank, count, col

# Load .env
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

# Get environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
spark_master_hostname = os.getenv('SPARK_MASTER_HOST_NAME')
spark_master_port = os.getenv('SPARK_MASTER_PORT')

# Create SparkContext and SparkSession
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
                pyspark
                .SparkConf()
                .setAppName('Dibimbing')
                .setMaster('spark://' + spark_master_hostname + ':' + spark_master_port)
                .set('spark.jars', '/spark-scripts/postgresql-42.2.18.jar')
            )
)
sparkcontext.setLogLevel('WARN')
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Create JDBC
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read data from PostgreSQL
data = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

# Create a time window based on customer (customerid)
window_spec = Window.partitionBy("customerid").orderBy("invoicedate")

# Create new column 'rank' base invoicedate
data = data.withColumn("rank", dense_rank().over(window_spec))

# Calculate the number of active customers every month
retention = data.groupBy("invoicedate").agg(count("customerid").alias("active_customers"))

# Calculate the total number of customers
total_customers = data.select("customerid").distinct().count()

# Calculates retention as a percentage of active customers divided by total customers ('2011-01-01' until '2011-01-31')
retention = retention.withColumn("retention_rate", (col("active_customers") / total_customers) * 100) \
    .filter((col("invoicedate") >= '2011-01-01') & (col("invoicedate") <= '2011-01-31')) \
    .orderBy('retention_rate', ascending=False)

# Write data to PostgreSQL
retention.limit(10).write.mode('overwrite').jdbc(
    jdbc_url,
    'public.top_10_rate_retail',
    properties=jdbc_properties
)

# Clear Garbage Collect using spark.stop()
spark.stop()