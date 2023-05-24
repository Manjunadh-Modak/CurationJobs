import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "/home/ma0804/Downloads/postgresql-42.4.2.jar").master("local").appName("Pyspark").getOrCreate()

table_name=spark.sparkContext.getConf().get("spark.driver.lookup_value")

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://172.30.3.196:5432/yeedu") \
    .option("driver", "org.postgresql.Driver").option("query", f"SELECT id,name,description,CAST(from_date as timestamp) FROM yeedu.{table_name}") \
    .option("user", "postgres").option("password", "postgres").load()

df.show(truncate=False)

df.write.mode("Overwrite").option("path", f"s3a://cdpmodakbucket/cdpdevenv/data/warehouse/tablespace/external/hive/yeedu/{table_name}.parquet")
