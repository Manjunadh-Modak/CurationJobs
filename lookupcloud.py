import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Pyspark").getOrCreate()

table_name=spark.sparkContext.getConf().get("spark.driver.lookup_value")

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://172.30.3.196:5432/yeedu") \
    .option("driver", "org.postgresql.Driver").option("query", f"SELECT * FROM yeedu.{table_name}") \
    .option("user", "postgres").option("password", "postgres").load()

df.show(truncate=False)

df.write.mode('overwrite').parquet("path", f"s3a://cdpmodakbucket/cdpdevenv/data/warehouse/tablespace/external/hive/yeedu/{table_name}.parquet")
