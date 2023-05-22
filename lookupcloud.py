import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Pyspark").getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://172.30.3.196:5432/yeedu") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "yeedu.lookup_cloud_providers") \
    .option("user", "postgres").option("password", "postgres").load()

df.printSchema()
df.select("id","name","description").show()

df.write.mode("Overwrite").option("path", "/cdpmodakbucket/cdpdevenv/data/warehouse/tablespace/external/hive/yeedu/lookup_cloud_provider.parquet")
