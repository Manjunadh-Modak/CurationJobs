import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.conf import SparkConf

spark = SparkSession.builder.master("local").appName("Pyspark").getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://172.30.3.196:5432/yeedu") \
    .option("driver", "org.postgresql.Driver").option("query", "SELECT id,name,description,CASE WHEN from_date IS NULL THEN '' ELSE to_char(from_date, 'MM-DD-YYYY HH24:MI:SS') END AS from_date,CAST(to_date AS text) as to_date FROM yeedu.lookup_cloud_providers") \
    .option("user", "postgres").option("password", "postgres").load()

df.show(truncate=False)

df.write.mode("Overwrite").option("path", "s3a://cdpmodakbucket/cdpdevenv/data/warehouse/tablespace/external/hive/yeedu/pysaprk_lookup_cloud_provider.parquet")
