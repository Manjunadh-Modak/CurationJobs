from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

# Create SparkSession
spark = SparkSession.builder \
              .appName('SparkByExamples.com') \
              .getOrCreate()
              
duration = 74 * 60
end_time = time.time() + (duration * 60)  # Convert minutes to seconds

while time.time() < end_time:
    df = spark.createDataFrame([["1"]], ["id"])
    df.select(
        current_date().alias("current_date"),
        date_format(current_date(), "yyyy MM dd").alias("yyyy MM dd"),
        date_format(current_timestamp(), "MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"),
        date_format(current_timestamp(), "yyyy MMM dd").alias("yyyy MMMM dd"),
        date_format(current_timestamp(), "yyyy MMMM dd E").alias("yyyy MMMM dd E")
    ).show()
    
    # SQL
    spark.sql("select current_date() as current_date, " +
              "date_format(current_timestamp(),'yyyy MM dd') as yyyy_MM_dd, " +
              "date_format(current_timestamp(),'MM/dd/yyyy hh:mm') as MM_dd_yyyy, " +
              "date_format(current_timestamp(),'yyyy MMM dd') as yyyy_MMMM_dd, " +
              "date_format(current_timestamp(),'yyyy MMMM dd E') as yyyy_MMMM_dd_E").show()
   
    time.sleep(300)
