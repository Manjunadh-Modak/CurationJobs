import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import spark.implicits._

val almaren = Almaren("App Name")

val lookupValue = sc.getConf.get("spark.driver.lookup_value")
val loginUrl = "http://172.30.3.196:8080/api/v1/login"
val loginData = """{ "username": "YSU0000", "password": "YSU0000", "timeout": "48" }"""
val lookupUrl = s"http://172.30.3.196:8080/api/v1/$lookupValue"

val df = almaren.builder
    .sql(s"""SELECT 
                uuid() as __ID__,
                '$loginUrl' as __URL__,'$loginData' as __DATA__""")
    .http(method = "POST", headers = Map("Content-Type" -> "application/json"),threadPoolSize = 10, batchSize = 10000).deserializer("JSON","__BODY__",None).batch

val token = df.select("token").as[String].first

val tokenNew=s"Bearer $token"

val df1 = almaren.builder
    .sql(s"""SELECT 
                uuid() as __ID__,
                '$lookupUrl' as __URL__""")
    .http(method = "GET", headers = Map("Content-Type" -> "application/json","Authorization"-> tokenNew),threadPoolSize = 10, batchSize = 10000).batch

df1.write.format("parquet").mode("overwrite").option("path", "s3a://cdpmodakbucket/cdpdevenv/data/warehouse/tablespace/external/hive/yeedu/cdp_meta_db_test").saveAsTable("nabu_test.cdp_meta_db_test")
