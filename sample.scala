import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.Almaren
 

val almaren = Almaren("App Name")
val someDF = Seq(
  (1, 1),
  (64, 64),
  (-27, 27)
).toDF("datastore_id", "table_id")
someDF.show(false)
var a = 700
 
while (a > 0) 
        {
            println("a is : " + a)
            a = a - 1;
            Thread.sleep(1000)
            val df = almaren.builder.sourceDataFrame(someDF).batch
            df.show(false)
        }
