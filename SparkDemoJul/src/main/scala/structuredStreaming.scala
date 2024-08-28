import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.itc.com.streamingDemo.lines

object structuredStreaming extends App{

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "DataframeDemo")
  sparkconf.set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  val lines = spark.readStream.format(source = "socket").option("host", "localhost").option("port", 9996).load()

  import spark.implicits._

  val words = lines.as[String].flatMap(_.split( " "))
  val wordcnt = words.groupBy( "value").count

  val producer1 = wordcnt.writeStream.outputMode(outputMode = "complete").format(source = "console").trigger(Trigger.ProcessingTime( "5 seconds")).option("checkpointLocation", "C:/spark/checkpoint").start()

  producer1.awaitTermination()
}