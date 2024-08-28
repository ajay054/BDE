package org.itc.com

import org.apache.spark.{SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingDemo extends App {

  val sc = new SparkContext(master = "local[*]", appName = "streamdemo")
  val ssc = new StreamingContext(sc, Seconds(2))
  val lines = ssc.socketTextStream(hostname = "localhost", port = 9996)
  val wordcnt = lines.flatMap(x => x.split( " ")).map(x => (x,1)).reduceByKey((x,y) => (x+y))
  wordcnt.print()

  ssc.start()
  ssc.awaitTermination()

}
