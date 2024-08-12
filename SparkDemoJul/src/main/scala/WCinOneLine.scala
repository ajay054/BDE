package org.itc.com

import org.apache.spark.SparkContext
object WCinOneLine extends App {

val sc = new SparkContext(master = "local[1]", appName = "AppName")

val result = sc.textFile( path = "data.txt").flatMap(line => line.split(" ")).map(w => (w.toLowerCase(),1)).reduceByKey((x, y) => x + y)

val res = result.collect()
for(r <- res){

  val word = r._1
  val cnt = r._2
  println(s"$word : $cnt")
}


}
