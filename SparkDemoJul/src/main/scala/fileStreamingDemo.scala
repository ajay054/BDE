package org.itc.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.itc.com.streamingDemo.lines
import org.apache.log4j.{Level, Logger}

object fileStreamingDemo extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "DataframeDemo")
  sparkConf.set("spark.master", "local[*]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
  sparkConf.set("spark.sql.streaming.schemaInference", "true")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val ddlSchema = """order_id int, order_date string, order_customer_id int, order_status string, amount int"""
  val ordersDF = spark.readStream.format("json").option("path", "input").load()

  ordersDF.createOrReplaceTempView("order")
  val onHoldData = spark.sql("select * from order where order_status = 'ON_HOLD'")

  val res = onHoldData.writeStream.format("json").option("path", "output").outputMode("append")
    .option("checkpointLocation", "checkpoint-Location")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .start()

  res.awaitTermination()
}
