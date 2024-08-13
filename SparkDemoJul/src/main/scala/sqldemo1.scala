package org.itc.com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField, StructType}

object DataframeDemo extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
    .setAppName("DataframeDemo")
    .set("spark.master", "local[*]")

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()

  // explicit schema way2 – ddl string
  val ddlSchema =
    """
product_number string, product_name string, product_category string, product_scale string,
product_manufacturer string, product_description string, length double, width double, height float
""".stripMargin

  // val productdf = spark.read.option("header",true).option("inferSchema",true).csv("D:\\Demos\\input\\warehouse\\products.csv")
  val productdf = spark.read.option("header", true)
    .schema(ddlSchema)
    .csv(path = "C:\\Users\\ajayk\\Documents\\Scala_24\\warehouse\\clean\\productscleaned2")

  productdf.createOrReplaceTempView(viewName = "product")

  spark.sql(sqlText = "select * from product").show(numRows = 5)
}
